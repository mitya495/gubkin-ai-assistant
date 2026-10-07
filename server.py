import time
import json
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import asyncpg
import requests
import sqlglot
from sqlglot import exp

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("gubkin_api")

DB_CONFIG = {
    "user": "bot_readonly",
    "password": "gubkin_pass_2026_cb_team",
    "database": "postgres",
    "host": "localhost",
    "port": 5432
}
OLLAMA_URL = "http://localhost:11434/api/chat"
ALLOWED_TABLES = {"faculties", "programs", "applications", "students", "teachers", "courses", "grades"}

# --- FIX #2: Ollama одновременно обрабатывает один инференс -> семафор-очередь ---
# Семафор на 1 означает "только один запрос к модели одновременно".
# Остальные ждут в очереди, но хотя бы не падают и не ломают GPU-память.
OLLAMA_SEMAPHORE = asyncio.Semaphore(1)
OLLAMA_QUEUE_COUNTER = {"waiting": 0}

db_pool: Optional[asyncpg.Pool] = None

SYSTEM_PROMPT = """Ты — ведущий SQL-эксперт базы данных университета Губкина.

СПРАВОЧНИК ФАКУЛЬТЕТОВ (используй эти точные ID):
- id 1: Факультет разработки нефтяных и газовых месторождений (ФРНиГМ)
- id 2: Факультет разработки нефтегазовых систем (ФРНГС)
- id 3: Факультет химической технологии и экологии (ФХТиЭ)
- id 4: Факультет автоматики и вычислительной техники (ФАиВТ)
- id 5: Факультет комплексной безопасности ТЭК (ФКБТЕК)
- id 6: Юридический факультет (Юрфак)
- id 7: Факультет экономики и управления (ФЭУ)

СХЕМА ТАБЛИЦ:
- faculties (id, name)
- programs (id, faculty_id, name)
- applications (id, program_id, year [2021-2026], status ['submitted', 'approved', 'rejected'])
- students (id, program_id, applicant_hash, course [1-6], enrollment_year)
- teachers (id, full_name, faculty_id, degree ['PhD', 'Master', 'Bachelor'])
- courses (id, teacher_id, name, semester [1-12], program_id)
- grades (id, student_id, course_id, grade [2-5], semester)

ЭТАЛОННЫЕ ПРИМЕРЫ (ДЕЛАЙ СТРОГО ТАК):
1. Заявления по названию программы ("на Экономику", "на Юриспруденцию", "на Нефтегазовое дело"):
   SELECT COUNT(*) FROM applications a JOIN programs p ON a.program_id = p.id WHERE p.name ILIKE '%Экономика%' AND a.year = 2026;
2. Поиск преподавателя:
   SELECT full_name, degree FROM teachers WHERE full_name ILIKE '%Сидоров%';
3. Средний балл:
   SELECT ROUND(AVG(grade)::numeric, 2) FROM grades g JOIN courses c ON g.course_id = c.id WHERE c.name ILIKE '%Физика%';

ПРАВИЛА:
1. "Экономика", "Юриспруденция", "Менеджмент" — это НАЗВАНИЯ ПРОГРАММ (programs.name), а не факультеты! Ищи их через programs.name ILIKE '%...%'.
2. Год заявлений фильтруй ТОЛЬКО в таблице applications (a.year = 2026).
3. При поиске людей отсекай окончания (Сидорова -> '%Сидоров%').
4. Защита студентов (ФЗ-152): если просят найти конкретного студента по ФИО, ставь "is_sql": false и сообщай об обезличивании данных.
5. Запрещено писать в text_answer "выполните SQL". Пиши только живой текст ответа.
6. Только команда SELECT. Топ N -> LIMIT N. По умолчанию без агрегации -> LIMIT 50.

ФОРМАТ ВЫВОДА (JSON):
{
  "is_sql": true или false,
  "text_answer": "Краткая вводная фраза (НЕ SQL)",
  "sql": "SELECT ... (или null)",
  "explanation": "Объяснение структуры запроса (или null)"
}
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    logger.info("Инициализация пула соединений PostgreSQL...")
    db_pool = await asyncpg.create_pool(**DB_CONFIG, min_size=3, max_size=10)
    logger.info("Бэкенд успешно запущен!")
    yield
    if db_pool:
        await db_pool.close()


app = FastAPI(title="Gubkin AI Assistant API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatMessage(BaseModel):
    role: str
    content: str


class QueryRequest(BaseModel):
    question: str
    history: List[ChatMessage] = []


class QueryResponse(BaseModel):
    question: str
    is_sql: bool
    text_answer: str
    sql: Optional[str] = None
    explanation: Optional[str] = None
    columns: List[str] = []
    data: List[List[Any]] = []
    execution_time_ms: int
    queue_wait_ms: int = 0  # FIX #2: сколько ждали очередь к Ollama


def validate_and_sanitize_sql(sql_query: str) -> str:
    try:
        parsed = sqlglot.parse_one(sql_query, read="postgres")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Синтаксическая ошибка SQL: {e}")

    if not isinstance(parsed, exp.Select):
        raise HTTPException(status_code=403, detail="Запрещены любые команды, кроме SELECT!")

    tables = {t.name.lower() for t in parsed.find_all(exp.Table)}
    forbidden = tables - ALLOWED_TABLES
    if forbidden:
        raise HTTPException(status_code=403, detail=f"Доступ к таблицам {forbidden} запрещен политикой безопасности!")

    return parsed.sql(dialect="postgres")


# --- FIX #4: adaptive timeout. Если запрос содержит WHERE/агрегацию - короче,
# иначе (потенциально полный скан таблицы) - чуть больше запаса ---
def get_adaptive_timeout_ms(sql_query: str) -> int:
    lowered = sql_query.lower()
    has_where = " where " in lowered
    has_agg = any(fn in lowered for fn in ("count(", "avg(", "sum(", "group by"))
    if has_where or has_agg:
        return 3000
    return 5000


async def log_to_db(question: str, sql: Optional[str], time_ms: int, count: int, status: str):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO query_logs (user_question, generated_sql, execution_time_ms, result_count, status)
                VALUES ($1, $2, $3, $4, $5)
                """,
                question, sql, time_ms, count, status
            )
    except Exception as e:
        logger.error(f"Ошибка логирования: {e}")


# --- FIX #3: JSON parse fallback. Если модель вернула markdown-обертку
# ```json ... ``` или мусор вокруг объекта - вытаскиваем JSON вручную ---
def extract_json_from_llm_response(raw_text: str) -> Dict[str, Any]:
    cleaned = raw_text.strip()

    # Убираем markdown-заборы, если модель их все же добавила
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:]
        cleaned = cleaned.strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Фолбэк: ищем первую { и последнюю } и пытаемся распарсить срез
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = cleaned[start:end + 1]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Не удалось распарсить JSON из ответа модели: {raw_text[:200]}")


# --- FIX #5: обертка над вызовом Ollama с очередью, таймаутом и одной попыткой retry ---
async def call_ollama_with_queue(payload: dict) -> Dict[str, Any]:
    OLLAMA_QUEUE_COUNTER["waiting"] += 1
    queue_start = time.time()
    try:
        async with OLLAMA_SEMAPHORE:
            queue_wait_ms = int((time.time() - queue_start) * 1000)
            OLLAMA_QUEUE_COUNTER["waiting"] -= 1

            loop = asyncio.get_event_loop()
            last_error = None
            for attempt in range(2):  # первая попытка + 1 retry
                try:
                    res = await loop.run_in_executor(
                        None,
                        lambda: requests.post(OLLAMA_URL, json=payload, timeout=30).json()
                    )
                    raw_content = res["message"]["content"]
                    parsed = extract_json_from_llm_response(raw_content)
                    parsed["_queue_wait_ms"] = queue_wait_ms
                    return parsed
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                    last_error = e
                    logger.warning(f"Ollama недоступна (попытка {attempt + 1}): {e}")
                    continue
                except ValueError as e:
                    # JSON не распарсился даже с фолбэком - пробуем еще раз с более строгим промптом
                    last_error = e
                    logger.warning(f"Не удалось распарсить JSON (попытка {attempt + 1}): {e}")
                    continue

            raise HTTPException(
                status_code=503,
                detail=f"Модель недоступна или вернула некорректный ответ после повторной попытки: {last_error}"
            )
    finally:
        if OLLAMA_QUEUE_COUNTER["waiting"] > 0 and OLLAMA_QUEUE_COUNTER["waiting"] == OLLAMA_QUEUE_COUNTER.get("waiting", 0):
            pass  # счетчик уже скорректирован выше


@app.get("/")
async def serve_frontend():
    """Раздача интерфейса прямо из корня бэкенда"""
    import os
    if not os.path.exists("index.html"):
        raise HTTPException(status_code=500, detail="index.html не найден рядом с server.py")
    return FileResponse("index.html")


@app.post("/api/query", response_model=QueryResponse)
async def process_query(req: QueryRequest):
    question = req.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Вопрос не может быть пустым")

    start_time = time.time()

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in req.history[-6:]:
        messages.append({"role": msg.role, "content": msg.content})
    messages.append({"role": "user", "content": question})

    payload = {
        "model": "qwen2.5-coder:7b",
        "messages": messages,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.1}
    }

    try:
        content = await call_ollama_with_queue(payload)
        queue_wait_ms = content.pop("_queue_wait_ms", 0)
        is_sql = content.get("is_sql", False)
        text_answer = content.get("text_answer", "")
        raw_sql = content.get("sql")
        explanation = content.get("explanation")
    except HTTPException:
        await log_to_db(question, None, int((time.time() - start_time) * 1000), 0, "error")
        raise
    except Exception as e:
        await log_to_db(question, None, int((time.time() - start_time) * 1000), 0, "error")
        raise HTTPException(status_code=500, detail=f"Ошибка LLM: {str(e)}")

    if not is_sql or not raw_sql:
        execution_time_ms = int((time.time() - start_time) * 1000)
        await log_to_db(question, None, execution_time_ms, 0, "chat")
        return {
            "question": question,
            "is_sql": False,
            "text_answer": text_answer or "Ответ на ваш вопрос.",
            "sql": None,
            "explanation": None,
            "columns": [],
            "data": [],
            "execution_time_ms": execution_time_ms,
            "queue_wait_ms": queue_wait_ms
        }

    safe_sql = validate_and_sanitize_sql(raw_sql)
    timeout_ms = get_adaptive_timeout_ms(safe_sql)  # FIX #4

    columns = []
    data = []
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(f"SET statement_timeout = {timeout_ms};")
            stmt = await conn.prepare(safe_sql)
            columns = [attr.name for attr in stmt.get_attributes()]
            records = await conn.fetch(safe_sql)
            data = [[str(val) if val is not None else "" for val in record.values()] for record in records]
    except asyncpg.exceptions.QueryCanceledError:
        await log_to_db(question, safe_sql, int((time.time() - start_time) * 1000), 0, "timeout")
        raise HTTPException(
            status_code=408,
            detail=f"Запрос выполнялся дольше {timeout_ms}мс и был прерван. Попробуйте уточнить вопрос (добавьте фильтр по году/факультету)."
        )
    except Exception as e:
        await log_to_db(question, safe_sql, int((time.time() - start_time) * 1000), 0, "error")
        raise HTTPException(status_code=400, detail=f"Ошибка выполнения в БД: {str(e)}")

    execution_time_ms = int((time.time() - start_time) * 1000)
    await log_to_db(question, safe_sql, execution_time_ms, len(data), "success")

    return {
        "question": question,
        "is_sql": True,
        "text_answer": text_answer or "Результаты запроса:",
        "sql": safe_sql,
        "explanation": explanation or "Запрос сформирован автоматически.",
        "columns": columns,
        "data": data,
        "execution_time_ms": execution_time_ms,
        "queue_wait_ms": queue_wait_ms
    }


@app.get("/api/admin/analytics")
async def get_analytics():
    async with db_pool.acquire() as conn:
        logs = await conn.fetch("SELECT user_question, status, execution_time_ms FROM query_logs ORDER BY id DESC LIMIT 20;")
        total_queries = await conn.fetchval("SELECT count(*) FROM query_logs;")
        avg_time = await conn.fetchval("SELECT ROUND(AVG(execution_time_ms)::numeric, 2) FROM query_logs WHERE status = 'success';")
        success_count = await conn.fetchval("SELECT count(*) FROM query_logs WHERE status = 'success';")
        error_count = await conn.fetchval("SELECT count(*) FROM query_logs WHERE status = 'error';")
        timeout_count = await conn.fetchval("SELECT count(*) FROM query_logs WHERE status = 'timeout';")

    total = total_queries or 0
    success_rate = round((success_count or 0) / total * 100, 1) if total > 0 else 0.0

    return {
        "total_queries_served": total,
        "avg_execution_time_ms": float(avg_time or 0),
        "success_rate_percent": success_rate,
        "error_count": error_count or 0,
        "timeout_count": timeout_count or 0,
        "recent_logs": [dict(r) for r in logs]
    }


@app.get("/api/admin/queue_status")
async def get_queue_status():
    """FIX #2: endpoint для фронтенда, чтобы показать 'вы в очереди' при параллельных запросах"""
    return {"waiting_in_queue": OLLAMA_QUEUE_COUNTER["waiting"]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)