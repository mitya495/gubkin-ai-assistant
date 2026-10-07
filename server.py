import time
import json
import logging
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
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

db_pool: Optional[asyncpg.Pool] = None

SYSTEM_PROMPT = """Ты — интеллектуальный ассистент базы данных университета Губкина. Ты общаешься с пользователем и переводишь его вопросы в безопасные PostgreSQL запросы.

СПРАВОЧНИК ФАКУЛЬТЕТОВ (используй эти точные ID и названия):
- id 1: Факультет разработки нефтяных и газовых месторождений (ФРНиГМ)
- id 2: Факультет разработки нефтегазовых систем (ФРНГС)
- id 3: Факультет химической технологии и экологии (ФХТиЭ)
- id 4: Факультет автоматики и вычислительной техники (ФАиВТ)
- id 5: Факультет комплексной безопасности ТЭК (ФКБТЕК)
- id 6: Юридический факультет (Юрфак)
- id 7: Факультет экономики и управления (ФЭУ)

СХЕМА ТАБЛИЦ:
- faculties (id, name)
- programs (id, faculty_id, name, year_started)
- applications (id, program_id, year [2021-2026], status ['submitted', 'approved', 'rejected'])
- students (id, program_id, applicant_hash, course [1-6], enrollment_year)
- teachers (id, full_name, faculty_id, degree ['PhD', 'Master', 'Bachelor'])
- courses (id, teacher_id, name, semester [1-12], program_id)
- grades (id, student_id, course_id, grade [2-5], semester)

ПРАВИЛА И ОГРАНИЧЕНИЯ:
1. ФОРМУЛИРОВКА ОТВЕТА (КРИТИЧНО!):
   В поле text_answer пиши краткую человеческую подводку (например: "Вот найденные данные:", "Результаты запроса:", "Статистика по вашему вопросу:").
   СТРОГО ЗАПРЕЩЕНО писать фразы вроде "выполните следующий SQL-запрос"! Система выполняет запрос сама!

2. ПОИСК ЛЮДЕЙ ПО ФАМИЛИИ:
   ФИО в базе есть ТОЛЬКО у преподавателей (teachers). Поиск человека по имени/фамилии — это ВСЕГДА поиск по teachers.full_name.
   Отсекай окончания склонений до основы: "Сидорова" -> ILIKE '%Сидоров%'.

3. ЗАЩИТА СТУДЕНТОВ (ФЗ-152):
   ТОЛЬКО если пользователь ЯВНО написал слово "студент" вместе с фамилией (например: "найди студента Сидорова"):
   Установи "is_sql": false и ответь:
   "В соответствии с ФЗ-152 и регламентом безопасности университета, данные студентов строго обезличены (ФИО отсутствуют в базе). Поиск конкретных студентов по фамилии недоступен. Доступна только общая статистика."

4. АББРЕВИАТУРЫ:
   Если в вопросе звучит аббревиатура (ФАиВТ, Юрфак, ФЭУ и т.д.) — это ФАКУЛЬТЕТ. Используй его ID или поиск по faculties.name. Никогда не ищи это в именах преподавателей!

5. ГОД ЗАЯВЛЕНИЙ:
   Год подачи заявлений находится в applications.year (НЕ путать с year_started у программ!).

6. СТАНДАРТЫ SQL:
   - ТОЛЬКО команда SELECT.
   - Поиск строк через ILIKE '%...%'.
   - В подзапросах используй IN, а не '='.
   - Средний балл: ROUND(AVG(grade)::numeric, 2).
   - Топ N -> LIMIT N. По умолчанию без агрегации -> LIMIT 50.

ФОРМАТ ВЫВОДА (JSON):
{
  "is_sql": true или false,
  "text_answer": "Краткая фраза ответа (НЕ SQL-код)",
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

@app.post("/api/query", response_model=QueryResponse)
async def process_query(req: QueryRequest):
    question = req.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Вопрос не может быть пустым")

    start_time = time.time()

    # Собираем контекст диалога
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
        res = requests.post(OLLAMA_URL, json=payload, timeout=30).json()
        content = json.loads(res["message"]["content"])
        is_sql = content.get("is_sql", False)
        text_answer = content.get("text_answer", "")
        raw_sql = content.get("sql")
        explanation = content.get("explanation")
    except Exception as e:
        await log_to_db(question, None, int((time.time() - start_time)*1000), 0, "error")
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
            "execution_time_ms": execution_time_ms
        }

    # Валидация SQL
    safe_sql = validate_and_sanitize_sql(raw_sql)

    # Выполнение в PostgreSQL
    columns = []
    data = []
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("SET statement_timeout = 3000;")
            stmt = await conn.prepare(safe_sql)
            columns = [attr.name for attr in stmt.get_attributes()]
            records = await conn.fetch(safe_sql)
            data = [[str(val) if val is not None else "" for val in record.values()] for record in records]
    except Exception as e:
        await log_to_db(question, safe_sql, int((time.time() - start_time)*1000), 0, "error")
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
        "execution_time_ms": execution_time_ms
    }

@app.get("/api/admin/analytics")
async def get_analytics():
    async with db_pool.acquire() as conn:
        logs = await conn.fetch("SELECT user_question, status, execution_time_ms FROM query_logs ORDER BY id DESC LIMIT 20;")
        total_queries = await conn.fetchval("SELECT count(*) FROM query_logs;")
        avg_time = await conn.fetchval("SELECT ROUND(AVG(execution_time_ms)::numeric, 2) FROM query_logs WHERE status = 'success';")
        
    return {
        "total_queries_served": total_queries or 0,
        "avg_execution_time_ms": float(avg_time or 0),
        "recent_logs": [dict(r) for r in logs]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)