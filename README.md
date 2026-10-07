<div align="center">

# 🛢️ Gubkin Private NLP-to-SQL Assistant

**Защищённая система трансляции естественного языка в аналитические SQL-запросы**

Развёрнута в закрытом On-Premise контуре университета

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Ollama](https://img.shields.io/badge/Ollama-Local_LLM-000000?logo=ollama&logoColor=white)
![Security](https://img.shields.io/badge/Security-Defense--in--Depth-critical)
![On-Premise](https://img.shields.io/badge/Deploy-On--Premise-success)

</div>

---

## 📌 О проекте

**Gubkin Private NLP-to-SQL Assistant** — корпоративное решение для работы с базой данных университета без знания SQL.

Система переводит запросы на естественном русском языке в безопасные SQL-выборки и возвращает результат в виде таблицы вместе с пояснением (Explainable AI) и телеметрией отклика.

### Ключевые преимущества

- **100% On-Premise** — данные не покидают локальный контур, нет зависимости от внешних провайдеров.
- **Privacy by Design** — персональные данные обучающихся обезличены на уровне схемы БД (с учётом требований 152-ФЗ).
- **Быстрый отклик** — локальный GPU-инференс и In-Memory кэширование типовых запросов.
- **Прозрачность** — к каждому ответу прилагаются SQL, объяснение и время выполнения.

---

## 🔒 Архитектура безопасности (Defense-in-Depth)

Защита построена на трёх независимых уровнях.

### 1. Изоляция на уровне схемы БД

- В схеме нет ФИО, паспортных данных, телефонов и контактов обучающихся.
- Сущности идентифицируются через синтетические ID и криптографические хэши (`applicant_hash`).
- Данные преподавателей публикуются в рамках регламента открытости информации вуза.

### 2. AST-валидация запросов

SQL, сгенерированный моделью, проверяется парсером [`sqlglot`](https://github.com/tobymao/sqlglot) до обращения к СУБД:

- разрешены только одиночные выражения `SELECT`;
- запрещены multi-statement запросы, инъекции и любые DDL/DML (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`);
- таблицы проверяются по белому списку;
- доступ к `pg_catalog` и `information_schema` заблокирован.

### 3. Ограничения на уровне СУБД

- Подключение под сервисной ролью `bot_readonly`: `SELECT` на все таблицы и `INSERT` только в `query_logs` (журнал аудита).
- Принудительный таймаут: `SET statement_timeout = '3000ms'` защищает от тяжёлых выборок.
- Автоматический `LIMIT 50` защищает память узла.

---

## 🏛 Поток данных

```text
[Клиентский интерфейс / Веб-виджет]
              │
              │ HTTP POST /api/query (вопрос + контекст диалога)
              ▼
[FastAPI Gateway]
  ├── In-Memory Semantic Cache
  ├── Менеджер сессий (multi-turn память)
  │
  ├── 1. Генерация SQL ──► [Ollama: Qwen 2.5 Coder 7B]
  │   2. Непроверенный SQL ◄──┘
  │
  ├── 3. AST-аудит ──► [sqlglot Validator]
  │   4. Проверенный SQL ◄──┘
  │
  └── 5. Выполнение ──► [PostgreSQL 16]
                          │ (read-only роль, timeout 3s)
      6. Формирование ответа ◄──┘
      ├── Запись телеметрии в query_logs
      └── JSON: данные + Explainable AI + телеметрия
```

---

## 📦 Схема базы данных

Схема нормализована, содержит внешние ключи, каскадные правила и B-Tree индексы.

| Таблица | Описание | Безопасность |
| :-- | :-- | :-- |
| `faculties` | Факультеты | Открытые данные |
| `programs` | Образовательные программы | Открытые данные |
| `teachers` | Преподаватели (ФИО, степени) | Публичные данные по регламенту вуза |
| `students` | Контингент (курс, хэш, год) | **Обезличено**, ФИО отсутствуют |
| `applications` | Заявления абитуриентов (2021–2026) | Деперсонализированный реестр |
| `courses` | Дисциплины кафедр | Связь лекторов с семестрами |
| `grades` | Успеваемость студентов | Оценки 2–5 |
| `query_logs` | Журнал аудита и телеметрии | Сервисный лог безопасности |

---

## 🛠 Технологический стек

| Слой | Технологии |
| :-- | :-- |
| Бэкенд | Python 3.11+, FastAPI, Uvicorn, Pydantic |
| Драйвер БД | asyncpg (асинхронный пул соединений) |
| СУБД | PostgreSQL 16 |
| Безопасность | sqlglot (AST-парсер) |
| LLM | Ollama, `qwen2.5-coder:7b` (GPU) |
| Фронтенд | Vanilla ES6 JS, HTML5, CSS3 (изолированный виджет) |

---

## 🚀 Развёртывание

### 1. PostgreSQL в Docker

```bash
docker run --name gubkin-pg -e POSTGRES_PASSWORD=<admin_password> -p 5432:5432 -d postgres:16
docker cp database_dump.sql gubkin-pg:/database_dump.sql
docker exec -it gubkin-pg psql -U postgres -d postgres -f /database_dump.sql
```

Сервисная роль с ограниченными правами:

```bash
docker exec -it gubkin-pg psql -U postgres -d postgres -c "
CREATE USER bot_readonly WITH PASSWORD '<service_password>';
GRANT CONNECT ON DATABASE postgres TO bot_readonly;
GRANT USAGE ON SCHEMA public TO bot_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bot_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bot_readonly;
GRANT INSERT ON TABLE query_logs TO bot_readonly;
GRANT USAGE, SELECT ON SEQUENCE query_logs_id_seq TO bot_readonly;"
```

> ⚠️ Замените `<admin_password>` и `<service_password>` на собственные значения и не храните реальные пароли в репозитории.

### 2. Локальная LLM

```bash
ollama pull qwen2.5-coder:7b
ollama serve
```

### 3. Бэкенд

```bash
pip install -r requirements.txt
python server.py
```

После запуска:

- API: http://localhost:8000
- Swagger UI: http://localhost:8000/docs
- Аналитика и аудит: http://localhost:8000/api/admin/analytics

### 4. Клиентский виджет

Откройте `index.html` в браузере. Виджет встраивается на любой портал подключением одного скрипта.

---

## 🔌 API

### `POST /api/query`

Основной эндпоинт обработки запросов.

**Запрос:**

```json
{
  "question": "Сколько заявлений подано на Экономику в 2026 году?",
  "history": [
    {"role": "user", "content": "Привет"},
    {"role": "assistant", "content": "Здравствуйте! Чем могу помочь?"}
  ]
}
```

**Ответ:**

```json
{
  "question": "Сколько заявлений подано на Экономику в 2026 году?",
  "is_sql": true,
  "text_answer": "Результаты запроса:",
  "sql": "SELECT COUNT(*) FROM applications AS a JOIN programs AS p ON a.program_id = p.id WHERE p.faculty_id = 7 AND a.year = 2026",
  "explanation": "Подсчёт заявлений с фильтром по факультету экономики (id 7) и 2026 году.",
  "columns": ["count"],
  "data": [["101"]],
  "execution_time_ms": 1150
}
```

### `GET /api/admin/analytics`

Служебный эндпоинт аудита и мониторинга SLA: общее число обращений, среднее время отклика, последние события безопасности.