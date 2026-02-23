# Tender Risk Radar — Hybrid AI Pipeline

Система анализа рисков государственных закупок Казахстана на основе данных [goszakup.gov.kz](https://goszakup.gov.kz).

## Анализ манипулятивных спецификаций

В рамках темы хакатона «Госзакупки» реализован анализ технических требований на признаки подгонки под поставщика:

- **LLM** анализирует текст лота (name_ru, name_kz) на указание конкретных брендов, моделей или узких параметров
- Результат: **РИСК: ДА/НЕТ** + краткое обоснование
- Отображается в Lot Detail при нажатии «Объяснить»

## Architecture

```
 ┌──────────────┐   GraphQL    ┌──────────────┐   SQL    ┌──────────────┐
 │ OWS V3 API   │◄────────────►│  ETL Layer   │────────►│  PostgreSQL  │
 │ (goszakup)   │              │ (backfill +  │         │  (14 tables) │
 └──────────────┘              │  incremental)│         └──────┬───────┘
                               └──────────────┘                │
                                                               ▼
                               ┌──────────────┐   31    ┌──────────────┐
                               │  Feature     │────────►│  Risk Score  │
                               │  Engine      │ rules   │  (0-100)     │
                               │  (31 indic.) │         │  + ML proba  │
                               └──────────────┘         └──────┬───────┘
                                                               │
                                     ┌─────────────────────────┤
                                     ▼                         ▼
                               ┌──────────────┐         ┌──────────────┐
                               │  ML Model    │         │  LLM Explain │
                               │  (LogReg)    │         │  (OpenAI)    │
                               │  local .pkl  │         │  PII-masked  │
                               └──────────────┘         └──────────────┘
```

### Score Formula

```
risk_final = round(100 × (0.6 × risk_rules/100 + 0.4 × risk_ml))
```

- **risk_rules** (0-100): weighted sum of 31 boolean indicators
- **risk_ml** (0-1): P(risky) from local Logistic Regression
- **LLM**: only for explanation, NEVER for scoring

## Quick Start

```bash
# 1. Clone & configure
cp .env.example .env
# Edit .env with your OWS_TOKEN, POSTGRES_PASSWORD, JWT_SECRET, OPENAI_API_KEY

# 2. Start all services
docker-compose up -d

# 3. Run database migrations
docker-compose exec backend alembic upgrade head

# 4. Create admin user (first time)
docker-compose exec backend python -c "
from app.core.security import create_user
import asyncio
asyncio.run(create_user('admin', 'admin123', 'admin'))
"

# 5. Trigger initial data load
curl -X POST http://localhost:8000/api/v1/admin/etl/backfill \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"date_from": "2024-01-01", "date_to": "2025-12-31"}'

# 6. Recompute features
curl -X POST http://localhost:8000/api/v1/admin/features/recompute \
  -H 'Authorization: Bearer <token>'

# 7. Train ML model (after features are computed)
docker-compose exec backend python -c "
from app.ml.train import train_model
import asyncio
asyncio.run(train_model())
"

# 8. Open frontend
open http://localhost:3000
```

## Services

| Service   | Port  | Description              |
|-----------|-------|--------------------------|
| frontend  | 3000  | Next.js dashboard        |
| backend   | 8000  | FastAPI REST API         |
| postgres  | 5432  | PostgreSQL               |
| redis     | 6379  | Cache + Celery broker    |
| worker    |   —   | Celery background tasks  |
| beat      |   —   | Celery periodic schedule |

## API Endpoints

| Method | Path                           | Description                    |
|--------|--------------------------------|--------------------------------|
| POST   | `/auth/login`                  | JWT login                      |
| GET    | `/dashboard`                   | Paginated lots with scores     |
| GET    | `/lots/{id}`                   | Lot detail + flags + evidence  |
| GET    | `/explain/lots/{id}/explain`   | AI explanation (LLM)           |
| GET    | `/tenders/{id}`                | Tender detail with lots        |
| GET    | `/suppliers/{biin}`            | Supplier profile + RNU         |
| GET    | `/customers/{bin}`             | Customer profile + risk lots   |
| POST   | `/notes`                       | Create analyst note/label      |
| POST   | `/admin/etl/backfill`          | Trigger ETL backfill           |
| POST   | `/admin/etl/incremental`       | Trigger incremental ETL        |
| GET    | `/admin/etl/status`            | ETL run history                |
| POST   | `/admin/features/recompute`    | Recompute all features         |

## 31 Risk Indicators

| #  | Code                          | Level  | Description                              |
|----|-------------------------------|--------|------------------------------------------|
| 1  | LOT_SPLITTING                 | MEDIUM | Разбивка крупной закупки на мелкие лоты  |
| 2  | SHORT_DEADLINE                | MEDIUM | Срок приёма заявок < 3 дней              |
| 3  | FEW_BIDS                      | HIGH   | Только 1-2 заявки                         |
| 4  | RECURRING_WINNER              | HIGH   | Один поставщик > 70% побед               |
| 5  | COMMON_REQUISITES             | HIGH   | Общие телефоны/email у участников         |
| 6  | NEW_COMPANY_BIG_CONTRACT      | HIGH   | Молодая компания + крупный контракт       |
| 7  | CAROUSEL_PATTERN              | HIGH   | Ротация победителей                       |
| 8  | RNU_FLAG                      | HIGH   | В реестре недобросовестных                 |
| 9  | DUMPING_FLAG                   | MEDIUM | Демпинг цены                              |
| 10 | IDENTICAL_BID_PRICES          | MEDIUM | Одинаковые цены у разных участников       |
| 11 | TINY_WIN_MARGIN               | MEDIUM | Разница < 1%                              |
| 12 | LATE_BID_SUBMISSION           | LOW    | Заявки за < 60 мин до дедлайна            |
| 13 | REPEAT_TENDER                 | MEDIUM | Повторный тендер                           |
| 14 | CANCELLED_TENDER              | MEDIUM | Тендер отменён                             |
| 15 | PAUSED_TENDER                 | MEDIUM | Тендер приостановлен                       |
| 16 | NIGHT_OR_WEEKEND_PUBLISH      | LOW    | Публикация в нерабочее время               |
| 17 | SHORT_DISCUSSION_WINDOW       | LOW    | Обсуждение < 2 дней                        |
| 18 | LAST_MINUTE_CHANGES           | MEDIUM | Изменения < 24ч до дедлайна                |
| 19 | SUPPLIER_CONCENTRATION        | MEDIUM | > 80% контрактов с одним заказчиком        |
| 20 | CUSTOMER_WINNER_CONCENTRATION | MEDIUM | Топ-3 поставщика > 80%                     |
| 21 | HIGH_WIN_RATE_FEW_BIDS        | HIGH   | Win-rate > 90%, avg bids < 3               |
| 22 | ADDENDUM_VALUE_INCREASE       | HIGH   | Допсоглашение + 20%                        |
| 23 | WIN_MIN_THEN_ADDENDUM         | HIGH   | Допсоглашение через 30 дней                |
| 24 | WEIRD_EXECUTION_TIME          | MEDIUM | < 7 или > 730 дней                         |
| 25 | HIGH_PREPAY                   | MEDIUM | Аванс > 50%                                |
| 26 | PAYMENTS_EXCEED_CONTRACT      | HIGH   | Платежи > суммы контракта                   |
| 27 | PAYMENT_WITHOUT_ACT           | HIGH   | Платежи без актов                           |
| 28 | OVERDUE_EXECUTION             | HIGH   | Просрочка исполнения                        |
| 29 | FINES_PRESENT                 | HIGH   | Штрафы в актах                              |
| 30 | LOW_EXECUTION_RATE            | MEDIUM | Исполнение < 50%                            |
| 31 | BANK_DETAILS_REUSE            | HIGH   | Общие реквизиты у разных компаний           |

## Testing

```bash
cd backend
python -m pytest tests/ -v
```

## Tech Stack

- **Backend**: FastAPI, SQLAlchemy, asyncpg, Celery, Redis
- **Frontend**: Next.js 14, Tailwind CSS, lucide-react
- **ML**: scikit-learn (LogisticRegression), joblib
- **LLM**: OpenAI API (gpt-4o-mini) — explanation only
- **Database**: PostgreSQL 15
- **ETL**: OWS V3 GraphQL with limit/after pagination
