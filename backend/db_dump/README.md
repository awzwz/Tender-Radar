# Дамп БД Tender Radar

## Восстановление (для сокомандника)

1. Запустите контейнеры (Postgres должен быть запущен первым):
   ```bash
   cd /path/to/project
   docker compose up -d postgres redis
   ```
   Дождитесь, пока Postgres станет healthy.

2. Восстановите дамп в БД:
   ```bash
   # Если есть файл backup.sql в этой папке:
   docker exec -i tender_radar_postgres psql -U postgres -d tender_radar < backend/db_dump/backup.sql

   # Или если дамп делали через скрипт и положили в backend/db_dump/backup.sql
   cat backend/db_dump/backup.sql | docker exec -i tender_radar_postgres psql -U postgres -d tender_radar
   ```

3. Скопируйте `.env.example` в `.env` и заполните переменные (пароль БД, JWT, OPENAI_API_KEY и т.д.).

4. Запустите backend и фронт:
   ```bash
   docker compose up -d
   # или локально: backend — uvicorn, frontend — npm run dev
   ```

## Создание дампа (для того, у кого уже есть заполненная БД)

Из корня проекта:

```bash
docker exec tender_radar_postgres pg_dump -U postgres --no-owner --no-acl tender_radar > backend/db_dump/backup.sql
```

Либо используйте скрипт (если добавлен):

```bash
./scripts/dump_db.sh
```

После создания дампа закоммитьте `backend/db_dump/backup.sql` и запушьте, чтобы сокомандник мог его скачать и восстановить.
