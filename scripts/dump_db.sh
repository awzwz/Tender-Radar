#!/bin/bash
# Создаёт дамп PostgreSQL в backend/db_dump/backup.sql
# Запускать из корня проекта. Контейнер postgres должен быть запущен.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DUMP_DIR="$ROOT_DIR/backend/db_dump"
mkdir -p "$DUMP_DIR"

echo "Dumping tender_radar DB to $DUMP_DIR/backup.sql ..."
docker exec tender_radar_postgres pg_dump -U postgres --no-owner --no-acl tender_radar > "$DUMP_DIR/backup.sql"
echo "Done. Commit and push backend/db_dump/backup.sql for teammates."
