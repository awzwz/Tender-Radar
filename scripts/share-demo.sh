#!/bin/bash
# Скрипт для демо — запускает localtunnel и даёт ссылку для шаринга
cd "$(dirname "$0")/.."
echo "Запуск туннелей..."
echo ""

# Backend
(npx --yes localtunnel --port 8000 2>&1 | tee /tmp/lt-backend.log) &
sleep 5
# Frontend  
(npx --yes localtunnel --port 3002 2>&1 | tee /tmp/lt-frontend.log) &
sleep 5

BACKEND_URL=$(grep -o 'https://[^[:space:]]*\.loca\.lt' /tmp/lt-backend.log | head -1)
FRONTEND_URL=$(grep -o 'https://[^[:space:]]*\.loca\.lt' /tmp/lt-frontend.log | head -1)

if [ -z "$FRONTEND_URL" ]; then
  echo "Ошибка: не удалось получить URL"
  exit 1
fi

echo ""
echo "Обновляю .env..."
# Добавить URL в CORS и NEXT_PUBLIC_API_URL
export BACKEND_URL
export FRONTEND_URL
export API_URL="${BACKEND_URL}/api/v1"

# Использовать envsubst или sed
if [ -n "$BACKEND_URL" ] && [ -n "$FRONTEND_URL" ]; then
  # Бэкап
  cp .env .env.bak 2>/dev/null || true
  # Обновить CORS — добавить новые URL если их нет
  if ! grep -q "loca.lt" .env; then
    sed -i.bak "s|CORS_ORIGINS=.*|CORS_ORIGINS=http://localhost:3000,http://localhost:3001,http://localhost:3002,http://127.0.0.1:3000,http://127.0.0.1:3001,http://127.0.0.1:3002,${BACKEND_URL},${FRONTEND_URL}|" .env 2>/dev/null || true
  fi
  # NEXT_PUBLIC_API_URL
  if grep -q "^NEXT_PUBLIC_API_URL=" .env; then
    sed -i.bak "s|^NEXT_PUBLIC_API_URL=.*|NEXT_PUBLIC_API_URL=${API_URL}|" .env 2>/dev/null || true
  else
    echo "NEXT_PUBLIC_API_URL=${API_URL}" >> .env
  fi
  docker compose restart backend frontend 2>/dev/null || true
fi

echo ""
echo "=============================================="
echo "  ССЫЛКА ДЛЯ ДЕМО (поделитесь ей):"
echo "  $FRONTEND_URL"
echo "=============================================="
echo ""
echo "При первом заходе localtunnel может показать «Click to Continue» — нажмите."
echo "Туннели работают. Ctrl+C для остановки."
wait
