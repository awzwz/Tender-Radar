volumes="tender-radar_postgres_data fire_postgres_data frauddetectionprediplomawork_postgres_data tender_radar_alt_postgres_data tender-radar-main-3_postgres_data"
for vol in $volumes; do
  echo "--- Checking volume: $vol ---"
  docker run --name temp_scan --rm -d -v "$vol:/var/lib/postgresql/data" -e POSTGRES_PASSWORD=pass postgres:15-alpine > /dev/null
  sleep 10
  
  # Get all database names
  databases=$(docker exec temp_scan psql -U postgres -t -c "SELECT datname FROM pg_database WHERE datistemplate = false;" | xargs)
  
  for db in $databases; do
    echo "  DB: $db"
    docker exec temp_scan psql -U postgres -d "$db" -c "SELECT relname, n_live_tup FROM pg_stat_user_tables WHERE n_live_tup > 0;"
  done
  
  docker stop temp_scan > /dev/null
done
