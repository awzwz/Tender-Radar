# Deploy: backend on AWS EC2 (free tier)

The backend (FastAPI + Celery worker + beat + Redis) runs on a single EC2
instance via Docker, behind Caddy (automatic HTTPS). **Postgres stays on
Supabase, frontend stays on Vercel** — they are not touched.

## Prerequisites
- An EC2 instance: Ubuntu 22.04/24.04, **t3.micro** (free tier), 20 GB disk.
- Security group inbound: **22 (SSH), 80 (HTTP), 443 (HTTPS)**.
- A domain/subdomain for the API (e.g. `api.tender-radar.online`) with an
  **A record → instance public IP** (required for HTTPS; the Vercel frontend
  is HTTPS and the browser needs an HTTPS backend).
- The production `.env` (already prepared locally from Railway).

## Steps
1. SSH in and provision:
   ```bash
   bash deploy/setup-ec2.sh        # installs Docker, swap, clones repo
   ```
2. Copy the production `.env` to `~/Tender-Radar/.env`.
3. Deploy:
   ```bash
   cd ~/Tender-Radar
   export DOMAIN=api.tender-radar.online
   docker compose -f deploy/docker-compose.prod.yml up -d --build
   ```
   Caddy will obtain a TLS cert automatically once the A record resolves.
4. Point the frontend at the new backend — on **Vercel** set
   `NEXT_PUBLIC_API_URL = https://api.tender-radar.online/api/v1`, and make sure
   `CORS_ORIGINS` in `.env` includes the Vercel domain. Redeploy the frontend.

## Notes
- Migrations run automatically (`alembic upgrade head`) on backend startup;
  against Supabase this is a no-op when already at head.
- Lean config for 1 GB RAM: 2 uvicorn workers, Celery concurrency 1, Redis
  capped at 128 MB, plus a 2 GB swap file. If memory is tight, drop uvicorn to
  `--workers 1`.
- Logs: `docker compose -f deploy/docker-compose.prod.yml logs -f backend`
- Update code: `git pull && docker compose -f deploy/docker-compose.prod.yml up -d --build`
