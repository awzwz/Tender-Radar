#!/usr/bin/env bash
# One-time provisioning for a fresh Ubuntu 22.04/24.04 EC2 instance (t3.micro).
# Installs Docker + compose plugin, adds a swap file (critical on 1 GB RAM),
# clones the repo, and prints next steps. Run as the default 'ubuntu' user:
#   curl -fsSL <raw-url>/deploy/setup-ec2.sh | bash
# or: bash deploy/setup-ec2.sh
set -euo pipefail

echo "==> Adding 2 GB swap (safety for 1 GB RAM)..."
if [ ! -f /swapfile ]; then
  sudo fallocate -l 2G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
fi

echo "==> Installing Docker..."
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
fi

echo "==> Cloning repo..."
if [ ! -d "$HOME/Tender-Radar" ]; then
  git clone https://github.com/awzwz/Tender-Radar.git "$HOME/Tender-Radar"
fi

cat <<'NEXT'

==> Provisioning done. Next steps:
  1. Put the production .env in ~/Tender-Radar/.env
     (copy from .env.example or scp the prepared one; ensure REDIS_URL is
      redis://redis:6379/0 — the compose overrides it anyway).
  2. Point an A record (e.g. api.your-domain.com) at this instance's public IP.
  3. Open ports 22, 80, 443 in the EC2 security group.
  4. Deploy:
       cd ~/Tender-Radar
       export DOMAIN=api.your-domain.com
       newgrp docker   # or log out/in so docker group applies
       docker compose -f deploy/docker-compose.prod.yml up -d --build
  5. Update Vercel env NEXT_PUBLIC_API_URL = https://api.your-domain.com/api/v1
     and CORS_ORIGINS in .env to include the Vercel domain. Redeploy frontend.

  Check:  curl https://api.your-domain.com/api/v1/prices/stats  (after login token)
  Logs:   docker compose -f deploy/docker-compose.prod.yml logs -f backend
NEXT
