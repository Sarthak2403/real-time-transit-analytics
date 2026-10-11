#!/usr/bin/env bash
# Called by EC2 user data with non-secret environment settings.
set -euo pipefail
: "${S3_BUCKET:?}" "${AWS_REGION:?}" "${EXPORT_START:?}" "${DEPLOYMENT_KEY:?}"
dnf install -y docker python3.11 python3.11-pip awscli2 curl
systemctl enable --now docker
install -d /usr/local/lib/docker/cli-plugins
COMPOSE_VERSION=${COMPOSE_VERSION:-v2.39.4}
ARCH=$(uname -m)
curl -fsSL "https://github.com/docker/compose/releases/download/$COMPOSE_VERSION/docker-compose-linux-$ARCH" -o /usr/local/lib/docker/cli-plugins/docker-compose
curl -fsSL "https://github.com/docker/compose/releases/download/$COMPOSE_VERSION/checksums.txt" -o /tmp/compose-checksums.txt
(cd /usr/local/lib/docker/cli-plugins; awk -v file="docker-compose-linux-$ARCH" '$2 == file {print $1 "  docker-compose"}' /tmp/compose-checksums.txt | sha256sum -c -)
chmod 755 /usr/local/lib/docker/cli-plugins/docker-compose
BUILDX_VERSION=${BUILDX_VERSION:-v0.26.1}
case "$ARCH" in x86_64) BUILDX_ARCH=amd64 ;; aarch64) BUILDX_ARCH=arm64 ;; *) exit 1 ;; esac
BUILDX_FILE="buildx-$BUILDX_VERSION.linux-$BUILDX_ARCH"
curl -fsSL "https://github.com/docker/buildx/releases/download/$BUILDX_VERSION/$BUILDX_FILE" -o /usr/local/lib/docker/cli-plugins/docker-buildx
curl -fsSL "https://github.com/docker/buildx/releases/download/$BUILDX_VERSION/checksums.txt" -o /tmp/buildx-checksums.txt
(cd /usr/local/lib/docker/cli-plugins; awk -v file="$BUILDX_FILE" '$2 == file {print $1 "  docker-buildx"}' /tmp/buildx-checksums.txt | sha256sum -c -)
chmod 755 /usr/local/lib/docker/cli-plugins/docker-buildx
install -d /opt/transit
aws s3 cp "s3://$S3_BUCKET/$DEPLOYMENT_KEY" /tmp/transit-source.tar.gz --region "$AWS_REGION"
tar -xzf /tmp/transit-source.tar.gz -C /opt/transit
if ! id transit-export >/dev/null 2>&1; then useradd --system --no-create-home --shell /sbin/nologin transit-export; fi
if [ ! -f /opt/transit/.env ]; then
    umask 077
    python3.11 - <<'PYENV'
import os,secrets
from pathlib import Path
Path('/opt/transit/.env').write_text('POSTGRES_DB=transit\nPOSTGRES_USER=transit\nPOSTGRES_PASSWORD='+secrets.token_hex(32)+'\nEVENTS_PER_SECOND=250\nVEHICLE_COUNT=100\nROUTE_COUNT=10\n')
PYENV
fi
python3.11 - <<'PYENV'
import os
from pathlib import Path
p=Path('/opt/transit/.env')
lines=[line for line in p.read_text().splitlines() if not line.startswith(('S3_BUCKET=','AWS_REGION=','EXPORT_START='))]
lines.extend(key+'='+os.environ[key] for key in ('S3_BUCKET','AWS_REGION','EXPORT_START'))
p.write_text('\n'.join(lines)+'\n')
PYENV
chown root:transit-export /opt/transit/.env
chmod 640 /opt/transit/.env
python3.11 -m venv /opt/transit/aws/.venv
/opt/transit/aws/.venv/bin/pip install -r /opt/transit/aws/requirements.txt
install -m 644 /opt/transit/aws/transit.service /opt/transit/aws/export.service /opt/transit/aws/export.timer /etc/systemd/system/
systemctl daemon-reload
cd /opt/transit
docker compose -f compose.yml -f aws/compose.override.yml build
# Enable the production-only restart override without changing the local Compose file.
cp /opt/transit/aws/compose.override.yml /opt/transit/compose.override.yml
systemctl enable --now transit.service export.timer
