#!/usr/bin/env bash
# First-time Let's Encrypt setup for the nginx that fronts the nt-sync API.
#
#   ./init-letsencrypt.sh --staging   try the whole flow against Let's Encrypt's test servers first
#   ./init-letsencrypt.sh             real certificate
#   ./init-letsencrypt.sh --force     ask for a new certificate even if one exists (e.g. staging -> real)
#
# nginx needs a certificate file to start, and Let's Encrypt needs nginx running to validate the
# domain. So the script: creates a throwaway certificate, starts nginx, deletes the throwaway one,
# asks Let's Encrypt for the real one and reloads nginx.
set -euo pipefail
cd "$(dirname "$0")"

staging=0
force=0
for arg in "$@"; do
  case "$arg" in
    --staging) staging=1 ;;
    --force) force=1 ;;
    *) echo "unknown option: $arg (use --staging and/or --force)" >&2; exit 2 ;;
  esac
done

if [ ! -f .env ]; then
  echo "Missing .env: run 'cp env.example .env' and fill it in." >&2
  exit 1
fi
# Read the two settings as plain text. The file is deliberately NOT sourced: sourcing would run
# whatever shell code it contains, before any validation could look at it.
env_value() {
  sed -n "s/^$1=//p" .env | tail -n 1 | tr -d '\r' | sed -e 's/^"\(.*\)"$/\1/' -e "s/^'\(.*\)'\$/\1/"
}
API_DOMAIN=$(env_value API_DOMAIN)
LETSENCRYPT_EMAIL=$(env_value LETSENCRYPT_EMAIL)
export API_DOMAIN LETSENCRYPT_EMAIL # docker compose also interpolates them from .env
: "${API_DOMAIN:?API_DOMAIN is not set in .env}"
: "${LETSENCRYPT_EMAIL:?LETSENCRYPT_EMAIL is not set in .env}"
# The domain is pasted into shell commands run in the certbot container: accept plain host names only.
if [[ ! "$API_DOMAIN" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]] || [[ "$API_DOMAIN" == "api.example.com" ]]; then
  echo "API_DOMAIN in .env must be your real domain name (got '$API_DOMAIN')." >&2
  exit 1
fi
if [[ ! "$LETSENCRYPT_EMAIL" =~ ^[^[:space:]@]+@[^[:space:]@]+$ ]] || [[ "$LETSENCRYPT_EMAIL" == "you@example.com" ]]; then
  echo "LETSENCRYPT_EMAIL in .env must be your real e-mail address." >&2
  exit 1
fi

mkdir -p data/certbot/conf data/certbot/www

staging_arg=()
if [ "$staging" -eq 1 ]; then
  staging_arg=(--staging)
  echo "### STAGING mode: the certificate will NOT be trusted by browsers or clients."
fi

request_certificate() {
  docker compose run --rm --entrypoint certbot certbot certonly --webroot -w /var/www/certbot \
    ${staging_arg[@]+"${staging_arg[@]}"} "$@" -d "$API_DOMAIN" --email "$LETSENCRYPT_EMAIL" \
    --agree-tos --no-eff-email --non-interactive
}

live="/etc/letsencrypt/live/$API_DOMAIN"

if [ -f "data/certbot/conf/renewal/$API_DOMAIN.conf" ]; then
  if [ "$force" -eq 0 ]; then
    echo "A certificate for $API_DOMAIN already exists: just making sure the services are up."
    echo "(use --force to request it again, for example to go from --staging to the real one)"
    docker compose up -d
    exit 0
  fi
  # A real certificate exists: keep it in place until the new one is issued.
  echo "### Replacing the existing certificate"
  docker compose up -d nginx
  sleep 3
  request_certificate --force-renewal
else
  echo "### 1/4 Temporary certificate, only so that nginx can start"
  docker compose run --rm --entrypoint sh certbot -c \
    "mkdir -p '$live' && openssl req -x509 -nodes -newkey rsa:2048 -days 1 \
       -keyout '$live/privkey.pem' -out '$live/fullchain.pem' -subj '/CN=localhost'"

  echo "### 2/4 Starting nginx"
  docker compose up -d nginx
  sleep 3

  echo "### 3/4 Removing the temporary certificate"
  docker compose run --rm --entrypoint sh certbot -c \
    "rm -rf '/etc/letsencrypt/live/$API_DOMAIN' '/etc/letsencrypt/archive/$API_DOMAIN' '/etc/letsencrypt/renewal/$API_DOMAIN.conf'"

  echo "### 4/4 Requesting the certificate for $API_DOMAIN"
  request_certificate
fi

echo "### Reloading nginx and starting the renewal service"
docker compose exec nginx nginx -s reload
docker compose up -d certbot

echo
echo "Done. Try:  curl -i https://$API_DOMAIN/v1/version   (401 without a key means it works)"
