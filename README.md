# nt-sync

Keeps Redis in step with the Maestra Tarifaria published by the medication-creation API, so other
systems read Redis and never recalculate or touch the database.

```
API (PHP)  --nt_version / nt_snapshot-->  nt-sync  --> SQLite (local copy)
                                             └-----> Redis (versioned keys + pointer)
```

Every `POLL_INTERVAL` it calls `nt_version` (cheap). Only when the version changed does it call
`nt_snapshot`, store the rows in SQLite and publish them to Redis. If Redis loses its data but the
SQLite copy is current, Redis is rebuilt from SQLite without asking the API to calculate again.
The rules stay in one place (the API); this service only copies results.

## Run

```sh
cp .env.example .env && chmod 600 .env     # fill in NT_API_URL, NT_API_KEY, REDIS_ADDR
go run ./cmd/nt-sync              # loop
go run ./cmd/nt-sync -once        # one cycle, then exit (what cron runs)
go run ./cmd/nt-sync -debug       # also log cycles without changes
go run ./cmd/nt-sync -env /etc/nt-sync.env   # settings file elsewhere
go run ./cmd/nt-sync -check       # read Redis back and report what is stored (see below)
```

At startup the program reads `.env` from the current folder (or the file given with `-env`). Variables that
are already set in the environment win over the file, and a missing file is fine: in a container or a service
the variables usually come from the runtime. The file accepts `KEY=VALUE`, `export KEY=VALUE`, `# comments`
and quoted values.

Docker: `docker build -t nt-sync .` and run it with the same variables and a volume on `/data`.

| Variable | Default | Meaning |
|---|---|---|
| `NT_API_URL` | required | Full URL of `api.php` |
| `NT_API_KEY` | required | API key (`X-API-Key`), same as one of `CI_PUBLICACION_API_KEYS` |
| `REDIS_ADDR` | required | `host:port` |
| `REDIS_PASSWORD`, `REDIS_DB`, `REDIS_TLS` | empty, `0`, `false` | Redis connection (use TLS in OCI) |
| `REDIS_PREFIX` | `nt` | Namespace of every key |
| `REDIS_KEEP_VERSIONS` | `2` | Versions kept in Redis (current + previous ones) |
| `POLL_INTERVAL` | `30s` | Time between `nt_version` checks (30-60 s recommended) |
| `SQLITE_PATH` | `data/nt.db` | Local copy |
| `NT_INDEX_FIELDS` | `cum_facturar,cod_auf,cod_mutual,estado,contrato` | Fields that get a lookup set |

## Cron on Ubuntu

`deploy/install-cron.sh` installs the cron entry that runs one cycle (`nt-sync -once`) every few minutes.
A change in the API reaches Redis within one interval; with no changes a cycle costs one `nt_version` call.

```sh
# on your machine: build for the server and copy the binary and the script
GOOS=linux GOARCH=amd64 go build -o nt-sync ./cmd/nt-sync
ssh user@server 'mkdir -p /opt/nt-sync/deploy'
scp nt-sync user@server:/opt/nt-sync/
scp deploy/install-cron.sh user@server:/opt/nt-sync/deploy/

# on the server: create /opt/nt-sync/.env (chmod 600), then
cd /opt/nt-sync && chmod +x nt-sync deploy/install-cron.sh
deploy/install-cron.sh --dry-run          # shows the cron line and warnings, changes nothing
deploy/install-cron.sh --every 5          # installs it (default: every 5 minutes)
deploy/install-cron.sh --remove           # removes it
```

- Minimal Ubuntu images (OCI, for example) do not ship `crontab`. If the script says it is missing:
  `sudo apt install cron && sudo systemctl enable --now cron`.
- The entry is `*/5 * * * * cd /opt/nt-sync && flock -n .nt-sync.lock ./nt-sync -once >> logs/nt-sync.log 2>&1`.
  `flock` stops a slow run from overlapping the next one; Redis also has its own lock.
- Normal runs only log when something is published or fails, so `logs/nt-sync.log` stays small. Add a
  `logrotate` rule if you want it capped.
- Cron has no environment of its own: the settings must be in `/opt/nt-sync/.env`.
- Running the script again replaces the entry (to change the interval) and never touches other crontab lines.
  Test the script itself with `deploy/test-install-cron.sh` (it uses a fake crontab).
- Why 5 minutes: changes in the Maestra Tarifaria are made by people, not continuously, and each check reads
  `nt_item` and the catalogs on the server. Use `--every 1` if Redis must follow changes within a minute.

## Redis layout for consumers

| Key | Type | Content |
|---|---|---|
| `nt:actual` | string | Version to read. **Always read this first.** |
| `nt:<version>:meta` | hash | `version`, `generado_en`, `total`, `con_precios` (`1`/`0`), `bases_faltantes` (JSON), `publicado_en` |
| `nt:<version>:filas` | hash | row key → row JSON exactly as the API sent it |
| `nt:<version>:idx:<field>:<VALUE>` | set | row keys whose field equals VALUE (trimmed, upper-case) |

Row key = the row `id`; when one line has several PLU the extra rows are `id#2`, `id#3`...

```sh
V=$(redis-cli GET nt:actual)
redis-cli SMEMBERS "nt:$V:idx:cum_facturar:123456-1"      # row keys
redis-cli HMGET "nt:$V:filas" 17 18                       # the rows
redis-cli HGET "nt:$V:meta" con_precios
```

A new version is written completely under its own prefix and only then does `nt:actual` change, so a
reader that resolved the pointer once always sees a consistent snapshot. The previous version stays
for the next cycle (`REDIS_KEEP_VERSIONS`); do not cache the version for longer than a few seconds.

## Checking what Redis holds

`nt-sync -check` connects to the Redis configured in the environment / `.env`, prints what a cycle stored
and exits with code 1 when something is wrong. It only reads: it does not call the API and does not touch
the SQLite copy.

```sh
cd /opt/nt-sync && ./nt-sync -check
```

It reports the version in `nt:actual`, the `meta` fields, how many rows `filas` holds, how many lookup
sets exist per indexed field, one sample row, the versions kept, and whether the publish lock is held.
It flags: nothing published, a pointer to a version without data, and a `meta.total` that differs from
the rows actually stored. The sample row is printed as stored, so it shows prices when the API sent them.

`-check` reads the same settings as a normal run, so `NT_API_URL` and `NT_API_KEY` must be set too, even
though they are not used.

## Behaviour worth knowing

- API errors (401 wrong key, 503 key not configured, timeouts) are logged and retried at the next
  tick; Redis keeps serving the last version.
- One instance at a time publishes (`nt:lock`, expires after 10 minutes if an instance dies).
- Prices arrive as `null` unless the API runs with `CI_PUBLICACION_CON_PRECIOS=1` (`con_precios` in `meta`).
  Anyone who can read Redis sees whatever the API sent: restrict Redis to private networks, TLS and per-client ACLs.
- The key is only sent in the `X-API-Key` header and never logged.

## Tests

```sh
go test ./...
```

Unit tests per package plus an end-to-end test with a fake API and an in-memory Redis (miniredis).
They do not cover a real Redis server or the real PHP API.
