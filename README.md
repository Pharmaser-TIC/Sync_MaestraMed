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
| `NT_CHANGES_MAXLEN` | `100000` | Entries kept in the `nt:cambios` change stream (see "Change stream"); `0` turns it off |
| `NT_INDEX_FIELDS` | `cum_facturar,cod_auf,cod_mutual,estado,contrato` | Fields that get a lookup set |
| `NT_SEARCH_FIELDS` | empty (search off) | Fields indexed for full-text search, e.g. `descripcion,descripcion_siesa`; needs Redis 8 or Redis Stack (see "Full-text search") |

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

### Reading every product

`nt:<version>:filas` holds every product: one field per row key, whose value is the row JSON with all
its columns. Page through it with `HSCAN` (do not use `HGETALL` on a big version: it blocks Redis):

```sh
V=$(redis-cli GET nt:actual)
redis-cli HSCAN "nt:$V:filas" 0 COUNT 500      # repeat with the returned cursor until it is 0
redis-cli HLEN "nt:$V:filas"                   # how many products
```

Resolve `nt:actual` once per full read and keep using that version until you finish: a new version can be
published in the middle, and the previous one stays readable (see `REDIS_KEEP_VERSIONS`).

## Change stream

To know **which product changed** (and update the databases that use it) read the Redis Stream
`nt:cambios`. On every publication nt-sync compares the new rows with the ones readers had and adds one
entry per row that was created, modified or deleted:

| Field | Content |
|---|---|
| `tipo` | `creado`, `modificado` or `eliminado` |
| `id` | row key (the row `id`; `id#2`, `id#3`... when a line has several PLU) |
| `version` | version that introduced the change |
| `anterior` | version it was compared with |
| `campos` | `modificado` only: changed fields, comma separated |
| `antes` | `modificado` only: JSON with the previous value of each changed field (`null` if it did not exist) |
| `fila` | the row as published (for `eliminado`, as it was) |

Each database reads with its own consumer group, so it receives every event once it asks, even if it was
down for a while, and the others are not affected:

```sh
redis-cli XGROUP CREATE nt:cambios mi_base '$' MKSTREAM    # once per consumer; '$' = only new changes ('0' = everything kept)
redis-cli XREADGROUP GROUP mi_base worker1 COUNT 100 BLOCK 5000 STREAMS nt:cambios '>'
redis-cli XACK nt:cambios mi_base <entry-id>               # after the change was applied
```

- **Delivery is at-least-once.** Entries are written before `nt:actual` switches; if a publication fails
  and is retried they can be written twice. Make the update idempotent (the row JSON in `fila` replaces the
  record) or dedupe by `(version, id)`.
- A consumer that crashes before `XACK` gets its pending entries back with `XAUTOCLAIM` / `XPENDING`.
- **The first publication emits nothing**, and neither does one made after Redis lost the previous version's
  rows: there is nothing to compare with. A new consumer should load everything once (previous section) and then
  follow the stream.
- Rows that differ only in the order of their JSON keys are not changes.
- The stream keeps about `NT_CHANGES_MAXLEN` entries (default 100000; `0` turns it off). A consumer that stays
  away longer than that loses the oldest entries: reload everything if `XINFO GROUPS` shows it lagging past the
  trimmed range. A massive change (for example a full price update) writes one entry per row.
- `nt-sync -check` prints the stream length and its newest entry.

## Full-text search

The lookup sets only answer exact matches. To find rows by part of a text (for example a medication
name) set `NT_SEARCH_FIELDS=descripcion,descripcion_siesa`. It needs a Redis with the search and
JSON modules (Redis 8, Redis Stack); at startup the program stops with a clear error when they are missing.

Each version then also holds one JSON document per row (`nt:<ver>:doc:<id>`), a RediSearch index
`nt:<ver>:ft`, and the alias `nt:ft`, which always points to the index of the current version and switches
together with `nt:actual`. Query the alias:

```sh
redis-cli FT.SEARCH nt:ft '@descripcion:adrenalin*'      # starts with
redis-cli FT.SEARCH nt:ft '@descripcion:*epinefrina*'      # contains
redis-cli FT.SEARCH nt:ft '@descripcion_siesa:adrelab'     # all the words
```

RediSearch lower-cases terms but does not strip accents, so the index reads the text from a `_busqueda`
object that the program adds to each document (lower-case, no accents; the original row is kept as is).
**Readers must apply the same rule to the search terms**: lower-case them and replace `á é í ó ú ü ñ ç`
by `a e i o u u n c` (`publish.Fold` does it in Go). Without it `acido*` would not find `ÁCIDO FÓLICO`.

Turning `NT_SEARCH_FIELDS` on or off takes effect at the next published version. `nt-sync -check` also
reports how many documents the search index of the current version holds.

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
