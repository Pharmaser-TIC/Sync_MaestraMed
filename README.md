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
| `NT_HTTP_ADDR`, `NT_CONSUMERS_FILE`, `NT_PRICE_FIELDS`, `NT_API_FIELDS` | `127.0.0.1:8080`, `consumers.json`, `precio_venta,precio_maximo,precio_final,precio_contrato`, 25 columns | Only for `nt-sync -serve` (see "HTTP API") |
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

## HTTP API for projects without Redis access

`nt-sync -serve` exposes the same data over HTTP, read-only, for projects that cannot reach Redis. It is a
separate process from the sync (run it as its own service): it does not call the upstream API, does not touch
SQLite and never writes to Redis. It reads the same `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `NT_HTTP_ADDR` | `127.0.0.1:8080` | Listen address. Loopback by default: publish it through a reverse proxy with TLS |
| `NT_CONSUMERS_FILE` | `consumers.json` | Projects allowed to call the API (see below). The server refuses to start without it |
| `NT_PRICE_FIELDS` | `precio_venta,precio_maximo,precio_final,precio_contrato` | Row fields hidden from the projects that may not see prices |
| `NT_API_FIELDS` | the 25 columns below | Columns the API returns, comma separated, matched exactly (case and spaces count). `*` returns every column |

### Which columns it returns

Redis keeps the whole row, but the API returns only the columns in `NT_API_FIELDS`, in every endpoint
(`productos`, one product and the `fila`/`antes`/`campos` of `cambios`). By default:

`id, cum_facturar, contrato, contrato Evento, tipo_producto, descripcion, cod_gen_siesa, cod_mutual, activo,
ffm_rips, umm_rips, upr_rips, descripcion_siesa, atc, registro_sanitario, estado_registro,
clasificacion_entrega, plu, subplan, precio_maximo, precio_final, precio_contrato, estado, clasificacion, cod_auf`

- It is an allow list on the top-level columns: a column that is not listed is never returned, whatever its
  name, and one that is listed but absent from a row is simply not there. Adding a column is a change of
  `NT_API_FIELDS` and a restart of `nt-sync-api`; nothing has to be republished.
- An update that only touched columns the API does not return is not delivered in `cambios` (the cursor
  still moves on), for every project.
- Exact filters (`NT_INDEX_FIELDS`) and the text search (`NT_SEARCH_FIELDS`) only work on columns the API
  returns: a filter on any other one (for example `?estado=ERROR`) is a `400`, and the search ignores them
  (`403` when none is left). Otherwise a caller could find out what a hidden column holds by probing it.
- A list with no column, or `*` mixed with names, stops the server from starting.
- The server also refuses to start when a column whose name contains `precio` is listed in `NT_API_FIELDS`
  but not in `NT_PRICE_FIELDS` and some project cannot see prices. **If your `.env` sets `NT_PRICE_FIELDS`
  explicitly, add `precio_maximo`, `precio_final` and `precio_contrato` to it before upgrading.** The
  price columns of the default list are exactly the four of the default `NT_PRICE_FIELDS`.
- `estado` and `cod_auf` are in the default `NT_INDEX_FIELDS`, so once they are returned they also work as
  exact filters (`?estado=ACTIVO`, `?cod_auf=1957`). If your `.env` sets `NT_INDEX_FIELDS` explicitly it must
  list them, and a change of that setting only applies to the next published version: to get it
  now, delete the local copy (`rm data/nt.db`) and run `./nt-sync -once`, which downloads and republishes.

### Who can call it, and who sees prices

Every request needs `X-API-Key`. Each project has its own key, and `consumers.json` lists the projects with the
**SHA-256 of their key** (never the key) and whether they may see prices. Copy `consumers.example.json`:

```sh
KEY=$(openssl rand -hex 32)                      # give this to the project, once
printf %s "$KEY" | sha256sum | cut -d' ' -f1     # put this in "key_sha256"
```

```json
{"consumers":[
  {"name":"facturacion","key_sha256":"<64 hex>","precios":true},
  {"name":"inventario","key_sha256":"<64 hex>","precios":false}
]}
```

- `"precios"` has no default: a consumer without it, a typo in a field name, a duplicated name or key, or a key
  that is not 64 hex characters stops the server from starting. Revoke a project by removing its entry and
  restarting.
- A project with `"precios": false` receives every product **without the fields in `NT_PRICE_FIELDS`** (by default
  `precio_venta`, `precio_maximo`, `precio_final` and `precio_contrato`; the last three are part of the
  returned columns), in
  every endpoint, including `antes`/`fila`/`campos` of the changes. An update that only touched prices is not
  delivered to it, and it cannot filter or search by a price field (403).
- It is a deny list: field names are matched ignoring case, and also inside nested objects and arrays, but
  **a new price column with another name is visible until you add it to `NT_PRICE_FIELDS`**. Check the list against a full row (`nt-sync -check` prints a sample) whenever the
  Maestra gets new columns. The server refuses to start if someone cannot see prices and the list is empty.
- Keys do not expire. Rotate by issuing a new key and replacing the hash.

### Endpoints

| Request | Answer |
|---|---|
| `GET /v1/version` | `version`, `generado_en`, `publicado_en`, `total`, `ultimo_cambio` |
| `GET /v1/versiones` | `{"actual": "<version>", "versiones": ["<oldest>", ..., "<newest>"]}`: the versions still kept in Redis, in the order of `nt:versiones`. `503` when nothing is published |
| `GET /v1/productos?limit=500&cursor=` | every product with the columns of `NT_API_FIELDS`, in pages. Follow `siguiente` until it is empty |
| `GET /v1/productos/{id}` | one product (row key; `#` travels as `%23`, e.g. `/v1/productos/3%232`) |
| `GET /v1/productos?cum_facturar=123456-1` | exact filter on a field of `NT_INDEX_FIELDS` (one at a time; trimmed, case-insensitive), with the whole line of every match (see below). `offset` (up to 100000) and `limit` to page, `total` in the answer |
| `GET /v1/productos?q=levo` | text search over `NT_SEARCH_FIELDS` (accents ignored, contains, words of 2+ letters, up to 6; AND). `501` when search is off |
| `GET /v1/cambios?desde=<id>&limit=500` | changes after that stream id: `eventos`, `siguiente`, `hay_mas` |

A paged read keeps reading the version it started on (the cursor carries it), even if a new one is published
in the middle; if that version was already removed the answer is `410`: start again without a cursor.

**Reading a specific version.** `version`, `productos` (list, exact filters and text search) and
`productos/{id}` accept `?version=<v>`, one of the versions of `/v1/versiones`; a client that pins the version
of each loading re-reads that exact one later. Without it nothing changes: the current version is read.

- A version that does not exist or was already removed (`REDIS_KEEP_VERSIONS` keeps two by default; it also
  covers one that was removed while the answer was being built) is `404 {"error": "..."}`, and so is a value
  that cannot be a version. An empty or repeated `version` is `400`, so a client never reads the current
  version by mistake.
- A cursor already carries its version: `version` together with the cursor of another version is `400`.
  Without `version`, the cursor decides, as before (and `410` if it was removed).
- `ultimo_cambio` of `/v1/version?version=` is the stream position recorded when that version was published
  (`0-0` for an old version published before that was recorded). `/v1/cambios` takes no `version`.
- The text search of a version needs that version to have been published with `NT_SEARCH_FIELDS` on; otherwise
  it answers `503` like when search is not available.

**The exact filter returns lines, not just matching rows.** Rows that share an `id` (a line with several PLU) are
the row keys `id`, `id#2`, `id#3`... and the index only holds the rows that carry the value. The filter
therefore answers with *every* row of each line it finds, even when only one variant carries the value:

- Order: by base id (numeric ids compare as numbers: 9 before 10), then variant number (`id`, `id#2`, ..., `id#10`).
  `total` counts every returned row and `offset`/`limit` page through that order.
- A value that matches nothing is `200` with `"productos": []` and `"total": 0`.
- A line with more than 1000 variants is refused (`500`) instead of probed until the deadline. Variants are
  probed in widening steps, so a line of 50 rows costs a handful of Redis reads, always inside the request deadline.

**Every product carries `id_fila`**: its row key (`"15158"`, or `"15158#2"` for a variant). It is added to the
product in `productos` and `productos/{id}` and is not a column of `NT_API_FIELDS`: the `id` of the product is
exactly what was published and is *the same* for every variant of a line, so `id_fila` is what tells them apart
(it is the `id` of the events in `cambios`, whose `fila` does not carry it). The name is reserved: a published
column called `id_fila` is replaced.

**Staying up to date** (same idea as the Redis stream, with a cursor held by the client instead of a consumer group):

1. `GET /v1/version` and keep `ultimo_cambio`: the stream position that goes with that `version` (`0-0` when
   there are no changes yet). Do not take it from anywhere else.
2. Load every product (`/v1/productos`, following `siguiente`).
3. Loop: `GET /v1/cambios?desde=<cursor>`; apply each event (`creado`/`modificado`: store `fila` by `id`;
   `eliminado`: delete `id`); keep `siguiente` as the new cursor; while `hay_mas` is true ask again right away,
   otherwise wait a few seconds. Applying an event twice is harmless.
4. `410` with `"perdido": true` means the stream was trimmed past your cursor (this includes `0-0` once the
   stream has been trimmed at all): go back to step 1.

Errors are JSON (`{"error": "..."}`): `400` bad parameter, `401` missing or unknown key, `403` price field not
allowed, `404` no such product or version, `410` start again, `501` search off, `503` nothing published, search unavailable
or too many requests in progress (retry after a second).

`GET /healthz` is the health check for monitors on the same host: no key, `200 {"status":"ok"}` when Redis
answers, `503 {"status":"unavailable"}` when it does not. It says nothing else (not even whether data is
published; use `/v1/version` with a key for that), takes no request slot and is not logged unless it fails. The
nginx setup does not forward it (only `/v1/` is exposed); if you open `NT_HTTP_ADDR` beyond loopback without a
proxy, it becomes public.

Limits: 64 requests are served at the same time (the rest get `503`), every request has a 20 s deadline, a page
holds at most 1000 products, `q` at most 100 characters. A consumer without prices can see that a publication
happened (`hay_mas` or a moving `siguiente`) even when its changes were only price updates. If Redis restarts or
fails over in the middle of a paged read, the cursor may skip or repeat rows: compare the number of products you
loaded with `total` of `/v1/version` and reload on a mismatch.

### Running it

```sh
./nt-sync -serve        # listens on NT_HTTP_ADDR, logs one line per request (consumer, path, status; never the key)
```

- **Expose it only through a reverse proxy with TLS** (nginx, Caddy) and add rate limiting there; the server
  itself speaks plain HTTP. Keep it on loopback unless the proxy runs elsewhere.
- Run it as a service (systemd) so it restarts: `ExecStart=/opt/nt-sync/nt-sync -serve`,
  `WorkingDirectory=/opt/nt-sync`, `Restart=always`, a non-root `User=`.
- **Use a Redis user with read commands only** for this process (its own `.env`), for example
  `ACL SETUSER nt-api on >password ~nt:* resetchannels -@all +@read +ping +hello +client|setinfo`. This ACL is a
  starting point: check `FT.SEARCH` and the stream commands work with it on your Redis before relying on it.
- A project that can reach Redis should keep reading the stream directly: it is lighter than polling HTTP.

### Exposing it with nginx (Docker + Let's Encrypt)

`deploy/nginx/` runs nginx and certbot in Docker in front of the API: HTTPS on 443, the certificate from
Let's Encrypt renewed automatically, and only `/v1/` forwarded to `127.0.0.1:8080`. The API itself keeps
running as it is (for example under systemd).

**Before you start**
1. **DNS**: an A record for your domain pointing to the server's public IP.
2. **Ports 80 and 443 reachable**: open them in the provider's firewall (for example the OCI security list of
   the subnet) **and** in the host's (`sudo ufw allow 80,443/tcp`). Port 80 is needed for the certificate
   validation and for renewals.
3. **Ports 80 and 443 free on the server**: `sudo ss -ltnp | grep -E ':(80|443) '` must print nothing. If another
   web server already uses them, add a virtual host there instead of using this stack.
4. Docker with the compose plugin, and the API answering on `127.0.0.1:8080`.

**Steps** (on the server, from this repository)
```sh
cd deploy/nginx
cp env.example .env && nano .env      # API_DOMAIN and LETSENCRYPT_EMAIL
./init-letsencrypt.sh --staging       # dry run against Let's Encrypt's test servers (no rate limits)
./init-letsencrypt.sh --force         # the real certificate
curl -i https://YOUR-DOMAIN/v1/version   # 401 without a key means nginx and the API are talking
```
Do the `--staging` run first: Let's Encrypt limits failed real attempts per hour. Afterwards:
`docker compose logs -f nginx`, `docker compose ps`, `docker compose down` (the services restart by themselves
after a reboot).

**What it does**
- Forwards only `/v1/*`, only `GET`/`HEAD`; everything else, `/healthz` included, is `404`. Plain HTTP is
  redirected to HTTPS (and serves the validation challenge).
- TLS 1.2/1.3, HTTP/2, HSTS (one year: once a client has seen it, it will only use HTTPS), no body accepted.
- Rate limits: 20 requests/s per IP (burst 40) and 30 requests/s per API key (burst 60), 20 simultaneous
  connections per IP; above that nginx answers `429`. A full load of the catalogue is a few dozen requests, so
  normal use never gets near it.
- nginx runs with the host network so it can reach the API on the host's loopback and bind 80/443.
- Certificates and challenge files live in `deploy/nginx/data/` (not in git): back it up. nginx reloads every
  6 hours to pick up renewed certificates; certbot tries to renew twice a day.
- The monitor for the API stays local: `curl http://127.0.0.1:8080/healthz` on the server.

The configuration was checked with `nginx -t` and a container with a stand-in upstream (proxy, 404s, method
restriction, redirect, rate limit, body limit and challenge path). Issuing the certificate needs a real
domain and open ports, so `init-letsencrypt.sh` has only been checked up to its validations: do the
`--staging` run first.

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
