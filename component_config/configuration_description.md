### Connection

Every configuration shares one connection, set on the configuration root:

- **Environment** — the Retain Cloud environment hosting your tenant (US, EU, UK, or Australia).
  Selects the correct regional API host.
- **Tenant** — your Retain Cloud tenant identifier (case-sensitive).
- **Username** — the email or account name used to sign in to Retain Cloud.
- **Password** — the password for the account above. Encrypted at rest, never logged.

### Table selection

Each configuration row extracts exactly one table. Add one row per table you want in Keboola
Storage; each row runs and can be re-run independently.

- **Table** — picked from a dropdown populated from your tenant's live table list once the
  connection fields above are filled in.
- **Load Type** — **Full Load** (default) overwrites the row's output table every run; recommended,
  since Retain Cloud keeps no change history and a full load is the only way upstream deletes are
  reflected in Storage. **Incremental Load** upserts by primary key instead — it never removes rows
  deleted upstream, and automatically falls back to a full load for a given run if that run's
  primary-key uniqueness check fails.
- **Page Size** — the row cap sent on the table's first fetch call (default 20000). Most tables
  finish in this single call; a larger table triggers exactly one further, final call sized to its
  actual row count. Raise this only for an unusually large table.

### Output

Each row produces one Keboola Storage table, named after the source table, with a native-typed
manifest. A primary key is set only when the table has a `<table>_guid` column and this run's
fetched rows confirm it is unique.
