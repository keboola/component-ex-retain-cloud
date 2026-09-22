ex-retain-cloud
=============

Description
-----------

An extractor for [Retain Cloud](https://www.retaininternational.com/) (retaininternational.com), a
resource-planning SaaS used to manage staffing, scheduling, and utilization data. The component
connects to Retain Cloud's generic **DataAccessAPI** — a tenant-scoped, table-access REST API — and
extracts selected tables into Keboola Storage as native-typed tables, one output table per source
table.

**Table of Contents:**

[TOC]

Functionality Notes
===================

- **Authentication is username/password, not OAuth or an API token.** The component signs in via
  `POST /IntegrationApi/token` with your Retain Cloud email/password (plus environment and tenant);
  the response is a bearer token used automatically on every subsequent call. There is no consent
  screen or app registration — the connection fields are sufficient on their own.
- **Each table is a full snapshot, every run.** Retain Cloud keeps no server-side change history, so
  a run always fetches the source table's current state in full (there is no "changed since X" fetch
  mode available today).
- **Output uses native data types.** Each column's declared type is verified against the rows
  actually returned before being emitted as a native `integer`/`float`/`boolean`/`timestamp` column;
  a column is written as `string` whenever that verification fails for any row.
- **A primary key is set automatically when it is safe to.** If the table has a `<table>_guid`
  column and this run's fetched rows confirm it is unique, that column is declared as the output
  table's primary key. Otherwise no primary key is set.

Prerequisites
=============

You need a Retain Cloud account (username and password) with access to the DataAccessAPI for your
tenant, plus your tenant identifier and the Retain Cloud environment your tenant runs on (US, EU,
UK, or Australia). Ask your Retain Cloud administrator if you are not sure which environment applies
to your tenant.

Features
========

| **Feature**              | **Description**                                                        |
|---------------------------|------------------------------------------------------------------------|
| Generic UI Form           | Dynamic UI form for easy configuration.                                |
| Row-Based Configuration   | One config row per table — add, remove, or re-run a single table's row independently. |
| Table Picker              | The table field is populated from your tenant's live table list; no need to know table names up front. |
| Incremental Load          | Optional per-row upsert-by-primary-key, with an automatic per-run fallback to a full load if this run's primary key doesn't verify as unique. |
| Native Data Typing        | Output columns use native types where the source data verifiably supports it. |

Supported Endpoints
===================

If you need additional endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/).

Configuration
=============

### Root configuration (shared connection)

| Parameter | Required | Description |
|---|---|---|
| Environment | yes | The Retain Cloud environment hosting your tenant: US, EU, UK, or Australia. |
| Tenant | yes | Your Retain Cloud tenant identifier (case-sensitive). |
| Username | yes | The email or account name used to sign in to Retain Cloud. |
| \#Password | yes | The password for the account above. Stored encrypted, never logged. |

### Row configuration (one row per table)

| Parameter | Required | Description |
|---|---|---|
| Table | yes | The source table to extract, selected from a dropdown populated from your tenant's live table list once the connection fields above are filled in. |
| Load Type | no (default Full Load) | **Full Load** overwrites the output table every run. **Incremental Load** upserts by primary key instead, but never removes rows deleted upstream — if this run's primary-key uniqueness check fails for the table, the row automatically falls back to Full Load for that run and logs a warning; it does not fail the job. |
| Page Size | no (default 20000) | The row cap used on this table's first fetch call. This API has no page-continuation token, so it is not a literal "page size" — most tables finish in this one call; a larger table triggers exactly one further call sized to its actual row count. |

Output
======

Each config row produces one output table in Keboola Storage, named after the source table, with a
native-typed manifest. A primary key (`<table>_guid`) is set only when the source column exists and
is verified unique for this run's fetched rows.

Development
-----------

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder with your desired path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, initialize the workspace, and run the component using the following
commands:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone  component-ex-retain-cloud
cd component-ex-retain-cloud
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and perform lint checks using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The functional tests replay VCR cassettes committed under `tests/functional/`. Re-recording them
takes **two** scaffold passes, each with its own secrets file (the second one derived from the
first by `tests/setup/make_badpassword_secrets.py`) — read
[`tests/setup/README.md`](tests/setup/README.md) before touching them. The cassettes are public
and the tenant behind them is a real customer, so the sanitizers in `VCR_SANITIZERS`
(`src/component.py`) are load-bearing, not cosmetic.

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).
