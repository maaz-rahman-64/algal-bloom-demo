# Algal Watch — Declarative Automation Bundle

End-to-end water-quality monitoring platform, all on serverless compute:

* **Generate data** — notebook that produces synthetic sensor, lab, weather, and flow data into a UC Volume landing zone (serverless job).
* **Transform water** — serverless Lakeflow Spark Declarative Pipeline (raw → validated → gold) with data-quality expectations.
* **Alert Level Forecast** — notebook that trains a LightGBM classifier, registers the model in Unity Catalog, scores a 7-day forecast, and writes SHAP explanations (serverless job).
* **Algal Watch - End-to-end build** — orchestration job that runs the three steps above in order. One click rebuilds everything.
* **Algal Watch Serverless Warehouse** — serverless SQL warehouse for the app and the Genie space.
* **Water Quality Site Risk Explorer** — Genie space over the gold tables (cloned from the original space and managed by the bundle).
* **Algal Watch Lakebase** — Lakebase Postgres project for the app's notification/acknowledgement log.
* **Algal Watch App** — React + Express (AppKit) dashboard with site signals, forecast charts, and Genie Q&A.

## Deploy

```bash
databricks bundle validate --strict -p <profile>
databricks bundle deploy -p <profile>          # dev target (default)
```

The first deploy takes about 15–25 minutes. Genie won't create a space until its tables
exist, so `bundle deploy` creates the jobs, pipeline, warehouse and Lakebase project, then
**runs the end-to-end job once** (the `job_runs.algal_watch_bootstrap` resource) and waits
for it to finish before creating the Genie space and the app. Later deploys only re-run
it when files under `src/notebooks/` or `src/pipeline/` change.

To rebuild the data and model at any other time, click **Run now** on
**Algal Watch - End-to-end build** in the Jobs UI, or run:

```bash
databricks bundle run algal_watch_end_to_end -p <profile>
```

### Choosing the workspace

The targets don't pin a workspace: whichever CLI profile you pass with `-p` is where the
bundle deploys. Create one with
`databricks auth login --host https://<your-workspace-url> --profile <profile>`.
To pin a target to one workspace, see the comment above `targets:` in `databricks.yml`.
If needed, also set:

* `--var catalog=<name>`: the data generator creates the catalog if it doesn't exist (needs
  `CREATE CATALOG`, and works with Default Storage). Otherwise point it at an existing
  catalog where you can create schemas.
* `--var lakebase_project_id=<id>`: Lakebase project IDs must be unique per workspace.

## Variables

| Variable | Description | Default |
|---|---|---|
| `catalog` | Unity Catalog catalog for all schemas | `algal_watch` |
| `schema_prefix` | Prefix for water schemas (\~_raw, \~_validated, \~_gold) | `water` |
| `as_of_date` | The demo's "today" that data and forecasts are anchored to | `2026-09-28` |
| `scenario` | Scenario file in `src/scenarios/` (without `.json`) | `inland_rivers` |
| `amber_biovolume` | Biovolume (mm³/L) at which a site is Amber | `0.4` |
| `red_biovolume` | Biovolume (mm³/L) at which a site is Red | `4.0` |
| `red_probability` | Forecast probability at which a day is called Red | `0.6` |
| `genie_description` | Description shown on the Genie space | generic text |
| `lakebase_project_id` | Lakebase Postgres project ID | `algal-watch` |

The warehouse ID, Genie space ID and Lakebase connection are bound to the app as app
resources, so there is nothing to look up or paste in.

## Customising for an organisation

The code is generic. Everything specific to a dataset or an organisation lives in three places:

* **Scenario** (`src/scenarios/<name>.json`, selected with `--var scenario=<name>`): the
  monitoring sites, which sites play the storyline roles (the site forecast to breach,
  the site already Red, the faulty sensor), current lab results, season timing, contact
  formats, and the alert history. The app seeds its notification log from that history
  on first use.
* **App profile** (`app/config/profile.json`): product and organisation name, logo,
  brand colour, date locale, default region, demo persona, council message wording and
  alert-level guidance.
* **Thresholds and Genie text** (bundle variables above). The pipeline, model and app
  all read the alert thresholds from these variables.

The default scenario, `inland_rivers`, uses real public place names (the Russian River,
Napa Valley and Coyote Creek watersheds around the San Francisco Bay Area) with synthetic
data. To create a variant, copy the bundle and replace or add files in
those three places. Keep variant-only assets, such as a customer logo, outside the
published bundle.

## Project layout

```
algal-watch-bundle/
├── databricks.yml                       # Bundle configuration + variables
├── resources/
│   ├── algal_watch_orchestration_job.yml  # Job: end-to-end build + deploy-time bootstrap run
│   ├── generate_data_job.yml            # Job: synthetic data generation
│   ├── transform_water_pipeline.yml     # SDP pipeline: raw → validated → gold
│   ├── forecast_job.yml                 # Job: ML train / register / score / explain
│   ├── algal_watch_warehouse.yml        # Serverless SQL warehouse
│   ├── site_risk_genie_space.yml        # Genie space (Water Quality Site Risk Explorer)
│   ├── algal_watch_lakebase.yml         # Lakebase Postgres project
│   └── algal_watch_app.yml              # Databricks App + its resource bindings
├── src/
│   ├── notebooks/
│   │   ├── 01_generate_data.py          # Data generator notebook
│   │   └── alert_level_forecast.py      # ML notebook
│   ├── pipeline/
│   │   └── my_transformation.py         # SDP transformation code
│   └── scenarios/
│       └── inland_rivers.json           # Default scenario: sites, storyline, alert history
└── app/                                 # Databricks App source
    ├── package.json
    ├── config/profile.json              # Branding, persona, locale and message copy
    ├── config/query-templates/          # Parameterised SQL for OBO queries ({{catalog}}, {{schema_prefix}})
    ├── shared/profile.ts                # Typed access to the profile (client + server)
    ├── scripts/render-queries.mjs       # Renders templates → config/queries/ at start-up
    ├── server/
    └── client/
```

## App notes

* The app's runtime config (command and env) lives in `resources/algal_watch_app.yml`,
  not `app/app.yaml`, so it can reference bundle variables and resources.
* The analytics plugin reads `config/queries/*.sql`. Those files are generated from
  `config/query-templates/` by `scripts/render-queries.mjs`, using `ALGAL_CATALOG` /
  `ALGAL_SCHEMA_PREFIX`. That happens before `npm run start`, `npm run dev` and `npm run typegen`.
* For local dev, put the `ALGAL_*` values from `.env.example`, `DATABRICKS_WAREHOUSE_ID`,
  the Lakebase `PG*` / `LAKEBASE_ENDPOINT` values, and `DATABRICKS_GENIE_SPACE_ID` in `app/.env`.
* The notification log is seeded from the `alert_history_seed` table (read as the
  signed-in user) whenever the app finds it empty. To reset the demo log, empty the
  `algal.alert_actions` table in the Lakebase database; the next page load re-seeds it.
* Query result types are committed in `shared/appkit-types/`, so the platform build doesn't
  need a running warehouse. After changing a query template, regenerate them with `npm run typegen`.
