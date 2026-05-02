# webrobot-price-comparison

A boilerplate vertical built on the [WebroBot](https://webrobot.eu) platform, demonstrating how to extend the stack with domain-specific plugins using a real-world, high-demand use case: **automated competitor price comparison via web scraping**.

This repository is intentionally structured as a reference implementation. The goal is to show the full plugin surface area — ETL stages, REST API endpoints, CMS connectors, CLI commands, and cloud dashboard panels — all wiring into the same WebroBot infrastructure without modifying the core.

---

## What WebroBot provides

- A distributed scraping engine (SpookyStuff + Spark) with a YAML pipeline DSL
- A plugin SDK for authoring custom ETL stages in Scala
- A REST API plugin system (Jersey/JAX-RS) for adding domain endpoints
- Cloud credential injection, MinIO storage, PostgreSQL, and Kubernetes job orchestration — all pre-wired
- A CLI and a cloud dashboard (Next.js) that pick up registered plugins automatically

You build the domain logic. The platform handles execution, scaling, auth, billing, and observability.

---

## Use case: price comparison

Monitor competitor prices for a product catalog identified by EAN codes. For each EAN, discover matching product pages on competitor sites, score the match confidence, and track price history over time.

Two scraping phases:

**Phase 1 — Discovery**
For each EAN × competitor domain, search Google (`site:` operator) to find the product URL on that site. Score the match using a three-tier strategy: EAN exact match, title similarity (Jaccard), and image comparison via Groq vision LLM. Persist confirmed matches.

**Phase 2 — Monitoring**
Re-visit saved URLs on a schedule to record updated prices and availability. No search needed — direct fetch from known URLs.

---

## Repository structure

```
webrobot-price-comparison/
  etl-plugin/          Scala — custom ETL stages registered via ServiceLoader
  rest-api-plugin/     Java  — REST endpoints + job orchestration + DB migrations
  opencart-plugin/     (coming) price sync to OpenCart via REST API
  woocommerce-plugin/  (coming) price sync to WooCommerce via WP REST API
  cli-plugin/          (coming) CLI commands: add products, run jobs, query prices
  dashboard-plugin/    (coming) Next.js panels for the WebroBot cloud dashboard
```

---

## etl-plugin

Scala module (Gradle). Implements four custom stages registered with the WebroBot ETL engine via Java `ServiceLoader`.

| Stage | Type | Description |
|-------|------|-------------|
| `pc_match_scorer` | `WTransformStage` | Tier 1: EAN exact match (0.95). Tier 2: Jaccard title similarity (0.50–0.85). Sets `match_confidence`. |
| `pc_image_match_stage` | `WTransformStage` | Tier 3: Groq vision LLM image comparison. Skipped if confidence already ≥ 0.90. Updates `match_confidence`, sets `image_match_confidence`. |
| `pc_save_match` | `WSinkStage` | UPSERT into `pc_matches` (unique on org + EAN + competitor site). Sets `match_id` on the row. |
| `pc_save_price` | `WSinkStage` | INSERT into `pc_price_history`. Requires `match_id` from `pc_save_match`. |

**Discovery pipeline** (defined in `rest-api-plugin`):
```
load_csv → searchEngine → visit → iextract → pc_match_scorer → pc_image_match_stage → pc_save_match
```

**Monitoring pipeline:**
```
load_csv → visit → iextract → pc_save_price
```

### Build

```bash
cd etl-plugin
gradle jar
```

Depends on `webrobot-plugin-sdk` (provided by the engine at runtime). No Spark dependency at compile time.

---

## rest-api-plugin

Java/Maven module. Registers as a WebroBot REST API plugin via `manifest.json`. Adds domain endpoints under `/webrobot/api/price-comparison/` and manages ETL job lifecycle.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/bootstrap` | Create ETL project + agents for the calling organization |
| `GET` / `POST` / `DELETE` | `/products` | Manage the EAN catalog to monitor |
| `GET` / `POST` / `DELETE` | `/competitors` | Manage competitor domains |
| `POST` | `/jobs/discovery` | Run phase 1: search → match → persist |
| `POST` | `/jobs/monitoring` | Run phase 2: re-fetch prices from saved URLs |
| `GET` | `/prices` | Current prices per EAN (from `pc_current_prices` view) |
| `GET` | `/matches` | Active confirmed matches |

### Database migrations

Flyway migrations in `src/main/resources/db/migration/price_comparison/`:

- `V1` — `pc_ean_catalog`, `pc_competitors`
- `V2` — `pc_matches` (unique on `organization_id, ean, competitor_site`)
- `V3` — `pc_price_history`, `pc_current_prices` view

### Multi-tenancy

All tables include `organization_id`. The org is resolved from the authenticated JWT on every request — never from the request body. Each organization gets its own ETL project, agents, MinIO path, and data rows.

### Build

```bash
cd rest-api-plugin
mvn package
```

### Credentials

The engine injects the following as environment variables into Spark workers via the cloud credential system:

| Variable | Used by |
|----------|---------|
| `GROQ_API_KEY` | `pc_image_match_stage` — Groq vision LLM |
| `GOOGLE_SEARCH_API_KEY` | `searchEngine` stage — discovery search |
| `GOOGLE_SEARCH_ENGINE_ID` | `searchEngine` stage |

---

## Roadmap

- [ ] `opencart-plugin` — sync `pc_current_prices` to OpenCart product prices via REST API
- [ ] `woocommerce-plugin` — sync to WooCommerce via WP REST API
- [ ] `cli-plugin` — `webrobot pc products add`, `webrobot pc jobs run`, `webrobot pc prices`
- [ ] `dashboard-plugin` — Next.js pages: product catalog, price charts, match review UI

---

## Related

- [WebroBot platform](https://webrobot.eu)
- [webrobot-plugin-sdk](https://github.com/WebRobot-Ltd/webrobot-etl) — SDK for authoring ETL stages
- [webrobot-etl](https://github.com/WebRobot-Ltd/webrobot-etl) — core ETL engine
