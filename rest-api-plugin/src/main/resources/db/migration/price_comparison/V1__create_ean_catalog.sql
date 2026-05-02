-- EAN catalog: input master table per organizzazione
CREATE TABLE IF NOT EXISTS pc_ean_catalog (
    id              BIGSERIAL PRIMARY KEY,
    organization_id BIGINT        NOT NULL,
    ean             VARCHAR(14)   NOT NULL,
    product_name    VARCHAR(500),
    brand           VARCHAR(200),
    image_url       TEXT,
    created_at      TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_pc_ean_catalog UNIQUE (organization_id, ean)
);

CREATE INDEX IF NOT EXISTS idx_pc_ean_catalog_org ON pc_ean_catalog (organization_id);

-- Competitor sites configurati per organizzazione
CREATE TABLE IF NOT EXISTS pc_competitors (
    id              BIGSERIAL PRIMARY KEY,
    organization_id BIGINT        NOT NULL,
    site_domain     VARCHAR(200)  NOT NULL,   -- es. "amazon.it", "eprice.it"
    site_name       VARCHAR(200),
    country_code    VARCHAR(2),               -- ISO 3166-1: IT, DE, FR, ...
    is_active       BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_pc_competitors UNIQUE (organization_id, site_domain)
);

CREATE INDEX IF NOT EXISTS idx_pc_competitors_org ON pc_competitors (organization_id, is_active);
