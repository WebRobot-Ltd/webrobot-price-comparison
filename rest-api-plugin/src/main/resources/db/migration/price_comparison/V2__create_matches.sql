-- Tabella matching: EAN → URL prodotto su sito concorrente
CREATE TABLE IF NOT EXISTS pc_matches (
    id                BIGSERIAL PRIMARY KEY,
    organization_id   BIGINT        NOT NULL,
    ean               VARCHAR(14)   NOT NULL,
    competitor_site   VARCHAR(200)  NOT NULL,
    product_url       TEXT          NOT NULL,
    product_title     TEXT,
    match_confidence  DECIMAL(4,3),             -- 0.000 – 1.000
    match_method      VARCHAR(20),              -- ean_exact | title_sim | image_sim
    is_active         BOOLEAN       NOT NULL DEFAULT TRUE,
    last_verified_at  TIMESTAMP,
    created_at        TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMP     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_pc_matches UNIQUE (organization_id, ean, competitor_site)
);

CREATE INDEX IF NOT EXISTS idx_pc_matches_ean    ON pc_matches (organization_id, ean);
CREATE INDEX IF NOT EXISTS idx_pc_matches_site   ON pc_matches (organization_id, competitor_site);
CREATE INDEX IF NOT EXISTS idx_pc_matches_active ON pc_matches (is_active, last_verified_at);
