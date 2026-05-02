-- Storico prezzi per URL matchata
CREATE TABLE IF NOT EXISTS pc_price_history (
    id               BIGSERIAL PRIMARY KEY,
    match_id         BIGINT        NOT NULL REFERENCES pc_matches(id) ON DELETE CASCADE,
    organization_id  BIGINT        NOT NULL,
    ean              VARCHAR(14)   NOT NULL,
    competitor_site  VARCHAR(200)  NOT NULL,
    price            DECIMAL(12,4),
    currency         VARCHAR(3),               -- ISO 4217: EUR, USD, GBP
    availability     VARCHAR(20),              -- in_stock | out_of_stock | unknown
    discount_pct     DECIMAL(5,2),
    scrape_status    VARCHAR(20)   NOT NULL DEFAULT 'ok', -- ok | error | blocked | timeout
    scraped_at       TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pc_price_history_match   ON pc_price_history (match_id, scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_pc_price_history_ean     ON pc_price_history (organization_id, ean, scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_pc_price_history_scraped ON pc_price_history (scraped_at DESC);

-- Vista: prezzo corrente per match (DISTINCT ON — PostgreSQL)
CREATE OR REPLACE VIEW pc_current_prices AS
SELECT DISTINCT ON (ph.match_id)
    ph.match_id,
    ph.organization_id,
    ph.ean,
    ph.competitor_site,
    m.product_url,
    m.product_title,
    m.match_confidence,
    ph.price,
    ph.currency,
    ph.availability,
    ph.discount_pct,
    ph.scrape_status,
    ph.scraped_at
FROM pc_price_history ph
JOIN pc_matches m ON ph.match_id = m.id
WHERE m.is_active = TRUE
ORDER BY ph.match_id, ph.scraped_at DESC;
