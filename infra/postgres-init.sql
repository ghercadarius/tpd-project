CREATE TABLE IF NOT EXISTS aggregates_5m (
    brand           TEXT        NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    volume          INTEGER     NOT NULL,
    neg_count       INTEGER     NOT NULL,
    neg_ratio       REAL        NOT NULL,
    avg_neg_prob    REAL        NOT NULL,
    unique_authors  INTEGER     NOT NULL,
    influencer_neg  REAL        NOT NULL,
    PRIMARY KEY (brand, window_start)
);

CREATE INDEX IF NOT EXISTS aggregates_5m_brand_time
    ON aggregates_5m (brand, window_end DESC);

CREATE TABLE IF NOT EXISTS alerts (
    id              BIGSERIAL PRIMARY KEY,
    brand           TEXT        NOT NULL,
    triggered_at    TIMESTAMPTZ NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    z_score         REAL        NOT NULL,
    neg_ratio       REAL        NOT NULL,
    volume          INTEGER     NOT NULL,
    severity        TEXT        NOT NULL,
    sample_text     TEXT
);

CREATE INDEX IF NOT EXISTS alerts_brand_time
    ON alerts (brand, triggered_at DESC);
