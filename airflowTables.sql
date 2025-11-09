CREATE TABLE real_time_metrics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    metric_type VARCHAR(50),
    metric_key VARCHAR(100),
    metric_value DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW()
);


CREATE TABLE anomalies (
    event_data JSONB,
    reason VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW()
);


CREATE TABLE daily_stats (
    stat_date DATE,
    metric_type VARCHAR(50),
    metric_key VARCHAR(100),
    metric_value DOUBLE PRECISION
) PARTITION BY RANGE (stat_date);


CREATE TABLE daily_stats_2025_11 PARTITION OF daily_stats
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');