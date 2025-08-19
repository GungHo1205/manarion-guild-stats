-- Guild Stats Database Schema
-- Optimized for time-series data with proper indexing

-- Main guild snapshots table - stores current levels at each timestamp
CREATE TABLE guild_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,           -- ISO format: 2025-01-15T14:30:00Z
    guild_name TEXT NOT NULL,
    guild_id INTEGER,
    guild_level INTEGER DEFAULT 0,
    nexus_level INTEGER NOT NULL,
    study_level INTEGER NOT NULL,
    total_upgrades INTEGER DEFAULT 0,
    
    -- Progress tracking (calculated from baseline)
    nexus_progress INTEGER DEFAULT 0,  -- Daily progress from baseline
    study_progress INTEGER DEFAULT 0,  -- Daily progress from baseline
    codex_cost INTEGER DEFAULT 0,      -- Estimated codex used for progress
    
    -- Metadata
    baseline_date TEXT,                -- Which baseline this progress is calculated against
    data_fresh BOOLEAN DEFAULT 1,     -- Whether this was fresh API data or cached
    
    UNIQUE(timestamp, guild_name)      -- Prevent duplicate entries
);

-- Daily baselines table - stores starting levels for each day
CREATE TABLE daily_baselines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,                -- YYYY-MM-DD format
    guild_name TEXT NOT NULL,
    nexus_level INTEGER NOT NULL,
    study_level INTEGER NOT NULL,
    created_at TEXT NOT NULL,          -- When this baseline was created
    
    UNIQUE(date, guild_name)           -- One baseline per guild per day
);

-- Market prices table - time-series price data
CREATE TABLE market_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    item_name TEXT NOT NULL,
    item_id INTEGER,
    buy_price INTEGER NOT NULL,
    sell_price INTEGER NOT NULL,
    average_price INTEGER GENERATED ALWAYS AS ((buy_price + sell_price) / 2) STORED,
    
    UNIQUE(timestamp, item_name)
);

-- Guild metadata table - relatively static guild information
CREATE TABLE guilds (
    guild_id INTEGER PRIMARY KEY,
    guild_name TEXT NOT NULL UNIQUE,
    owner_id INTEGER,
    last_seen TEXT,                    -- Last time this guild was processed
    is_active BOOLEAN DEFAULT 1,      -- Whether guild is still active
    total_upgrades INTEGER DEFAULT 0,
    guild_level INTEGER DEFAULT 0
);

-- Processing logs table - track script execution and performance
CREATE TABLE processing_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    execution_time_seconds REAL,
    guilds_processed INTEGER,
    guilds_skipped INTEGER,
    api_calls_made INTEGER,
    data_freshness TEXT,               -- JSON: {"guild_data_fresh": true, "market_data_fresh": false}
    errors TEXT,                       -- Any errors encountered
    baseline_created BOOLEAN DEFAULT 0 -- Whether a new baseline was created this run
);

-- Indexes for performance
CREATE INDEX idx_guild_snapshots_timestamp ON guild_snapshots(timestamp);
CREATE INDEX idx_guild_snapshots_guild_name ON guild_snapshots(guild_name);
CREATE INDEX idx_guild_snapshots_guild_timestamp ON guild_snapshots(guild_name, timestamp);

CREATE INDEX idx_baselines_date ON daily_baselines(date);
CREATE INDEX idx_baselines_guild_date ON daily_baselines(guild_name, date);

CREATE INDEX idx_market_timestamp ON market_prices(timestamp);
CREATE INDEX idx_market_item_name ON market_prices(item_name);
CREATE INDEX idx_market_item_timestamp ON market_prices(item_name, timestamp);

CREATE INDEX idx_guilds_name ON guilds(guild_name);
CREATE INDEX idx_guilds_active ON guilds(is_active);

CREATE INDEX idx_processing_timestamp ON processing_logs(timestamp);

-- Views for common queries
CREATE VIEW latest_guild_data AS
SELECT 
    gs.*,
    g.owner_id,
    g.is_active
FROM guild_snapshots gs
JOIN guilds g ON gs.guild_name = g.guild_name
WHERE gs.timestamp = (
    SELECT MAX(timestamp) 
    FROM guild_snapshots gs2 
    WHERE gs2.guild_name = gs.guild_name
);

CREATE VIEW daily_progress_summary AS
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as guilds_tracked,
    SUM(nexus_progress) as total_nexus_progress,
    SUM(study_progress) as total_study_progress,
    SUM(codex_cost) as total_codex_used,
    AVG(nexus_level) as avg_nexus_level,
    AVG(study_level) as avg_study_level
FROM guild_snapshots
GROUP BY DATE(timestamp)
ORDER BY date DESC;

-- Sample queries for reference:

-- Get guild progress for last 24 hours
-- SELECT guild_name, nexus_level, study_level, timestamp 
-- FROM guild_snapshots 
-- WHERE timestamp >= datetime('now', '-24 hours')
-- ORDER BY guild_name, timestamp;

-- Get current market prices
-- SELECT item_name, buy_price, sell_price, timestamp
-- FROM market_prices 
-- WHERE timestamp = (SELECT MAX(timestamp) FROM market_prices)
-- ORDER BY item_name;

-- Get top guilds by daily progress
-- SELECT guild_name, nexus_progress, study_progress, codex_cost
-- FROM latest_guild_data
-- WHERE baseline_date = date('now')
-- ORDER BY (nexus_progress + study_progress) DESC;