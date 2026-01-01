CREATE TABLE IF NOT EXISTS vars (
    name VARCHAR(150) NOT NULL,
    value TEXT NOT NULL,
    remark TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (name)
);

INSERT INTO vars (name, value, remark) VALUES ('coins', 'ETH,BTC,BNB,SOL,XRP,DOGE,ADA,HYPE,SUI', '标的币种') ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, remark = EXCLUDED.remark;

CREATE TABLE IF NOT EXISTS address (
    address    VARCHAR(42) NOT NULL,
    total_equity NUMERIC(32,12) NOT NULL DEFAULT 0, -- 总资产
    prep_pnl    NUMERIC(32,12) NOT NULL DEFAULT 0, -- 合约盈亏
    prep_open NUMERIC(32,12) NOT NULL DEFAULT 0, -- 合约持仓
    prep_equity NUMERIC(32,12) NOT NULL DEFAULT 0, -- 合约保证金
    sum_upnl   NUMERIC(32,12) NOT NULL DEFAULT 0, -- 未实现盈亏
    earliest_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- 最早活动时间
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- 创建时间 
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- 更新时间
    PRIMARY KEY (address)
);
CREATE INDEX IF NOT EXISTS idx_address_total_equity ON address (total_equity DESC);
CREATE INDEX IF NOT EXISTS idx_address_earliest_at ON address (earliest_at);

CREATE TABLE IF NOT EXISTS skip_address (
    address VARCHAR(42) NOT NULL,
    type SMALLINT NOT NULL DEFAULT 0,
    remark TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (address)
);

CREATE TABLE IF NOT EXISTS portfolio (
    time TIMESTAMPTZ NOT NULL,
    address VARCHAR(42) NOT NULL,
    equity NUMERIC(18, 6) NOT NULL,
    pnl NUMERIC(18, 6) NOT NULL,
    PRIMARY KEY (address, time)
);

SELECT create_hypertable('portfolio', 'time', chunk_time_interval => INTERVAL '7 days');
SELECT add_retention_policy(
        'portfolio',
        INTERVAL '1 year',
        schedule_interval => INTERVAL '24 hours'
);

CREATE MATERIALIZED VIEW portfolio_times AS
SELECT DISTINCT time AS time FROM portfolio ORDER BY time;

CREATE MATERIALIZED VIEW portfolio_last AS
SELECT DISTINCT ON (address)
       P.address,
       P.time,
       P.equity,
       P.pnl,
       CASE WHEN P.pnl < 0 THEN 2 WHEN P.pnl > 0 THEN 3 ELSE 1 END AS pnl_type
    FROM portfolio P LEFT JOIN skip_address S ON P.address=S.address WHERE S.address IS NULL
    ORDER BY P.address, P.time DESC
WITH NO DATA;

CREATE UNIQUE INDEX idx_portfolio_last_address ON portfolio_last (address);
CREATE INDEX index_portfolio_last_equity ON portfolio_last (equity);
CREATE INDEX index_portfolio_last_pnl ON portfolio_last (pnl);
CREATE INDEX index_portfolio_last_pnl_type ON portfolio_last (pnl_type);

CREATE OR REPLACE FUNCTION refresh_skip_address(clear bool)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    -- 90天没有盈亏的地址
    INSERT INTO skip_address (address, "type") (WITH _active AS (
        SELECT address, FIRST(pnl, time)=LAST(pnl, time) AS noact FROM portfolio WHERE "time" > NOW() - INTERVAL '90 days' GROUP BY address
    ) SELECT address, 1 AS "type" FROM _active WHERE noact) ON CONFLICT (address) DO NOTHING;
    -- 盈亏为0的地址
    INSERT INTO skip_address (address, "type", remark) SELECT address, 1 as "type", '超90天未开仓' as remark FROM portfolio_last WHERE pnl=0 ON CONFLICT (address) DO NOTHING;
    -- Pnl 在±100以内，equity小于100
    INSERT INTO skip_address (address, "type", remark) SELECT address, 1 as "type", 'pnl±100以内' as remark FROM portfolio_last WHERE (pnl>=-100 AND pnl <=100) ON CONFLICT (address) DO NOTHING;
    IF clear THEN
        -- 删除数据
        DELETE FROM portfolio WHERE address IN (SELECT address FROM skip_address);
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS summary (
    pnl_type SMALLINT NOT NULL,
    time TIMESTAMP NOT NULL,
    equity NUMERIC(18, 6) NOT NULL,
    pnl NUMERIC(18, 6) NOT NULL,
    num INT NOT NULL,
    PRIMARY KEY (pnl_type, time)
);

CREATE OR REPLACE FUNCTION refresh_summary(last_time TIMESTAMPTZ)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    WITH _last AS (
        SELECT DISTINCT ON (address)
            P.address,
            P.equity,
            P.pnl,
            P.time,
            L.pnl_type
        FROM portfolio P INNER JOIN portfolio_last L ON P.address=L.address
        WHERE P.time <= last_time
        ORDER  BY P.address, P.time DESC
    )
    INSERT INTO summary (pnl_type, time, equity, pnl, num) SELECT pnl_type, last_time AS time, sum(equity) AS equity, sum(pnl) AS pnl, COUNT(1) AS num FROM _last GROUP BY pnl_type ON CONFLICT (pnl_type, time) DO UPDATE SET equity = EXCLUDED.equity, pnl = EXCLUDED.pnl, num = EXCLUDED.num;
END;
$$;

CREATE OR REPLACE FUNCTION refresh_summary_all()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE r record;
BEGIN
    FOR r IN (SELECT time FROM portfolio_times ORDER BY time) LOOP
        PERFORM refresh_summary(r.time);
    END LOOP;
END;
$$;

-- 地址集合表
CREATE TABLE IF NOT EXISTS address_set (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    sql_query TEXT NOT NULL,
    excluded_addresses TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 地址集合的缓存结果表
CREATE TABLE IF NOT EXISTS address_set_cache (
    set_id INTEGER NOT NULL REFERENCES address_set(id) ON DELETE CASCADE,
    address VARCHAR(42) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (set_id, address)
);

CREATE TABLE IF NOT EXISTS address_set_summary (
    set_id INTEGER NOT NULL REFERENCES address_set(id) ON DELETE CASCADE,
    time TIMESTAMPTZ NOT NULL,
    equity NUMERIC(18, 6) NOT NULL,
    pnl NUMERIC(18, 6) NOT NULL,
    PRIMARY KEY (set_id, time)
);

CREATE OR REPLACE FUNCTION refresh_address_set_summary(p_set_id INTEGER, last_time TIMESTAMPTZ)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    WITH _last AS (
        SELECT DISTINCT ON (address)
            P.address,
            P.equity,
            P.pnl,
            P.time
        FROM portfolio P INNER JOIN address_set_cache C ON P.address=C.address
        WHERE C.set_id = p_set_id AND P.time <= last_time
        ORDER  BY P.address, P.time DESC
    )
    INSERT INTO address_set_summary (set_id, time, equity, pnl) SELECT p_set_id AS set_id, last_time AS time, COALESCE(sum(equity), 0) AS equity, COALESCE(sum(pnl), 0) AS pnl FROM _last ON CONFLICT (set_id, time) DO UPDATE SET equity = EXCLUDED.equity, pnl = EXCLUDED.pnl;
END;
$$;

CREATE OR REPLACE FUNCTION refresh_address_set_summary_all(p_set_id INTEGER)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE r record;
BEGIN
    DELETE FROM address_set_summary WHERE set_id = p_set_id;
    FOR r IN (SELECT time FROM portfolio_times ORDER BY time) LOOP
        PERFORM refresh_address_set_summary(p_set_id, r.time);
    END LOOP;
END;
$$;

-- 更新地址集合缓存的函数
CREATE OR REPLACE FUNCTION refresh_address_set_cache(p_set_id INTEGER)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_sql TEXT;
    v_excluded_addresses TEXT[];
    r record;
BEGIN
    -- 获取SQL查询
    SELECT sql_query, excluded_addresses INTO v_sql, v_excluded_addresses FROM address_set WHERE id = p_set_id;
    
    -- 清除旧的缓存数据
    DELETE FROM address_set_cache WHERE set_id = p_set_id;
    -- 如果排除地址列表非空且有元素，则排除这些地址
    IF array_length(v_excluded_addresses, 1) > 0 THEN
        -- 执行SQL查询并排除指定地址
        EXECUTE format(
            'INSERT INTO address_set_cache (set_id, address) 
             SELECT $1, address FROM (%s) q 
             WHERE address NOT IN (SELECT unnest($2))',
            v_sql
        ) USING p_set_id, v_excluded_addresses;
    ELSE
        -- 如果排除地址列表为空，使用整个查询结果
        EXECUTE format(
            'INSERT INTO address_set_cache (set_id, address) 
             SELECT $1, address FROM (%s) q',
            v_sql
        ) USING p_set_id;
    END IF;

    -- 刷新地址集合统计数据
    FOR r IN (SELECT time FROM portfolio_times ORDER BY time) LOOP
        PERFORM refresh_address_set_summary(p_set_id, r.time);
    END LOOP;
END;
$$;

-- 用户交易记录
CREATE TABLE IF NOT EXISTS hyper_fills (
    time TIMESTAMPTZ NOT NULL,
    tid BIGINT NOT NULL,
    oid BIGINT NOT NULL,
    address VARCHAR(42) NOT NULL,
    coin VARCHAR(10) NOT NULL,
    price NUMERIC(18, 6) NOT NULL,
    size NUMERIC(18, 6) NOT NULL,
    dir VARCHAR(100) NOT NULL,
    pnl NUMERIC(18, 6) NOT NULL DEFAULT 0,
    fee NUMERIC(18, 6) NOT NULL DEFAULT 0,
    PRIMARY KEY (tid, time)
);
SELECT create_hypertable('hyper_fills', 'time', chunk_time_interval => INTERVAL '7 days');
CREATE INDEX idx_hyper_fills_address ON hyper_fills (address);
CREATE INDEX idx_hyper_fills_coin ON hyper_fills (coin);

-- 每个地址手续费占盈亏比例（7天，15天，30天）
CREATE VIEW fill_fee AS
SELECT V.address, (V.vol_7d*0.045/ABS(P.pnl))::NUMERIC(18, 4) AS fee_7d, (V.vol_30d*0.045/ABS(P.pnl))::NUMERIC(18, 4) AS fee_30d, (V.vol_all*0.045/ABS(P.pnl))::NUMERIC(18, 4) AS fee_all FROM portfolio_volume V INNER JOIN portfolio_last P ON V.address=P.address;

-- 交易量记录表
CREATE TABLE IF NOT EXISTS portfolio_volume (
    address VARCHAR(42) NOT NULL, -- 地址
    vol_7d NUMERIC(18, 6) NOT NULL, -- 7天交易量
    vol_30d NUMERIC(18, 6) NOT NULL, -- 30天交易量
    vol_all NUMERIC(18, 6) NOT NULL, -- 累计交易量
    update_time TIMESTAMPTZ NOT NULL, -- 更新时间
    PRIMARY KEY (address)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_volume_address ON portfolio_volume (address);

-- 每个地址平均开仓频次（7天，15天，30天）
CREATE MATERIALIZED VIEW fill_summary AS
WITH
times AS (
    SELECT address, COUNT(1) FILTER (WHERE time > NOW() - INTERVAL '7 days') AS r7, COUNT(1) FILTER (WHERE time > NOW() - INTERVAL '15 days') AS r15, COUNT(1) AS r30 FROM hyper_fills WHERE time > NOW() - INTERVAL '30 days' GROUP BY address
),
summary AS (
    SELECT address,
           AVG(size * price) AS avg_value,
           AVG(pnl/(size * price)) FILTER (WHERE pnl > 0) AS pnl_per_value_1,
           AVG(pnl/(size * price)) FILTER (WHERE pnl < 0) AS pnl_per_value_2,
           SUM(fee)/SUM(ABS(pnl)) FILTER (WHERE pnl!=0) AS fee_per_pnl,
           COUNT(1) AS num
    FROM hyper_fills GROUP BY address
)
SELECT S.address, T.r7::NUMERIC/7 AS r7, T.r15::NUMERIC/15 AS r15, T.r30::NUMERIC/30 AS r30, avg_value, pnl_per_value_1, pnl_per_value_2, fee_per_pnl, num FROM summary S LEFT JOIN times T ON S.address=T.address;
