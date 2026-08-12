-- =============================================================================
-- BOOTSTRAP / REFERENCE DUMP ONLY
-- Source of truth for schema evolution: Alembic (`migrations/versions/`).
-- Do NOT mount this file as docker-entrypoint-initdb.d in production/paper.
-- Fresh environments must run: `alembic upgrade head`
-- This file mirrors Alembic revision 20260802_0008 target state for humans.
-- Later revisions are NOT reflected below; read the migrations for:
--   20260804_0009 — ledger provenance, critical_events, reconciliation_runs
--   20260806_0010 — exchange_orders, exchange_fills, positions.exchange_order_id
--                   (see docs/EXCHANGE_ADAPTER.md)
-- =============================================================================

CREATE TABLE IF NOT EXISTS wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    address TEXT NOT NULL UNIQUE,
    win_rate NUMERIC(7, 4) NOT NULL DEFAULT 0,
    total_trades INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty NUMERIC(18, 8) NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'FILLED', 'REJECTED', 'FAILED')),
    wallet_id UUID NOT NULL REFERENCES wallets (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS positions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol TEXT NOT NULL,
    entry_price NUMERIC(18, 8) NOT NULL,
    qty NUMERIC(18, 8) NOT NULL,
    pnl NUMERIC(18, 8) NOT NULL DEFAULT 0,
    wallet_id UUID NOT NULL REFERENCES wallets (id),
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    exit_price NUMERIC(18, 8),
    close_reason TEXT CHECK (
        close_reason IS NULL
        OR close_reason IN (
            'STOP_LOSS',
            'TAKE_PROFIT',
            'ADMIN',
            'RISK',
            'KILL_SWITCH',
            'LEAD_CLOSE'
        )
    ),
    stop_loss_price NUMERIC(18, 8),
    take_profit_price NUMERIC(18, 8),
    reserved_margin NUMERIC(18, 8),
    order_id UUID UNIQUE REFERENCES paper_orders (id),
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at TIMESTAMPTZ,
    CHECK (
        (closed_at IS NULL AND exit_price IS NULL AND close_reason IS NULL)
        OR (
            closed_at IS NOT NULL
            AND exit_price IS NOT NULL
            AND close_reason IS NOT NULL
        )
    )
);

CREATE TABLE IF NOT EXISTS daily_stats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL UNIQUE,
    total_pnl NUMERIC(18, 8) NOT NULL DEFAULT 0,
    trade_count INTEGER NOT NULL DEFAULT 0,
    max_drawdown NUMERIC(18, 8) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS master_pool (
    id SMALLINT PRIMARY KEY,
    total_capital NUMERIC(18, 8) NOT NULL,
    allocated_capital NUMERIC(18, 8) NOT NULL DEFAULT 0,
    available_capital NUMERIC(18, 8) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT master_pool_singleton_check CHECK (id = 1),
    CONSTRAINT master_pool_total_nonneg CHECK (total_capital >= 0),
    CONSTRAINT master_pool_allocated_nonneg CHECK (allocated_capital >= 0),
    CONSTRAINT master_pool_available_nonneg CHECK (available_capital >= 0),
    CONSTRAINT master_pool_conservation CHECK (
        allocated_capital + available_capital = total_capital
    )
);

CREATE TABLE IF NOT EXISTS wallet_balances (
    wallet_id UUID PRIMARY KEY REFERENCES wallets (id),
    initial_capital NUMERIC(18, 8) NOT NULL,
    current_equity NUMERIC(18, 8) NOT NULL,
    available_balance NUMERIC(18, 8) NOT NULL,
    reserved_margin NUMERIC(18, 8) NOT NULL DEFAULT 0,
    unrealized_pnl NUMERIC(18, 8) NOT NULL DEFAULT 0,
    realized_pnl NUMERIC(18, 8) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT wallet_balances_initial_nonneg CHECK (initial_capital >= 0),
    CONSTRAINT wallet_balances_available_nonneg CHECK (available_balance >= 0),
    CONSTRAINT wallet_balances_reserved_nonneg CHECK (reserved_margin >= 0),
    CONSTRAINT wallet_balances_conservation CHECK (
        available_balance + reserved_margin = initial_capital + realized_pnl
    ),
    CONSTRAINT wallet_balances_equity CHECK (
        current_equity = available_balance + reserved_margin + unrealized_pnl
    )
);

CREATE TABLE IF NOT EXISTS capital_ledger (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID REFERENCES wallets (id),
    entry_type TEXT NOT NULL CHECK (
        entry_type IN (
            'MASTER_INIT',
            'ALLOCATE',
            'RESERVE',
            'RELEASE',
            'REALIZE_PNL',
            'MARK_UNREALIZED'
        )
    ),
    amount NUMERIC(18, 8) NOT NULL,
    balance_after_available NUMERIC(18, 8),
    balance_after_reserved NUMERIC(18, 8),
    position_id UUID REFERENCES positions (id),
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_paper_orders_symbol ON paper_orders (symbol);
CREATE INDEX IF NOT EXISTS idx_paper_orders_created_at ON paper_orders (created_at);
CREATE INDEX IF NOT EXISTS idx_paper_orders_wallet_id ON paper_orders (wallet_id);
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions (symbol);
CREATE INDEX IF NOT EXISTS idx_positions_opened_at ON positions (opened_at);
CREATE INDEX IF NOT EXISTS idx_positions_closed_at ON positions (closed_at);
CREATE INDEX IF NOT EXISTS idx_positions_wallet_id ON positions (wallet_id);
CREATE INDEX IF NOT EXISTS idx_capital_ledger_wallet_id ON capital_ledger (wallet_id);
CREATE INDEX IF NOT EXISTS idx_capital_ledger_created_at ON capital_ledger (created_at);
CREATE UNIQUE INDEX IF NOT EXISTS uq_capital_ledger_reserve_position
    ON capital_ledger (position_id)
    WHERE entry_type = 'RESERVE' AND position_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_capital_ledger_release_position
    ON capital_ledger (position_id)
    WHERE entry_type = 'RELEASE' AND position_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_capital_ledger_realize_position
    ON capital_ledger (position_id)
    WHERE entry_type = 'REALIZE_PNL' AND position_id IS NOT NULL;

-- Legacy linkage registries (Phase 5): non-fatal historical orphans snapshotted at upgrade.
CREATE TABLE IF NOT EXISTS phase5_legacy_unlinked_orders (
    order_id UUID PRIMARY KEY REFERENCES paper_orders (id)
);
CREATE TABLE IF NOT EXISTS phase5_legacy_unlinked_positions (
    position_id UUID PRIMARY KEY REFERENCES positions (id)
);

-- Phase 6: risk controls / kill switch / SoD equity
CREATE TABLE IF NOT EXISTS risk_control_lock (
    id SMALLINT PRIMARY KEY,
    CONSTRAINT risk_control_lock_singleton CHECK (id = 1)
);
INSERT INTO risk_control_lock (id) VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS equity_sod_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    as_of_date DATE NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('GLOBAL', 'WALLET')),
    wallet_id UUID REFERENCES wallets (id),
    equity NUMERIC(18, 8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT equity_sod_scope_wallet_check CHECK (
        (scope = 'GLOBAL' AND wallet_id IS NULL)
        OR (scope = 'WALLET' AND wallet_id IS NOT NULL)
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_equity_sod_global_date
    ON equity_sod_snapshots (as_of_date)
    WHERE scope = 'GLOBAL';
CREATE UNIQUE INDEX IF NOT EXISTS uq_equity_sod_wallet_date
    ON equity_sod_snapshots (as_of_date, wallet_id)
    WHERE scope = 'WALLET';

CREATE TABLE IF NOT EXISTS kill_switch_state (
    id SMALLINT PRIMARY KEY,
    active BOOLEAN NOT NULL DEFAULT false,
    reason TEXT,
    actor TEXT,
    activated_at TIMESTAMPTZ,
    deactivated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT kill_switch_state_singleton CHECK (id = 1)
);
INSERT INTO kill_switch_state (id, active) VALUES (1, false) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS kill_switch_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL CHECK (event_type IN ('ACTIVATE', 'DEACTIVATE')),
    active_after BOOLEAN NOT NULL,
    reason TEXT,
    actor TEXT,
    equity_at_event NUMERIC(18, 8),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_kill_switch_events_created_at
    ON kill_switch_events (created_at);

CREATE TABLE IF NOT EXISTS risk_denials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reason_code TEXT NOT NULL,
    detail TEXT,
    wallet_id UUID REFERENCES wallets (id),
    sod_equity NUMERIC(18, 8),
    current_equity NUMERIC(18, 8),
    loss_pct NUMERIC(18, 8),
    limit_pct NUMERIC(18, 8),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_risk_denials_created_at ON risk_denials (created_at);
