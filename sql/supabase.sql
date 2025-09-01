-- =========
-- Extensions
-- =========
create extension if not exists "pgcrypto"; -- for gen_random_uuid()

-- =========
-- Enums
-- =========
do $$
begin
  if not exists (select 1 from pg_type where typname = 'planet_enum') then
    create type planet_enum as enum (
      'SUN','MOON','MERCURY','VENUS','MARS','JUPITER','SATURN','URANUS','NEPTUNE','PLUTO'
    );
  end if;

  if not exists (select 1 from pg_type where typname = 'aspect_enum') then
    create type aspect_enum as enum ('conjunction','square','opposition');
  end if;

  if not exists (select 1 from pg_type where typname = 'severity_enum') then
    create type severity_enum as enum ('major','minor');
  end if;

  if not exists (select 1 from pg_type where typname = 'rules_clarity_enum') then
    create type rules_clarity_enum as enum ('clear','medium','ambiguous');
  end if;

  if not exists (select 1 from pg_type where typname = 'decision_enum') then
    create type decision_enum as enum ('YES','NO','Skip');
  end if;

  if not exists (select 1 from pg_type where typname = 'side_enum') then
    create type side_enum as enum ('YES','NO');
  end if;

  if not exists (select 1 from pg_type where typname = 'category_enum') then
    create type category_enum as enum (
      'geopolitics','conflict','accidents_infrastructure','legal_regulatory','markets_finance',
      'communications_tech','public_sentiment','sports','entertainment','science_health','weather'
    );
  end if;
end$$;

-- =========
-- Helpers
-- =========
create or replace function set_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end$$;

-- =========
-- Users (optional, for Telegram linking)
-- =========
create table if not exists app_user (
  id uuid primary key default gen_random_uuid(),
  telegram_id bigint unique,
  username text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
drop trigger if exists trg_app_user_updated on app_user;
create trigger trg_app_user_updated before update on app_user
for each row execute function set_updated_at();

-- =========
-- Impact Map (versioned) + normalized rules
-- =========
create table if not exists impact_map_versions (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  is_active boolean not null default false,
  notes text,
  json_blob jsonb not null,
  updated_at timestamptz not null default now()
);
create index if not exists idx_impact_map_versions_active on impact_map_versions (is_active);
drop trigger if exists trg_impact_map_versions_updated on impact_map_versions;
create trigger trg_impact_map_versions_updated before update on impact_map_versions
for each row execute function set_updated_at();

create table if not exists impact_map_rules (
  id uuid primary key default gen_random_uuid(),
  version_id uuid not null references impact_map_versions(id) on delete cascade,
  planet1 planet_enum not null,
  planet2 planet_enum not null,
  aspect aspect_enum not null,
  category category_enum not null,
  weight integer not null check (weight between -3 and 3),
  unique (version_id, planet1, planet2, aspect, category)
);
create index if not exists idx_impact_map_rules_key on impact_map_rules (version_id, planet1, planet2, aspect, category);

-- =========
-- Aspect Events (quarterly ephemeris)
-- =========
create table if not exists aspect_events (
  id uuid primary key default gen_random_uuid(),
  quarter text not null,                               -- e.g. '2025-Q3'
  start_utc timestamptz not null,
  peak_utc  timestamptz not null,
  end_utc   timestamptz not null,
  planet1 planet_enum not null,
  planet2 planet_enum not null,
  aspect aspect_enum not null,
  orb_deg numeric(6,3) not null,                       -- e.g. 0.800
  severity severity_enum not null,
  is_eclipse boolean not null default false,
  notes text,
  source text,                                         -- e.g. 'skyfield'
  confidence numeric(3,2) default 0.90,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (quarter, planet1, planet2, aspect, peak_utc)
);
create index if not exists idx_aspect_events_quarter on aspect_events (quarter, peak_utc);
create index if not exists idx_aspect_events_combo on aspect_events (planet1, planet2, aspect, peak_utc);
drop trigger if exists trg_aspect_events_updated on aspect_events;
create trigger trg_aspect_events_updated before update on aspect_events
for each row execute function set_updated_at();

-- =========
-- Markets (Polymarket normalized + LLM tags)
-- =========
create table if not exists markets (
  id text primary key,                                 -- Polymarket id
  title text not null,
  description text,
  rules text,
  deadline_utc timestamptz not null,
  price_yes double precision check (price_yes between 0 and 1),
  liquidity_score double precision,
  rules_clarity rules_clarity_enum,
  category_tags category_enum[] not null default '{}',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists idx_markets_deadline on markets (deadline_utc);
create index if not exists idx_markets_rules_clarity on markets (rules_clarity);
create index if not exists idx_markets_category_tags on markets using gin (category_tags);
drop trigger if exists trg_markets_updated on markets;
create trigger trg_markets_updated before update on markets
for each row execute function set_updated_at();

-- Optional: cache of raw LLM tag JSON
create table if not exists cached_market_tags (
  market_id text primary key references markets(id) on delete cascade,
  model text not null,
  response_json jsonb not null,
  created_at timestamptz not null default now()
);

-- =========
-- Aspect Contributions (per market x aspect)
-- =========
create table if not exists aspect_contributions (
  id uuid primary key default gen_random_uuid(),
  market_id text not null references markets(id) on delete cascade,
  aspect_id uuid not null references aspect_events(id) on delete cascade,
  temporal_w double precision not null,
  angular_w  double precision not null,
  severity_w double precision not null,
  category_w double precision not null,
  contribution double precision not null,
  params_json jsonb,                                   -- e.g., lambda_days, orb_limits used
  explain text,
  created_at timestamptz not null default now(),
  unique (market_id, aspect_id)
);
create index if not exists idx_aspect_contrib_market on aspect_contributions (market_id);
create index if not exists idx_aspect_contrib_aspect on aspect_contributions (aspect_id);

-- =========
-- Opportunities (per market result per scan)
-- =========
create table if not exists opportunities (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references app_user(id),
  market_id text not null references markets(id) on delete cascade,
  p0 double precision not null check (p0 between 0 and 1),
  s_astro double precision not null,
  p_astro double precision not null check (p_astro between 0 and 1),
  edge_net double precision not null check (edge_net between -1 and 1),
  size_fraction double precision not null check (size_fraction between 0 and 1),
  decision decision_enum not null,
  lambda_gain double precision not null,
  costs jsonb not null,                                -- {"fee_bps":60,"spread":0.01,"slippage":0.005}
  config_snapshot jsonb not null,                      -- scan parameters snapshot
  approved boolean not null default false,
  approved_by uuid references app_user(id),
  notes text,
  created_at timestamptz not null default now()
);
create index if not exists idx_opportunities_market on opportunities (market_id, created_at desc);
create index if not exists idx_opportunities_rank on opportunities (decision, edge_net desc) where decision <> 'Skip';

-- =========
-- Backtests & Trades (paper)
-- =========
create table if not exists backtests (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references app_user(id),
  name text not null,
  params_json jsonb not null,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  metrics_json jsonb
);

create table if not exists trades_paper (
  id uuid primary key default gen_random_uuid(),
  backtest_id uuid references backtests(id) on delete cascade,
  user_id uuid references app_user(id),
  market_id text references markets(id),
  strategy_tag text,
  side side_enum not null,
  entry_price double precision not null check (entry_price between 0 and 1),
  size double precision not null check (size >= 0),
  ts_open timestamptz not null default now(),
  ts_close timestamptz,
  exit_price double precision check (exit_price between 0 and 1),
  outcome smallint check (outcome in (0,1)),
  fees double precision not null default 0,
  pnl double precision,
  created_at timestamptz not null default now()
);
create index if not exists idx_trades_backtest on trades_paper (backtest_id);
create index if not exists idx_trades_market on trades_paper (market_id);

-- =========
-- P&L Daily (optional, for summaries)
-- =========
create table if not exists pnl_daily (
  id bigserial primary key,
  user_id uuid references app_user(id),
  as_of date not null,
  realized double precision not null default 0,
  unrealized double precision not null default 0,
  fees double precision not null default 0,
  equity_close double precision not null default 0,
  unique (user_id, as_of)
);

-- =========
-- App Config (active knobs for scans)
-- =========
create table if not exists app_config (
  id smallint primary key default 1,
  lambda_gain double precision not null default 0.10,
  edge_threshold double precision not null default 0.04,
  lambda_days double precision not null default 5,
  orb_limits jsonb not null default '{"square":8,"opposition":8,"conjunction":6}',
  k_cap double precision not null default 5,
  min_liquidity double precision not null default 0.5,
  min_days_buffer integer not null default 7,
  eclipse_amp double precision not null default 1.0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
insert into app_config (id) values (1) on conflict do nothing;
drop trigger if exists trg_app_config_updated on app_config;
create trigger trg_app_config_updated before update on app_config
for each row execute function set_updated_at();

-- =========
-- Analytics & Backtesting Tables (Session 9)
-- =========
create table if not exists test_runs (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  type text not null check (type in ('backtest', 'forwardtest')),
  config jsonb not null,
  start_date timestamptz not null,
  end_date timestamptz,
  status text not null default 'running' check (status in ('running', 'completed', 'failed', 'stopped')),
  metrics jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists idx_test_runs_type_status on test_runs (type, status);
create index if not exists idx_test_runs_created on test_runs (created_at desc);
drop trigger if exists trg_test_runs_updated on test_runs;
create trigger trg_test_runs_updated before update on test_runs
for each row execute function set_updated_at();

create table if not exists test_trades (
  id uuid primary key default gen_random_uuid(),
  test_run_id uuid not null references test_runs(id) on delete cascade,
  market_id text references markets(id),
  opportunity_id uuid references opportunities(id),
  side text not null check (side in ('YES', 'NO')),
  qty double precision not null check (qty > 0),
  entry_price double precision not null check (entry_price between 0 and 1),
  exit_price double precision check (exit_price between 0 and 1),
  entry_time timestamptz not null,
  exit_time timestamptz,
  fees double precision not null default 0,
  realized_pnl double precision,
  outcome smallint check (outcome in (0, 1)),
  metadata jsonb,
  created_at timestamptz not null default now()
);
create index if not exists idx_test_trades_run on test_trades (test_run_id);
create index if not exists idx_test_trades_market on test_trades (market_id);
create index if not exists idx_test_trades_entry_time on test_trades (entry_time);

create table if not exists test_equity (
  id uuid primary key default gen_random_uuid(),
  test_run_id uuid not null references test_runs(id) on delete cascade,
  ts timestamptz not null,
  equity_usdc double precision not null,
  realized_pnl double precision not null default 0,
  unrealized_pnl double precision not null default 0,
  fees_usdc double precision not null default 0,
  positions_count integer not null default 0,
  created_at timestamptz not null default now()
);
create index if not exists idx_test_equity_run_ts on test_equity (test_run_id, ts);

create table if not exists test_opportunities (
  id uuid primary key default gen_random_uuid(),
  test_run_id uuid not null references test_runs(id) on delete cascade,
  market_id text references markets(id),
  scan_time timestamptz not null,
  p0 double precision not null check (p0 between 0 and 1),
  p_astro double precision not null check (p_astro between 0 and 1),
  edge_net double precision not null,
  decision text not null check (decision in ('BUY', 'SELL', 'HOLD')),
  size_fraction double precision not null check (size_fraction between 0 and 1),
  executed boolean not null default false,
  execution_price double precision check (execution_price between 0 and 1),
  execution_qty double precision check (execution_qty >= 0),
  metadata jsonb,
  created_at timestamptz not null default now()
);
create index if not exists idx_test_opps_run_scan on test_opportunities (test_run_id, scan_time);
create index if not exists idx_test_opps_executed on test_opportunities (test_run_id, executed);

-- =========
-- RLS (optional; keep disabled for service role usage)  
-- =========
-- alter table <name> enable row level security;
-- (Add policies later if exposing anon client directly)

