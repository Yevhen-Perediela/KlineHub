import { useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Database,
  LockKeyhole,
  LogOut,
  PauseCircle,
  PlayCircle,
  Plus,
  RefreshCw,
  Server,
  Trash2,
  Wifi,
  Waves,
  Search,
  Clock3,
  CandlestickChart,
  Gauge,
  Radio,
  Layers3,
  TimerReset,
  HardDrive,
} from "lucide-react";

const API_BASE = "";
const DASHBOARD_PASSWORD = "imklinehubadmin";
const DASHBOARD_AUTH_KEY = "klinehub_dashboard_auth";

type InternalHealth = {
  status: string;
  redis: string;
  db: string;
  ws: string;
  ws_connected: boolean;
  ws_connecting: boolean;
  ws_reconnect_count: number;
  active_streams_count: number;
  tracked_pairs_total: number;
  tracked_pairs_active: number;
  candles_persisted_total: number;
  ws_last_error?: string | null;
  ws_last_message_at?: string | null;
  ws_connected_at?: string | null;
  last_kline_event?: Record<string, unknown> | null;
  last_persisted_candle?: Record<string, unknown> | null;
  chart_ws?: Record<string, number>;
};

type InternalStats = {
  tracked_pairs_total: number;
  tracked_pairs_active: number;
  tracked_pairs_paused: number;
  redis_ok: boolean;
  db_ok: boolean;
  active_streams_count: number;
  ws_connected: boolean;
  ws_reconnect_count: number;
  candles_persisted_total: number;
  chart_ws?: Record<string, number>;
};

type TrackedPair = {
  id: number;
  exchange: string;
  market: string;
  symbol: string;
  interval: string;
  status: string;
  source: string;
  priority: number;
  created_at: string;
  updated_at: string;
};

type PairListResponse = {
  items: TrackedPair[];
  count: number;
};

type OperationalOps = {
  websocket_clients: Record<string, number>;
  exchange_limits: Record<
    string,
    {
      requests_total: number;
      errors_total: number;
      rate_limited_total: number;
      last_status_code?: number | null;
      last_error?: string | null;
      last_request_at?: string | null;
      limit?: string | null;
      remaining?: string | null;
      used_weight?: string | null;
      reset_at?: string | null;
    }
  >;
  tracked_pair_lifecycle: {
    by_source: Record<string, number>;
    by_status: Record<string, number>;
    on_demand_active: number;
    on_demand_expiring_1h: number;
    recent_changes: Array<{
      exchange: string;
      market: string;
      symbol: string;
      interval: string;
      status: string;
      source: string;
      auto_stop_at?: string | null;
      updated_at?: string | null;
    }>;
  };
  stream_workers: Array<{
    worker_id: number;
    exchange: string;
    market: string;
    provider: string;
    transport: string;
    stream_count: number;
    status: string;
  }>;
  cold_streams: {
    warmup_total: number;
    warmup_failed_total: number;
    active_streams_count: number;
    active_streams: string[];
  };
  data_freshness: Array<{
    exchange: string;
    market: string;
    symbol: string;
    interval: string;
    status: string;
    source: string;
    last_closed_open_time?: number | null;
    age_sec?: number | null;
    open_cached: boolean;
    last_cached: boolean;
    stale: boolean;
  }>;
};

type RefreshPopularPairsResponse = {
  ok: boolean;
  dry_run: boolean;
  mode: "pause" | "delete";
  summary: {
    desired_total: number;
    current_total: number;
    to_add: number;
    to_resume: number;
    to_pause: number;
    to_delete: number;
    unchanged: number;
    failed: number;
    reload_triggered: boolean;
  };
  groups: Record<
    string,
    {
      desired: number;
      added: number;
      resumed: number;
      paused: number;
      deleted: number;
      unchanged: number;
    }
  >;
  failed_items: Array<{
    exchange: string;
    market: string;
    symbol: string;
    interval: string;
    action: string;
    error?: string | null;
  }>;
};

type FormState = {
  exchange: string;
  market: string;
  symbol: string;
  interval: string;
  backfill_limit: string;
};

const defaultForm: FormState = {
  exchange: "bybit",
  market: "futures",
  symbol: "BTCUSDT",
  interval: "1h",
  backfill_limit: "300",
};

const exchangeOptions = ["bybit", "binance", "oanda"];
const marketOptions = ["spot", "futures", "forex"];
const intervalOptions = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"];

function formatDate(value?: string | null) {
  if (!value) return "—";
  const d = new Date(value);
  return isNaN(d.getTime()) ? value : d.toLocaleString();
}

function formatUnixMs(value?: number | null) {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}

function formatAge(seconds?: number | null) {
  if (seconds === null || seconds === undefined) return "—";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
  return `${Math.floor(seconds / 86400)}d`;
}

function statusPill(ok: boolean) {
  return ok
    ? "border-emerald-400/20 bg-emerald-500/15 text-emerald-200"
    : "border-rose-400/20 bg-rose-500/15 text-rose-200";
}

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
    ...init,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `HTTP ${res.status}`);
  }

  return res.json();
}

function StatCard({
  title,
  value,
  subtitle,
  icon: Icon,
  accent,
}: {
  title: string;
  value: string | number;
  subtitle?: string;
  icon: any;
  accent: string;
}) {
  return (
    <div className="relative overflow-hidden rounded-[28px] border border-white/10 bg-white/5 p-5 shadow-2xl shadow-black/20 backdrop-blur-xl">
      <div className={`absolute inset-x-0 top-0 h-px ${accent}`} />
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="text-sm text-slate-400">{title}</div>
          <div className="mt-2 text-3xl font-semibold tracking-tight text-white">{value}</div>
          {subtitle ? <div className="mt-2 text-sm text-slate-400">{subtitle}</div> : null}
        </div>
        <div className="rounded-2xl border border-white/10 bg-white/5 p-3">
          <Icon className="h-5 w-5 text-slate-100" />
        </div>
      </div>
    </div>
  );
}

function CompactStatCard({
  title,
  value,
  subtitle,
  icon: Icon,
  accent,
}: {
  title: string;
  value: string | number;
  subtitle?: string;
  icon: any;
  accent: string;
}) {
  return (
    <div className="relative overflow-hidden rounded-[24px] border border-white/10 bg-white/[0.06] p-4 shadow-xl shadow-black/15">
      <div className={`absolute inset-x-0 top-0 h-px ${accent}`} />
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="text-sm leading-5 text-slate-400">{title}</div>
          <div className="mt-3 break-words text-4xl font-semibold leading-none tracking-tight text-white">
            {value}
          </div>
          {subtitle ? <div className="mt-3 text-sm leading-5 text-slate-400">{subtitle}</div> : null}
        </div>
        <div className="shrink-0 rounded-2xl border border-white/10 bg-white/5 p-2.5">
          <Icon className="h-4 w-4 text-slate-100" />
        </div>
      </div>
    </div>
  );
}

function JsonCard({ title, value }: { title: string; value: unknown }) {
  return (
    <div className="rounded-[28px] border border-white/10 bg-slate-950/50 p-4 shadow-inner shadow-black/20">
      <div className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">{title}</div>
      <pre className="overflow-auto rounded-2xl border border-white/5 bg-black/20 p-4 text-xs leading-6 text-slate-300">
        {JSON.stringify(value ?? null, null, 2)}
      </pre>
    </div>
  );
}

function HealthBadge({ label, ok }: { label: string; ok: boolean }) {
  return (
    <div className={`flex items-center justify-between rounded-2xl border px-4 py-3 ${statusPill(ok)}`}>
      <span className="text-sm font-medium">{label}</span>
      {ok ? <CheckCircle2 className="h-4 w-4" /> : <AlertTriangle className="h-4 w-4" />}
    </div>
  );
}

function LoginGate({ onUnlock }: { onUnlock: () => void }) {
  const [password, setPassword] = useState("");
  const [loginError, setLoginError] = useState("");

  const submitLogin = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (password === DASHBOARD_PASSWORD) {
      window.sessionStorage.setItem(DASHBOARD_AUTH_KEY, password);
      setLoginError("");
      onUnlock();
      return;
    }

    setLoginError("Wrong password");
    setPassword("");
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-black p-4 text-slate-100">
      <form
        onSubmit={submitLogin}
        className="w-full max-w-sm rounded-lg border border-white/10 bg-zinc-950 p-6 shadow-2xl shadow-black/40"
      >
        <div className="mb-6 flex items-center gap-3">
          <div className="rounded-lg border border-cyan-400/20 bg-cyan-500/10 p-3">
            <LockKeyhole className="h-5 w-5 text-cyan-200" />
          </div>
          <div>
            <div className="text-sm font-semibold uppercase tracking-[0.18em] text-cyan-200">KlineHub</div>
            <h1 className="mt-1 text-xl font-semibold text-white">Dashboard access</h1>
          </div>
        </div>

        <label className="block text-sm text-slate-300">
          Password
          <input
            autoFocus
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            type="password"
            className="mt-2 w-full rounded-lg border border-white/10 bg-black px-4 py-3 text-white outline-none placeholder:text-slate-600 focus:border-cyan-400/40"
            placeholder="Enter password"
          />
        </label>

        {loginError ? <div className="mt-3 text-sm text-rose-300">{loginError}</div> : null}

        <button
          type="submit"
          className="mt-5 w-full rounded-lg bg-cyan-400 px-4 py-3 text-sm font-semibold text-slate-950 transition hover:bg-cyan-300"
        >
          Enter dashboard
        </button>
      </form>
    </div>
  );
}

export default function KlineHubMonitorDashboard() {
  const [authenticated, setAuthenticated] = useState(
    () => window.sessionStorage.getItem(DASHBOARD_AUTH_KEY) === DASHBOARD_PASSWORD
  );
  const [health, setHealth] = useState<InternalHealth | null>(null);
  const [stats, setStats] = useState<InternalStats | null>(null);
  const [ops, setOps] = useState<OperationalOps | null>(null);
  const [pairs, setPairs] = useState<TrackedPair[]>([]);
  const [loading, setLoading] = useState(true);
  const [mutating, setMutating] = useState(false);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [form, setForm] = useState<FormState>(defaultForm);
  const [refreshDryRun, setRefreshDryRun] = useState(false);
  const [refreshMode, setRefreshMode] = useState<"pause" | "delete">("pause");
  const [refreshResult, setRefreshResult] = useState<RefreshPopularPairsResponse | null>(null);

  const loadAll = async () => {
    setError("");
    setLoading(true);
    try {
      const [healthRes, statsRes, pairsRes, opsRes] = await Promise.all([
        fetchJson<InternalHealth>("/internal/health"),
        fetchJson<InternalStats>("/internal/stats"),
        fetchJson<PairListResponse>("/internal/pairs"),
        fetchJson<OperationalOps>("/internal/ops"),
      ]);
      setHealth(healthRes);
      setStats(statsRes);
      setPairs(pairsRes.items);
      setOps(opsRes);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load data");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!authenticated) return;

    loadAll();
    const id = window.setInterval(loadAll, 10000);
    return () => window.clearInterval(id);
  }, [authenticated]);

  const logout = () => {
    window.sessionStorage.removeItem(DASHBOARD_AUTH_KEY);
    setAuthenticated(false);
    setHealth(null);
    setStats(null);
    setOps(null);
    setPairs([]);
  };

  const filteredPairs = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return pairs;
    return pairs.filter((pair) =>
      [pair.symbol, pair.exchange, pair.market, pair.interval, pair.status]
        .join(" ")
        .toLowerCase()
        .includes(q)
    );
  }, [pairs, search]);

  const staleFreshness = useMemo(
    () => (ops?.data_freshness ?? []).filter((item) => item.stale).slice(0, 8),
    [ops]
  );

  const freshestPairs = useMemo(
    () =>
      [...(ops?.data_freshness ?? [])]
        .sort((a, b) => (a.age_sec ?? Number.MAX_SAFE_INTEGER) - (b.age_sec ?? Number.MAX_SAFE_INTEGER))
        .slice(0, 8),
    [ops]
  );

  const submitAddPair = async () => {
    setMutating(true);
    setError("");
    try {
      await fetchJson("/internal/pairs", {
        method: "POST",
        body: JSON.stringify({
          exchange: form.exchange,
          market: form.market,
          symbol: form.symbol.trim().toUpperCase(),
          interval: form.interval,
          backfill_limit: Number(form.backfill_limit || 300),
        }),
      });
      await loadAll();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to add pair");
    } finally {
      setMutating(false);
    }
  };

  const pairAction = async (pair: TrackedPair, action: "pause" | "resume" | "delete") => {
    setMutating(true);
    setError("");
    try {
      const base = `/internal/pairs/${pair.exchange}/${pair.market}/${pair.symbol}/${pair.interval}`;
      if (action === "delete") {
        await fetchJson(base, { method: "DELETE" });
      } else {
        await fetchJson(`${base}/${action}`, { method: "POST" });
      }
      await loadAll();
    } catch (err) {
      setError(err instanceof Error ? err.message : `Failed to ${action} pair`);
    } finally {
      setMutating(false);
    }
  };

  const submitPopularRefresh = async () => {
    setMutating(true);
    setError("");
    try {
      const result = await fetchJson<RefreshPopularPairsResponse>("/internal/refresh-popular-pairs", {
        method: "POST",
        body: JSON.stringify({
          dry_run: refreshDryRun,
          mode: refreshMode,
          crypto_interval: "1h",
          oanda_interval: "1m",
          binance: {
            spot_base_limit: 150,
            futures_base_limit: 150,
            quotes: ["USDT", "USDC"],
          },
          bybit: {
            spot_base_limit: 100,
            futures_base_limit: 100,
            quotes: ["USDT", "USDC"],
          },
          oanda: {
            enable_forex: true,
            enable_metals: true,
            forex_symbols: [
              "EUR_USD",
              "GBP_USD",
              "USD_JPY",
              "AUD_USD",
              "USD_CAD",
              "USD_CHF",
              "NZD_USD",
              "EUR_CHF",
              "EUR_CAD",
              "EUR_AUD",
              "EUR_NZD",
              "EUR_JPY",
              "GBP_JPY",
              "EUR_GBP",
              "GBP_CHF",
              "GBP_CAD",
              "GBP_AUD",
              "AUD_JPY",
              "AUD_CAD",
              "AUD_CHF",
              "CAD_JPY",
              "CAD_CHF",
              "CHF_JPY",
              "NZD_JPY",
              "USD_SEK",
              "USD_NOK",
              "USD_SGD",
              "EUR_SEK",
              "EUR_NOK",
            ],
            metals_symbols: ["XAU_USD", "XAG_USD"],
          },
        }),
      });
      setRefreshResult(result);
      await loadAll();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to refresh popular pairs");
    } finally {
      setMutating(false);
    }
  };

  if (!authenticated) {
    return <LoginGate onUnlock={() => setAuthenticated(true)} />;
  }

  return (
    <div className="min-h-screen bg-black text-slate-100">
      <div className="relative mx-auto max-w-7xl p-4 md:p-6 lg:p-8">
        <div className="mb-8 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="inline-flex items-center gap-2 rounded-lg border border-cyan-400/20 bg-cyan-500/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-cyan-200">
              <Waves className="h-3.5 w-3.5" />
              KlineHub
            </div>
            <h1 className="mt-4 text-4xl font-semibold tracking-tight text-white md:text-5xl">
              Market Data Monitor
            </h1>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-400 md:text-base">
              Clean operational view for stream health, tracked pairs, persistence, and fast control actions.
            </p>
          </div>

          <div className="flex flex-wrap gap-2">
            <button
              onClick={loadAll}
              disabled={loading || mutating}
              className="inline-flex items-center justify-center gap-2 rounded-lg border border-white/10 bg-white/8 px-4 py-2.5 text-sm font-medium text-white backdrop-blur transition hover:bg-white/15 disabled:opacity-50"
            >
              <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
              Refresh
            </button>
            <button
              onClick={logout}
              className="inline-flex items-center justify-center gap-2 rounded-lg border border-white/10 bg-white/8 px-4 py-2.5 text-sm font-medium text-white backdrop-blur transition hover:bg-white/15"
            >
              <LogOut className="h-4 w-4" />
              Logout
            </button>
          </div>
        </div>

        {error ? (
          <div className="mb-6 rounded-[24px] border border-rose-400/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200 shadow-lg shadow-rose-950/20">
            {error}
          </div>
        ) : null}

        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
          <StatCard
            title="Worker status"
            value={health?.status ?? "—"}
            subtitle={`WS ${health?.ws ?? "—"}`}
            icon={Server}
            accent="bg-gradient-to-r from-cyan-400/80 to-blue-500/80"
          />
          <StatCard
            title="Tracked pairs"
            value={stats?.tracked_pairs_total ?? 0}
            subtitle={`Active ${stats?.tracked_pairs_active ?? 0} · Paused ${stats?.tracked_pairs_paused ?? 0}`}
            icon={Activity}
            accent="bg-gradient-to-r from-violet-400/80 to-fuchsia-500/80"
          />
          <StatCard
            title="Active streams"
            value={health?.active_streams_count ?? 0}
            subtitle={`Reconnects ${health?.ws_reconnect_count ?? 0}`}
            icon={Wifi}
            accent="bg-gradient-to-r from-emerald-400/80 to-teal-500/80"
          />
          <StatCard
            title="Persisted candles"
            value={health?.candles_persisted_total ?? 0}
            subtitle={`Last event ${formatDate(health?.ws_last_message_at)}`}
            icon={CandlestickChart}
            accent="bg-gradient-to-r from-amber-400/80 to-orange-500/80"
          />
        </div>

        <div className="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-3">
          <div className="rounded-lg border border-white/10 bg-zinc-950 p-5 shadow-xl shadow-black/40">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold text-white">Realtime gateway</h2>
                <p className="text-sm text-slate-500">Connections from external projects</p>
              </div>
              <Radio className="h-5 w-5 text-cyan-300" />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <CompactStatCard title="Chart clients" value={ops?.websocket_clients.chart_ws_connections_current ?? 0} icon={Wifi} accent="bg-cyan-400/80" />
              <CompactStatCard title="Chart subs" value={ops?.websocket_clients.chart_ws_subscriptions_current ?? 0} icon={Activity} accent="bg-emerald-400/80" />
              <CompactStatCard title="Legacy clients" value={ops?.websocket_clients.legacy_market_clients ?? 0} icon={Server} accent="bg-slate-400/80" />
              <CompactStatCard title="Dropped updates" value={ops?.websocket_clients.chart_ws_dropped_updates_total ?? 0} icon={AlertTriangle} accent="bg-rose-400/80" />
            </div>
          </div>

          <div className="rounded-lg border border-white/10 bg-zinc-950 p-5 shadow-xl shadow-black/40">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold text-white">Exchange limits</h2>
                <p className="text-sm text-slate-500">REST usage, errors, and rate-limit signals</p>
              </div>
              <Gauge className="h-5 w-5 text-amber-300" />
            </div>
            <div className="space-y-3">
              {Object.entries(ops?.exchange_limits ?? {}).map(([exchange, item]) => (
                <div key={exchange} className="rounded-lg border border-white/10 bg-black/50 p-3">
                  <div className="flex items-center justify-between gap-3">
                    <div className="font-semibold uppercase text-white">{exchange}</div>
                    <span className={`rounded-md px-2 py-1 text-xs ${item.rate_limited_total ? "bg-rose-500/15 text-rose-200" : "bg-emerald-500/15 text-emerald-200"}`}>
                      {item.rate_limited_total ? "rate limited" : "ok"}
                    </span>
                  </div>
                  <div className="mt-2 grid grid-cols-3 gap-2 text-xs text-slate-400">
                    <div>Requests <span className="text-white">{item.requests_total}</span></div>
                    <div>Errors <span className="text-white">{item.errors_total}</span></div>
                    <div>429 <span className="text-white">{item.rate_limited_total}</span></div>
                    <div>Status <span className="text-white">{item.last_status_code ?? "—"}</span></div>
                    <div>Remaining <span className="text-white">{item.remaining ?? "—"}</span></div>
                    <div>Weight <span className="text-white">{item.used_weight ?? "—"}</span></div>
                  </div>
                  {item.last_error ? <div className="mt-2 truncate text-xs text-rose-300">{item.last_error}</div> : null}
                </div>
              ))}
              {!Object.keys(ops?.exchange_limits ?? {}).length ? (
                <div className="rounded-lg border border-white/10 bg-black/50 p-4 text-sm text-slate-500">No REST telemetry yet.</div>
              ) : null}
            </div>
          </div>

          <div className="rounded-lg border border-white/10 bg-zinc-950 p-5 shadow-xl shadow-black/40">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold text-white">Pair automation</h2>
                <p className="text-sm text-slate-500">Auto-added, paused, and on-demand tracking</p>
              </div>
              <Layers3 className="h-5 w-5 text-violet-300" />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <CompactStatCard title="On-demand active" value={ops?.tracked_pair_lifecycle.on_demand_active ?? 0} icon={TimerReset} accent="bg-violet-400/80" />
              <CompactStatCard title="Expiring 1h" value={ops?.tracked_pair_lifecycle.on_demand_expiring_1h ?? 0} icon={Clock3} accent="bg-amber-400/80" />
            </div>
            <div className="mt-4 grid grid-cols-2 gap-3 text-sm">
              <div className="rounded-lg border border-white/10 bg-black/50 p-3">
                <div className="mb-2 text-xs uppercase tracking-[0.16em] text-slate-500">By source</div>
                {Object.entries(ops?.tracked_pair_lifecycle.by_source ?? {}).map(([key, value]) => (
                  <div key={key} className="flex justify-between text-slate-300"><span>{key}</span><span className="text-white">{value}</span></div>
                ))}
              </div>
              <div className="rounded-lg border border-white/10 bg-black/50 p-3">
                <div className="mb-2 text-xs uppercase tracking-[0.16em] text-slate-500">By status</div>
                {Object.entries(ops?.tracked_pair_lifecycle.by_status ?? {}).map(([key, value]) => (
                  <div key={key} className="flex justify-between text-slate-300"><span>{key}</span><span className="text-white">{value}</span></div>
                ))}
              </div>
            </div>
          </div>
        </div>

        <div className="mt-6 grid grid-cols-1 gap-4 xl:grid-cols-[1fr_1.2fr]">
          <div className="rounded-lg border border-white/10 bg-zinc-950 p-5 shadow-xl shadow-black/40">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold text-white">Stream workers</h2>
                <p className="text-sm text-slate-500">Upstream worker groups and stream density</p>
              </div>
              <HardDrive className="h-5 w-5 text-emerald-300" />
            </div>
            <div className="space-y-2">
              {(ops?.stream_workers ?? []).map((worker) => (
                <div key={worker.worker_id} className="grid grid-cols-[3rem_1fr_auto] items-center gap-3 rounded-lg border border-white/10 bg-black/50 px-3 py-2 text-sm">
                  <div className="text-slate-500">#{worker.worker_id}</div>
                  <div>
                    <div className="font-medium text-white">{worker.exchange} / {worker.market}</div>
                    <div className="text-xs text-slate-500">{worker.provider} · {worker.transport} · {worker.status}</div>
                  </div>
                  <div className="rounded-md bg-cyan-500/10 px-2 py-1 text-xs text-cyan-200">{worker.stream_count} streams</div>
                </div>
              ))}
              {!(ops?.stream_workers ?? []).length ? (
                <div className="rounded-lg border border-white/10 bg-black/50 p-4 text-sm text-slate-500">No active workers configured.</div>
              ) : null}
            </div>
          </div>

          <div className="rounded-lg border border-white/10 bg-zinc-950 p-5 shadow-xl shadow-black/40">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold text-white">Cold streams and freshness</h2>
                <p className="text-sm text-slate-500">Warmups, stale pairs, and Redis cache availability</p>
              </div>
              <TimerReset className="h-5 w-5 text-fuchsia-300" />
            </div>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              <CompactStatCard title="Warmups" value={ops?.cold_streams.warmup_total ?? 0} icon={RefreshCw} accent="bg-fuchsia-400/80" />
              <CompactStatCard title="Warmup failed" value={ops?.cold_streams.warmup_failed_total ?? 0} icon={AlertTriangle} accent="bg-rose-400/80" />
              <CompactStatCard title="Stale active" value={staleFreshness.length} icon={Clock3} accent="bg-amber-400/80" />
              <CompactStatCard title="Active streams" value={ops?.cold_streams.active_streams_count ?? 0} icon={Wifi} accent="bg-cyan-400/80" />
            </div>
            <div className="mt-4 overflow-hidden rounded-lg border border-white/10 bg-black/50">
              <table className="w-full text-left text-xs">
                <thead className="bg-zinc-950 text-slate-500">
                  <tr>
                    <th className="px-3 py-2 font-medium">Pair</th>
                    <th className="px-3 py-2 font-medium">Age</th>
                    <th className="px-3 py-2 font-medium">Redis</th>
                    <th className="px-3 py-2 font-medium">Last closed</th>
                  </tr>
                </thead>
                <tbody>
                  {freshestPairs.map((item) => (
                    <tr key={`${item.exchange}:${item.market}:${item.symbol}:${item.interval}`} className="border-t border-white/5">
                      <td className="px-3 py-2">
                        <div className="font-medium text-white">{item.symbol}</div>
                        <div className="text-slate-500">{item.exchange}/{item.market} · {item.interval}</div>
                      </td>
                      <td className={`px-3 py-2 ${item.stale ? "text-amber-200" : "text-slate-300"}`}>{formatAge(item.age_sec)}</td>
                      <td className="px-3 py-2 text-slate-300">{item.open_cached ? "open" : "—"} / {item.last_cached ? "last" : "—"}</td>
                      <td className="px-3 py-2 text-slate-400">{formatUnixMs(item.last_closed_open_time)}</td>
                    </tr>
                  ))}
                  {!freshestPairs.length ? (
                    <tr><td colSpan={4} className="px-3 py-6 text-center text-slate-500">No freshness data yet.</td></tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <div className="mt-8 grid grid-cols-1 gap-6 xl:grid-cols-[1.35fr_0.95fr]">
          <div className="rounded-[32px] border border-white/10 bg-white/5 p-5 shadow-2xl shadow-black/20 backdrop-blur-xl md:p-6">
            <div className="mb-5 flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
              <div>
                <h2 className="text-2xl font-semibold text-white">Tracked pairs</h2>
                <p className="mt-1 text-sm text-slate-400">Search and control tracked symbols without leaving the dashboard.</p>
              </div>
              <div className="relative w-full md:max-w-xs">
                <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-500" />
                <input
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search symbol, market, interval..."
                  className="w-full rounded-2xl border border-white/10 bg-slate-950/70 py-2.5 pl-10 pr-4 text-sm text-white outline-none placeholder:text-slate-500 focus:border-cyan-400/30"
                />
              </div>
            </div>

            <div className="overflow-hidden rounded-[24px] border border-white/10 bg-slate-950/40">
              <div className="max-h-[620px] overflow-auto">
                <table className="w-full text-left text-sm">
                  <thead className="sticky top-0 z-10 bg-slate-950/95 backdrop-blur">
                    <tr className="border-b border-white/5 text-slate-400">
                      <th className="px-4 py-3 font-medium">Symbol</th>
                      <th className="px-4 py-3 font-medium">Exchange / Market</th>
                      <th className="px-4 py-3 font-medium">Interval</th>
                      <th className="px-4 py-3 font-medium">Status</th>
                      <th className="px-4 py-3 font-medium">Updated</th>
                      <th className="px-4 py-3 text-right font-medium">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredPairs.map((pair) => (
                      <tr key={`${pair.exchange}-${pair.market}-${pair.symbol}-${pair.interval}`} className="border-b border-white/5 text-slate-200 transition hover:bg-white/[0.03]">
                        <td className="px-4 py-3">
                          <div className="font-semibold text-white">{pair.symbol}</div>
                          <div className="text-xs text-slate-500">#{pair.id}</div>
                        </td>
                        <td className="px-4 py-3 text-slate-300">{pair.exchange} / {pair.market}</td>
                        <td className="px-4 py-3 text-slate-300">{pair.interval}</td>
                        <td className="px-4 py-3">
                          <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${pair.status === "active" ? statusPill(true) : "border-amber-400/20 bg-amber-500/15 text-amber-200"}`}>
                            {pair.status}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-slate-400">{formatDate(pair.updated_at)}</td>
                        <td className="px-4 py-3">
                          <div className="flex flex-wrap justify-end gap-2">
                            {pair.status === "active" ? (
                              <button
                                onClick={() => pairAction(pair, "pause")}
                                disabled={mutating}
                                className="inline-flex items-center gap-1 rounded-xl border border-amber-400/20 bg-amber-500/10 px-3 py-1.5 text-xs font-medium text-amber-200 transition hover:bg-amber-500/20 disabled:opacity-50"
                              >
                                <PauseCircle className="h-3.5 w-3.5" /> Pause
                              </button>
                            ) : (
                              <button
                                onClick={() => pairAction(pair, "resume")}
                                disabled={mutating}
                                className="inline-flex items-center gap-1 rounded-xl border border-emerald-400/20 bg-emerald-500/10 px-3 py-1.5 text-xs font-medium text-emerald-200 transition hover:bg-emerald-500/20 disabled:opacity-50"
                              >
                                <PlayCircle className="h-3.5 w-3.5" /> Resume
                              </button>
                            )}
                            <button
                              onClick={() => pairAction(pair, "delete")}
                              disabled={mutating}
                              className="inline-flex items-center gap-1 rounded-xl border border-rose-400/20 bg-rose-500/10 px-3 py-1.5 text-xs font-medium text-rose-200 transition hover:bg-rose-500/20 disabled:opacity-50"
                            >
                              <Trash2 className="h-3.5 w-3.5" /> Delete
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                    {!filteredPairs.length ? (
                      <tr>
                        <td colSpan={6} className="px-4 py-12 text-center text-slate-500">
                          {loading ? "Loading pairs..." : "No pairs found."}
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div className="space-y-6">
            <div className="rounded-[32px] border border-white/10 bg-white/5 p-5 shadow-2xl shadow-black/20 backdrop-blur-xl md:p-6">
              <div className="mb-6 flex items-center gap-3">
                <div className="rounded-2xl border border-white/10 bg-white/5 p-3">
                  <RefreshCw className="h-4 w-4 text-slate-100" />
                </div>
                <div>
                  <h2 className="text-xl font-semibold text-white">Refresh popular pairs</h2>
                  <p className="text-sm text-slate-400">One backend action to sync Binance, Bybit, and curated OANDA pairs.</p>
                </div>
              </div>

              <div className="grid grid-cols-1 gap-3 text-sm">
                <div className="grid grid-cols-2 gap-3">
                  <label className="rounded-2xl border border-white/10 bg-slate-950/50 px-4 py-3 text-slate-300">
                    <div className="mb-2 text-xs uppercase tracking-[0.18em] text-slate-500">Mode</div>
                    <select
                      value={refreshMode}
                      onChange={(e) => setRefreshMode(e.target.value as "pause" | "delete")}
                      className="w-full bg-transparent text-white outline-none"
                    >
                      <option value="pause" className="bg-slate-950 text-white">Safe pause sync</option>
                      <option value="delete" className="bg-slate-950 text-white">Hard delete sync</option>
                    </select>
                  </label>
                  <label className="flex items-center gap-3 rounded-2xl border border-white/10 bg-slate-950/50 px-4 py-3 text-slate-300">
                    <input
                      type="checkbox"
                      checked={refreshDryRun}
                      onChange={(e) => setRefreshDryRun(e.target.checked)}
                      className="h-4 w-4 rounded border-white/20 bg-slate-900"
                    />
                    <span>Dry run preview only</span>
                  </label>
                </div>

                <button
                  onClick={submitPopularRefresh}
                  disabled={mutating}
                  className="rounded-2xl bg-gradient-to-r from-emerald-400 to-cyan-500 px-4 py-3 text-sm font-semibold text-slate-950 transition hover:opacity-90 disabled:opacity-50"
                >
                  Refresh popular pairs
                </button>
              </div>

              {refreshResult ? (
                <div className="mt-5 space-y-4">
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    <CompactStatCard
                      title="Desired"
                      value={refreshResult.summary.desired_total}
                      accent="bg-gradient-to-r from-cyan-400/80 to-blue-500/80"
                      icon={Activity}
                    />
                    <CompactStatCard
                      title="Add / Resume"
                      value={`${refreshResult.summary.to_add} / ${refreshResult.summary.to_resume}`}
                      accent="bg-gradient-to-r from-emerald-400/80 to-teal-500/80"
                      icon={Plus}
                    />
                    <CompactStatCard
                      title="Pause / Delete"
                      value={`${refreshResult.summary.to_pause} / ${refreshResult.summary.to_delete}`}
                      accent="bg-gradient-to-r from-amber-400/80 to-orange-500/80"
                      icon={PauseCircle}
                    />
                    <CompactStatCard
                      title="Failed"
                      value={refreshResult.summary.failed}
                      subtitle={refreshResult.summary.reload_triggered ? "reload triggered" : "no reload"}
                      accent="bg-gradient-to-r from-rose-400/80 to-pink-500/80"
                      icon={AlertTriangle}
                    />
                  </div>
                  <JsonCard title="Popular pair sync result" value={refreshResult} />
                </div>
              ) : null}
            </div>

            <div className="rounded-[32px] border border-white/10 bg-white/5 p-5 shadow-2xl shadow-black/20 backdrop-blur-xl md:p-6">
              <div className="mb-5 flex items-center gap-3">
                <div className="rounded-2xl border border-white/10 bg-white/5 p-3">
                  <Plus className="h-4 w-4 text-slate-100" />
                </div>
                <div>
                  <h2 className="text-xl font-semibold text-white">Add pair</h2>
                  <p className="text-sm text-slate-400">Track a new symbol and trigger backfill immediately.</p>
                </div>
              </div>

              <div className="grid grid-cols-1 gap-3 text-sm md:grid-cols-2">
                <select
                  value={form.exchange}
                  onChange={(e) => setForm((s) => ({ ...s, exchange: e.target.value }))}
                  className="rounded-2xl border border-white/10 bg-slate-950/70 px-4 py-3 text-white outline-none focus:border-cyan-400/30"
                >
                  {exchangeOptions.map((exchange) => (
                    <option key={exchange} value={exchange} className="bg-slate-950 text-white">
                      {exchange}
                    </option>
                  ))}
                </select>
                <select
                  value={form.market}
                  onChange={(e) => setForm((s) => ({ ...s, market: e.target.value }))}
                  className="rounded-2xl border border-white/10 bg-slate-950/70 px-4 py-3 text-white outline-none focus:border-cyan-400/30"
                >
                  {marketOptions.map((market) => (
                    <option key={market} value={market} className="bg-slate-950 text-white">
                      {market}
                    </option>
                  ))}
                </select>
                <input value={form.symbol} onChange={(e) => setForm((s) => ({ ...s, symbol: e.target.value.toUpperCase() }))} className="rounded-2xl border border-white/10 bg-slate-950/70 px-4 py-3 text-white outline-none placeholder:text-slate-500 focus:border-cyan-400/30" placeholder="symbol" />
                <select
                  value={form.interval}
                  onChange={(e) => setForm((s) => ({ ...s, interval: e.target.value }))}
                  className="rounded-2xl border border-white/10 bg-slate-950/70 px-4 py-3 text-white outline-none focus:border-cyan-400/30"
                >
                  {intervalOptions.map((interval) => (
                    <option key={interval} value={interval} className="bg-slate-950 text-white">
                      {interval}
                    </option>
                  ))}
                </select>
                <input value={form.backfill_limit} onChange={(e) => setForm((s) => ({ ...s, backfill_limit: e.target.value }))} className="rounded-2xl border border-white/10 bg-slate-950/70 px-4 py-3 text-white outline-none placeholder:text-slate-500 focus:border-cyan-400/30 md:col-span-2" placeholder="backfill limit" />
              </div>

              <button
                onClick={submitAddPair}
                disabled={mutating}
                className="mt-4 w-full rounded-2xl bg-gradient-to-r from-cyan-400 to-blue-500 px-4 py-3 text-sm font-semibold text-slate-950 transition hover:opacity-90 disabled:opacity-50"
              >
                Add / Reactivate pair
              </button>
            </div>

            <div className="grid grid-cols-1 gap-6 lg:grid-cols-2 xl:grid-cols-1">
              <div className="rounded-[32px] border border-white/10 bg-white/5 p-5 shadow-2xl shadow-black/20 backdrop-blur-xl md:p-6">
                <h2 className="text-xl font-semibold text-white">Service health</h2>
                <div className="mt-4 space-y-3">
                  <HealthBadge label="Redis" ok={health?.redis === "ok"} />
                  <HealthBadge label="Database" ok={health?.db === "ok"} />
                  <HealthBadge label="Market WebSocket" ok={!!health?.ws_connected} />
                </div>
                <div className="mt-4 rounded-2xl border border-white/10 bg-slate-950/50 p-4 text-sm text-slate-300">
                  <div className="flex items-center gap-2"><Clock3 className="h-4 w-4 text-slate-500" /> Connected at: <span className="text-white">{formatDate(health?.ws_connected_at)}</span></div>
                  <div className="mt-2">Last event: <span className="text-white">{formatDate(health?.ws_last_message_at)}</span></div>
                  <div className="mt-2">Last error: <span className="text-white">{health?.ws_last_error || "—"}</span></div>
                </div>
              </div>

              <JsonCard title="Last kline event" value={health?.last_kline_event} />
              <JsonCard title="Last persisted candle" value={health?.last_persisted_candle} />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
