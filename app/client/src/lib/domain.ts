// Domain logic for Algal Watch: alert tiers, the derived 7-day "chance of Red"
// forecast, and small formatting helpers. All numeric SQL values may arrive as
// strings at runtime, so everything is coerced via Number().
import { profile } from '../../../shared/profile';

export type Tier = 'Green' | 'Amber' | 'Red';

// Alert-tier thresholds. They are bundle variables (shared with the pipeline and
// the model), served by /api/config and applied once at start-up, before any
// page renders, via configureThresholds().
export interface Thresholds {
  /** Cyanobacteria biovolume (mm³/L) at which a site is Amber. */
  amberBiovolume: number;
  /** Cyanobacteria biovolume (mm³/L) at which a site is Red. */
  redBiovolume: number;
  /** Forecast probability at which a day is called Red. */
  redProbability: number;
}

let thresholds: Thresholds | null = null;

export function configureThresholds(t: Thresholds): void {
  thresholds = t;
}

export function getThresholds(): Thresholds {
  if (!thresholds) throw new Error('Alert thresholds have not been configured yet');
  return thresholds;
}

export interface SignalRow {
  site_id: string;
  site_name: string;
  site_type: string;
  valley: string;
  river: string;
  council_name: string;
  base_flow_ml_day: number | string | null;
  latitude: number | string | null;
  longitude: number | string | null;
  biovolume: number | string | null;
  dominant_genus: string | null;
  microcystin: number | string | null;
  sample_date: string | null;
  temp_now: number | string | null;
  temp_prev: number | string | null;
  phyco_now: number | string | null;
  phyco_prev: number | string | null;
  qa_excluded_7d: number | string | null;
  sonde_as_of: string | null;
  flow_now: number | string | null;
  flow_prev: number | string | null;
}

const num = (v: number | string | null | undefined): number => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

export function tierFromBiovolume(biovol: number): Tier {
  const { amberBiovolume, redBiovolume } = getThresholds();
  if (biovol >= redBiovolume) return 'Red';
  if (biovol >= amberBiovolume) return 'Amber';
  return 'Green';
}

const logistic = (x: number) => 1 / (1 + Math.exp(-x));
const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v));

export interface ForecastPoint {
  day: number; // 0..6
  date: Date;
  prob: number; // 0..1 chance of Red
  tier: Tier;
}

export interface SiteForecast {
  tierNow: Tier;
  biovolume: number;
  points: ForecastPoint[];
  /** first day index (0..6) the forecast turns Red, or null if it never does */
  daysUntilRed: number | null;
  redDate: Date | null;
  redProb: number | null; // probability on the crossing day
  peakProb: number;
  willReachRed: boolean; // crosses Red within the 7-day window and not already Red
  nextChange: string;
  asOf: Date;
}

export const HORIZON = 7;

/** Deterministic 7-day forecast derived from the site's current signals. */
export function forecastForSite(row: SignalRow): SiteForecast {
  const { amberBiovolume, redBiovolume, redProbability } = getThresholds();
  const biovolume = num(row.biovolume);
  const tierNow = tierFromBiovolume(biovolume);

  const phycoNow = num(row.phyco_now);
  const phycoPrev = num(row.phyco_prev);
  const tempNow = num(row.temp_now);
  const tempPrev = num(row.temp_prev);
  const qaExcluded = num(row.qa_excluded_7d);

  // Sonde trend is unreliable when many recent readings were QA-excluded, so
  // discount its influence on the growth estimate.
  const reliability = qaExcluded > 20 ? 0.3 : qaExcluded > 5 ? 0.6 : 1;

  // Daily biovolume growth estimate from phycocyanin momentum + warming.
  const momentum = clamp((phycoNow - phycoPrev) / Math.max(phycoPrev, 5), -1, 4);
  const tempBoost = clamp((tempNow - tempPrev) / 12, -0.4, 1.2);
  const growth = clamp(0.02 + (0.05 * momentum + 0.03 * tempBoost) * reliability, -0.03, 0.2);

  // The Red probability is anchored to the *projected biovolume* relative to the
  // Red threshold, so a low-biovolume site can't spike to Red on noise alone.
  const asOf = parseDate(row.sonde_as_of || row.sample_date);
  const points: ForecastPoint[] = [];
  for (let day = 0; day < HORIZON; day++) {
    const projBiovol = biovolume * Math.pow(1 + growth, day);
    const prob = clamp(logistic(1.1 * (projBiovol - redBiovolume)), 0, 0.98);
    const date = addDays(asOf, day);
    let tier: Tier;
    if (prob >= redProbability) tier = 'Red';
    else if (tierNow !== 'Green' || projBiovol >= amberBiovolume || prob >= 0.3) tier = 'Amber';
    else tier = 'Green';
    points.push({ day, date, prob, tier });
  }

  return summarizeForecast(tierNow, biovolume, points, asOf);
}

// A row of the model serving table (config/queries/site_forecast.obo.sql).
export interface ForecastRow {
  site_id: string;
  horizon_day: number | string;
  forecast_date: string;
  red_probability: number | string;
  forecast_tier: string;
  as_of_date: string;
  scored_at: string;
  model_name: string;
  model_alias: string;
  model_version: string;
}

export interface ModelInfo {
  name: string;
  alias: string;
  version: string;
  scoredAt: Date;
}

/** Build a forecast from the model serving table rows for one site. */
export function forecastFromRows(biovolume: number, rows: ForecastRow[]): SiteForecast {
  const tierNow = tierFromBiovolume(biovolume);
  const sorted = [...rows].sort((a, b) => num(a.horizon_day) - num(b.horizon_day));
  const points: ForecastPoint[] = sorted.map((r) => ({
    day: num(r.horizon_day),
    date: parseDate(r.forecast_date),
    prob: clamp(num(r.red_probability), 0, 1),
    tier: (r.forecast_tier as Tier) ?? 'Green',
  }));
  const asOf = parseDate(sorted[0]?.as_of_date) || (points[0]?.date ?? new Date());
  return summarizeForecast(tierNow, biovolume, points, asOf);
}

export function modelInfoFromRows(rows: ForecastRow[]): ModelInfo | null {
  const r = rows[0];
  if (!r) return null;
  return { name: r.model_name, alias: r.model_alias, version: r.model_version, scoredAt: parseDate(r.scored_at) };
}

function summarizeForecast(tierNow: Tier, biovolume: number, points: ForecastPoint[], asOf: Date): SiteForecast {
  const crossing = points.find((p) => p.tier === 'Red');
  const alreadyRed = tierNow === 'Red';
  const willReachRed = !alreadyRed && !!crossing;
  const peakProb = points.length ? Math.max(...points.map((p) => p.prob)) : 0;

  let nextChange: string;
  if (alreadyRed) {
    nextChange = points.every((p) => p.tier === 'Red') ? 'Stays Red all week' : 'Easing later this week';
  } else if (crossing && crossing.day > 0) {
    nextChange = `Likely Red in ${crossing.day} day${crossing.day === 1 ? '' : 's'} · ${shortDate(crossing.date)}`;
  } else if (crossing) {
    nextChange = `Likely Red today · ${shortDate(crossing.date)}`;
  } else if (tierNow === 'Amber') {
    nextChange = 'Holds Amber';
  } else {
    nextChange = 'No change';
  }

  return {
    tierNow,
    biovolume,
    points,
    daysUntilRed: crossing && !alreadyRed ? crossing.day : null,
    redDate: crossing && !alreadyRed ? crossing.date : null,
    redProb: crossing && !alreadyRed ? crossing.prob : null,
    peakProb,
    willReachRed,
    nextChange,
    asOf,
  };
}

// ---- genus / conditions helpers ----

export function genusToxicity(genus: string | null): string {
  const g = (genus || '').toLowerCase();
  if (g.includes('microcystis') || g.includes('dolichospermum') || g.includes('anabaena') || g.includes('nodularia'))
    return 'potentially toxic';
  if (g.includes('aphanocapsa') || g.includes('planktolyngbya')) return 'low toxicity';
  return 'monitor toxicity';
}

export function biovolumeBand(biovol: number): string {
  const { amberBiovolume, redBiovolume } = getThresholds();
  if (biovol >= redBiovolume) return `Red band ≥ ${redBiovolume}`;
  if (biovol >= amberBiovolume) return `Amber band ${amberBiovolume}–${redBiovolume}`;
  return `Green band < ${amberBiovolume}`;
}

export function trendLabel(now: number, prev: number, unit: string, digits = 1): { dir: string; text: string } {
  const delta = now - prev;
  const dir = delta > 0.05 ? 'rising' : delta < -0.05 ? 'falling' : 'steady';
  const sign = delta >= 0 ? '+' : '−';
  return { dir, text: `${sign}${Math.abs(delta).toFixed(digits)} ${unit} over 7 days` };
}

export function pctChange(now: number, prev: number): { dir: string; text: string } {
  if (!prev) return { dir: 'steady', text: 'stable' };
  const change = ((now - prev) / prev) * 100;
  const dir = change > 1 ? 'rising' : change < -1 ? 'falling' : 'steady';
  const sign = change >= 0 ? '+' : '−';
  return { dir, text: `${dir}, ${sign}${Math.abs(change).toFixed(0)}% over 7 days` };
}

// ---- date helpers ----

export function parseDate(s: string | null | undefined): Date {
  if (!s) return new Date();
  // Accept 'YYYY-MM-DD' or ISO timestamps.
  const d = new Date(s.length <= 10 ? `${s}T00:00:00Z` : s);
  return Number.isNaN(d.getTime()) ? new Date() : d;
}

export function addDays(d: Date, n: number): Date {
  const r = new Date(d);
  r.setUTCDate(r.getUTCDate() + n);
  return r;
}

export function shortDate(d: Date): string {
  return d.toLocaleDateString(profile.locale, { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' });
}

export function weekday(d: Date): string {
  return d.toLocaleDateString(profile.locale, { weekday: 'short', day: 'numeric', timeZone: 'UTC' });
}

export function dateTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString(profile.locale, {
    day: 'numeric',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'UTC',
  });
}

export function timeOnly(iso: string): string {
  return new Date(iso).toLocaleTimeString(profile.locale, {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'UTC',
  });
}

export function minutesBetween(a: string, b: string): number | null {
  const t1 = new Date(a).getTime();
  const t2 = new Date(b).getTime();
  if (Number.isNaN(t1) || Number.isNaN(t2)) return null;
  return Math.round((t2 - t1) / 60000);
}

export function n(v: number | string | null | undefined): number {
  return num(v);
}
