import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { sql } from '@databricks/appkit-ui/js';
import {
  forecastForSite,
  forecastFromRows,
  modelInfoFromRows,
  n,
  type ForecastRow,
  type ModelInfo,
  type SignalRow,
  type SiteForecast,
  type Tier,
} from './domain';
import type { AlertAction } from './api';

export interface ProcessedSite {
  row: SignalRow;
  forecast: SiteForecast;
  tierNow: Tier;
  model: ModelInfo | null; // provenance when the forecast came from the serving table
}

export function useSites(region: string): {
  sites: ProcessedSite[];
  loading: boolean;
  error: string | null;
} {
  const signals = useAnalyticsQuery('site_signals', { valley: sql.string(region) });
  // Forecast comes from the model serving table; if it's unavailable the app
  // falls back to deriving the curve from the site's current signals.
  const forecast = useAnalyticsQuery('site_forecast', { valley: sql.string(region) });

  const rows = (signals.data ?? []) as SignalRow[];
  const fcRows = (forecast.data ?? []) as ForecastRow[];
  const bySite = new Map<string, ForecastRow[]>();
  for (const r of fcRows) {
    const list = bySite.get(r.site_id) ?? [];
    list.push(r);
    bySite.set(r.site_id, list);
  }

  const sites: ProcessedSite[] = rows.map((row) => {
    const rowsForSite = bySite.get(row.site_id);
    if (rowsForSite && rowsForSite.length > 0) {
      const fc = forecastFromRows(n(row.biovolume), rowsForSite);
      return { row, forecast: fc, tierNow: fc.tierNow, model: modelInfoFromRows(rowsForSite) };
    }
    const fc = forecastForSite(row);
    return { row, forecast: fc, tierNow: fc.tierNow, model: null };
  });

  return { sites, loading: signals.loading, error: signals.error };
}

// ---- Alerts derived from current status + forecast, reconciled with the log ----

export type AlertStatus = 'awaiting officer' | 'acknowledged' | 'council_notified';

export interface DerivedAlert {
  alertId: string;
  siteId: string;
  siteName: string;
  council: string;
  type: 'tier' | 'forecast';
  tier: string; // 'Red' | 'Amber' | 'Forecast'
  label: string; // display label of the alert kind
  raisedAt: string; // ISO
  status: AlertStatus;
  lastAction: AlertAction | null;
}

/**
 * Build the list of alerts that need (or have had) officer action:
 * - a tier alert for any site currently Red or Amber
 * - a forecast advisory for any site forecast to cross Red within 7 days
 * Each is reconciled against the persisted action log to set its status.
 */
export function deriveAlerts(sites: ProcessedSite[], actions: AlertAction[]): DerivedAlert[] {
  const latestByAlert = new Map<string, AlertAction>();
  for (const a of actions) {
    // actions are returned newest-first
    if (!latestByAlert.has(a.alert_id)) latestByAlert.set(a.alert_id, a);
  }

  const statusFor = (alertId: string): { status: AlertStatus; last: AlertAction | null } => {
    const last = latestByAlert.get(alertId) ?? null;
    if (!last) return { status: 'awaiting officer', last: null };
    if (last.action === 'council_notified') return { status: 'council_notified', last };
    return { status: 'acknowledged', last };
  };

  const out: DerivedAlert[] = [];
  for (const s of sites) {
    const raisedTier = s.row.sample_date
      ? `${s.row.sample_date}T07:05:00Z`
      : new Date().toISOString();

    if (s.tierNow === 'Red' || s.tierNow === 'Amber') {
      const alertId = `${s.row.site_id}:${s.tierNow}`;
      const { status, last } = statusFor(alertId);
      out.push({
        alertId,
        siteId: s.row.site_id,
        siteName: s.row.site_name,
        council: s.row.council_name,
        type: 'tier',
        tier: s.tierNow,
        label: `${s.tierNow} alert`,
        raisedAt: raisedTier,
        status,
        lastAction: last,
      });
    }

    if (s.forecast.willReachRed) {
      const alertId = `${s.row.site_id}:Forecast`;
      const { status, last } = statusFor(alertId);
      const raised = s.forecast.asOf ? isoAtMorning(s.forecast.asOf) : new Date().toISOString();
      out.push({
        alertId,
        siteId: s.row.site_id,
        siteName: s.row.site_name,
        council: s.row.council_name,
        type: 'forecast',
        tier: 'Forecast',
        label: 'Forecast advisory',
        raisedAt: raised,
        status,
        lastAction: last,
      });
    }
  }

  // Include historical logged actions whose alerts are no longer live.
  const liveIds = new Set(out.map((a) => a.alertId));
  for (const a of actions) {
    if (liveIds.has(a.alert_id)) continue;
    liveIds.add(a.alert_id);
    out.push({
      alertId: a.alert_id,
      siteId: a.site_id,
      siteName: a.site_name,
      council: a.council_name ?? '',
      type: a.tier === 'Forecast' ? 'forecast' : 'tier',
      tier: a.tier,
      label: a.tier === 'Forecast' ? 'Forecast advisory' : `${a.tier} alert`,
      raisedAt: a.raised_at ?? a.created_at,
      status: a.action === 'council_notified' ? 'council_notified' : 'acknowledged',
      lastAction: a,
    });
  }

  out.sort((x, y) => new Date(y.raisedAt).getTime() - new Date(x.raisedAt).getTime());
  return out;
}

function isoAtMorning(d: Date): string {
  const r = new Date(d);
  r.setUTCHours(5, 30, 0, 0);
  return r.toISOString();
}
