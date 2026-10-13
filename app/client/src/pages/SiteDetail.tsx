import { useMemo, useState } from 'react';
import { Link, useParams } from 'react-router';
import { Button, Card, CardContent, Skeleton } from '@databricks/appkit-ui/react';
import { toast } from 'sonner';
import { useAppState } from '../AppState';
import { useSites, type ProcessedSite } from '../lib/useSites';
import { TierBadge, ForecastChips, BRAND } from '../components/tiers';
import { ForecastChart } from '../components/ForecastChart';
import { NotifyModal, type NotifyContext } from '../components/NotifyModal';
import {
  biovolumeBand,
  genusToxicity,
  trendLabel,
  pctChange,
  shortDate,
  dateTime,
  timeOnly,
  minutesBetween,
  n,
  type SiteForecast,
} from '../lib/domain';
import { logAction, type AlertAction } from '../lib/api';
import { forecastRedMessage, redAlertMessage } from '../lib/messages';
import { profile } from '../../../shared/profile';

function buildMessage(site: ProcessedSite): string {
  if (site.tierNow === 'Red') return redAlertMessage(site);
  return forecastRedMessage(site, ' within the next 7 days');
}

function primaryAlert(site: ProcessedSite): { alertId: string; tier: string; raisedAt: string; label: string } {
  const f = site.forecast;
  if (f.willReachRed) {
    return {
      alertId: `${site.row.site_id}:Forecast`,
      tier: 'Forecast',
      raisedAt: (() => {
        const d = new Date(f.asOf);
        d.setUTCHours(5, 30, 0, 0);
        return d.toISOString();
      })(),
      label: `Forecast advisory · ${site.row.site_name} · likely Red ${f.redDate ? shortDate(f.redDate) : ''}${
        f.redProb != null ? ` (${Math.round(f.redProb * 100)}%)` : ''
      }`,
    };
  }
  return {
    alertId: `${site.row.site_id}:${site.tierNow}`,
    tier: site.tierNow,
    raisedAt: site.row.sample_date ? `${site.row.sample_date}T07:05:00Z` : new Date().toISOString(),
    label: `${site.tierNow} alert · ${site.row.site_name}`,
  };
}

export function SiteDetail() {
  const { siteId } = useParams();
  const { region, actions, reloadActions } = useAppState();
  const { sites, loading, error } = useSites(region);
  const [notifyOpen, setNotifyOpen] = useState(false);
  const [acking, setAcking] = useState(false);

  const site = sites.find((s) => s.row.site_id === siteId);
  const siteActions = useMemo(
    () => actions.filter((a) => a.site_id === siteId),
    [actions, siteId],
  );

  if (loading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-10 w-96" />
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <Skeleton className="h-96 lg:col-span-2" />
          <Skeleton className="h-96" />
        </div>
      </div>
    );
  }
  if (error) {
    return <div className="rounded-md border border-red-200 bg-red-50 p-4 text-sm text-red-700">{error}</div>;
  }
  if (!site) {
    return (
      <div className="text-sm text-slate-500">
        Site not found in {region}. <Link to="/" className="text-[var(--brand)] underline">Back to sites</Link>
      </div>
    );
  }

  const alert = primaryAlert(site);
  const ctx: NotifyContext = {
    alertId: alert.alertId,
    siteId: site.row.site_id,
    siteName: site.row.site_name,
    council: site.row.council_name,
    tier: alert.tier,
    tierLabel: alert.label,
    raisedAt: alert.raisedAt,
    message: buildMessage(site),
  };

  const onAcknowledge = async () => {
    setAcking(true);
    try {
      await logAction({
        alert_id: alert.alertId,
        site_id: site.row.site_id,
        site_name: site.row.site_name,
        tier: alert.tier,
        action: 'acknowledged',
        council_name: site.row.council_name,
        message: `Forecast acknowledged by officer. ${site.forecast.nextChange}.`,
        raised_at: alert.raisedAt,
      });
      await reloadActions();
      toast.success('Acknowledged and logged', { description: 'Saved to Lakebase' });
    } catch (err) {
      toast.error('Could not save', { description: err instanceof Error ? err.message : 'Unknown error' });
    } finally {
      setAcking(false);
    }
  };

  return (
    <div className="space-y-5">
      <div>
        <div className="text-sm text-slate-500">
          <Link to="/" className="text-[var(--brand)] hover:underline">
            Sites
          </Link>{' '}
          / {site.row.site_name}
        </div>
        <div className="mt-1 flex flex-wrap items-start justify-between gap-3">
          <div>
            <h1 className="text-2xl font-bold text-slate-900">
              {site.row.river ? `${site.row.river} at ${site.row.site_name}` : site.row.site_name}
            </h1>
            <p className="text-sm text-slate-500">
              Site {site.row.site_id} · {site.row.site_type} · {site.row.river} · Council: {site.row.council_name}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Button variant="outline" onClick={() => void onAcknowledge()} disabled={acking}>
              {acking ? 'Saving…' : 'Acknowledge forecast'}
            </Button>
            <Button onClick={() => setNotifyOpen(true)} style={{ backgroundColor: BRAND, color: 'white' }}>
              Notify council
            </Button>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <ForecastCard site={site} className="lg:col-span-2" />
        <ConditionsCard site={site} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          <ActivityCard site={site} actions={siteActions} />
        </div>
        <LevelsCard />
      </div>

      <NotifyModal ctx={ctx} open={notifyOpen} onOpenChange={setNotifyOpen} />
    </div>
  );
}

function ForecastCard({ site, className = '' }: { site: ProcessedSite; className?: string }) {
  const f: SiteForecast = site.forecast;
  const big = f.daysUntilRed != null ? `${f.daysUntilRed}` : site.tierNow === 'Red' ? 'Red' : '—';
  const bigUnit = f.daysUntilRed != null ? (f.daysUntilRed === 1 ? 'day' : 'days') : '';
  return (
    <Card className={`shadow-none ${className}`}>
      <CardContent className="p-5">
        <div className="flex items-start justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">Chance of a Red alert, next 7 days</h2>
            <p className="mt-1 text-sm text-slate-500">Red is called when forecast probability ≥ 60%</p>
          </div>
          <TierBadge tier={site.tierNow} />
        </div>

        <div className="mt-4 flex items-baseline gap-3 flex-wrap">
          <span className="text-5xl font-bold text-slate-900">
            {big}
            {bigUnit && <span className="text-2xl font-semibold"> {bigUnit}</span>}
          </span>
          <span className="text-sm text-slate-500">
            {f.willReachRed && f.redDate ? (
              <>
                until likely Red · <span className="font-semibold text-slate-700">{shortDate(f.redDate)}</span>
                {f.redProb != null ? ` (${Math.round(f.redProb * 100)}%)` : ''}
              </>
            ) : site.tierNow === 'Red' ? (
              `now · peak ${Math.round(f.peakProb * 100)}% chance of Red this week`
            ) : site.tierNow === 'Amber' ? (
              `Holds Amber · peak ${Math.round(f.peakProb * 100)}% chance of Red`
            ) : (
              `Low risk · peak ${Math.round(f.peakProb * 100)}% chance of Red`
            )}
          </span>
        </div>

        <div className="mt-3">
          <ForecastChart points={f.points} />
        </div>

        <div className="mt-3 flex flex-wrap items-center justify-between gap-3 border-t pt-3">
          <div className="flex items-center gap-2">
            <span className="text-sm text-slate-500">Forecast tier by day</span>
            <ForecastChips points={f.points} size="sm" />
          </div>
          <span className="text-xs text-slate-400">
            {site.model
              ? `Model ${site.model.name}@${site.model.alias} (${site.model.version}) · scored ${shortDate(
                  site.model.scoredAt,
                )} ${timeOnly(site.model.scoredAt.toISOString())}`
              : `Forecast derived from current signals · scored ${shortDate(f.asOf)} 05:30`}
          </span>
        </div>
      </CardContent>
    </Card>
  );
}

function Stat({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div>
      <div className="text-sm text-slate-500">{label}</div>
      <div className="mt-0.5 text-xl font-bold text-slate-900">{value}</div>
      {sub && <div className="text-xs text-slate-500">{sub}</div>}
    </div>
  );
}

function ConditionsCard({ site }: { site: ProcessedSite }) {
  const r = site.row;
  const biovol = n(r.biovolume);
  const temp = trendLabel(n(r.temp_now), n(r.temp_prev), '°C');
  const flow = pctChange(n(r.flow_now), n(r.flow_prev));
  const phyco = trendLabel(n(r.phyco_now), n(r.phyco_prev), 'RFU');
  const qa = n(r.qa_excluded_7d);
  return (
    <Card className="shadow-none">
      <CardContent className="p-5">
        <h2 className="text-lg font-semibold text-slate-900">Current conditions</h2>
        <p className="mt-1 text-sm text-slate-500">
          Latest validated data · lab sample {r.sample_date ? shortDate(new Date(`${r.sample_date}T00:00:00Z`)) : '—'}
        </p>
        <div className="mt-4 grid grid-cols-2 gap-x-6 gap-y-5">
          <Stat label="Cyanobacteria biovolume" value={`${biovol.toFixed(1)} mm³/L`} sub={biovolumeBand(biovol)} />
          <Stat
            label="Dominant genus"
            value={r.dominant_genus || '—'}
            sub={genusToxicity(r.dominant_genus)}
          />
          <Stat label="Water temperature" value={`${n(r.temp_now).toFixed(1)} °C`} sub={temp.text} />
          <Stat label="River flow" value={`${Math.round(n(r.flow_now)).toLocaleString()} ML/d`} sub={flow.text} />
          <Stat label="Phycocyanin (sonde)" value={phyco.dir} sub={phyco.text} />
          <Stat
            label="Sensor QA"
            value={qa > 0 ? 'Flagged' : 'OK'}
            sub={qa > 0 ? `${qa} readings excluded` : 'no readings excluded'}
          />
        </div>
      </CardContent>
    </Card>
  );
}

interface FeedItem {
  ts: string;
  primary: React.ReactNode;
  badge?: string;
}

function ActivityCard({ site, actions }: { site: ProcessedSite; actions: AlertAction[] }) {
  const r = site.row;
  const items: FeedItem[] = [];

  if (site.forecast.willReachRed && site.forecast.redDate) {
    const d = new Date(site.forecast.asOf);
    d.setUTCHours(5, 30, 0, 0);
    const acted = actions.some((a) => a.alert_id === `${r.site_id}:Forecast`);
    items.push({
      ts: d.toISOString(),
      primary: (
        <span>
          <span className="font-medium">Forecast advisory:</span> likely Red on {shortDate(site.forecast.redDate)}
        </span>
      ),
      badge: acted ? undefined : 'awaiting officer',
    });
  }

  for (const a of actions) {
    if (a.action === 'council_notified') {
      const mins = a.raised_at ? minutesBetween(a.raised_at, a.created_at) : null;
      items.push({
        ts: a.created_at,
        primary: (
          <span>
            {a.council_name} notified of <span className="font-semibold">{a.tier}</span> by {a.actor_name}
          </span>
        ),
        badge: mins != null ? `${mins} min after alert` : undefined,
      });
    } else {
      items.push({
        ts: a.created_at,
        primary: (
          <span>
            <span className="font-semibold">{a.tier}</span> acknowledged by {a.actor_name}
          </span>
        ),
      });
    }
  }

  if ((site.tierNow === 'Red' || site.tierNow === 'Amber') && r.sample_date) {
    items.push({
      ts: `${r.sample_date}T07:05:00Z`,
      primary: (
        <span>
          {site.tierNow} alert raised from validated lab result ({n(r.biovolume).toFixed(1)} mm³/L)
        </span>
      ),
    });
    const collected = new Date(`${r.sample_date}T00:00:00Z`);
    collected.setUTCDate(collected.getUTCDate() - 1);
    collected.setUTCHours(9, 10, 0, 0);
    items.push({ ts: collected.toISOString(), primary: <span>Lab sample collected, sent to the lab</span> });
  }

  items.sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime());

  return (
    <Card className="shadow-none">
      <CardContent className="p-5">
        <h2 className="text-lg font-semibold text-slate-900 mb-3">Activity at this site</h2>
        <div className="divide-y">
          {items.map((it, i) => (
            <div key={i} className="flex items-start gap-4 py-3">
              <div className="w-28 shrink-0 text-xs text-slate-500">{dateTime(it.ts)}</div>
              <div className="flex-1 text-sm text-slate-800">
                {it.primary}
                {it.badge && (
                  <span className="ml-2 rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-500 align-middle">
                    {it.badge}
                  </span>
                )}
              </div>
            </div>
          ))}
          {items.length === 0 && <div className="py-3 text-sm text-slate-500">No activity recorded.</div>}
        </div>
      </CardContent>
    </Card>
  );
}

function LevelsCard() {
  return (
    <Card className="shadow-none">
      <CardContent className="p-5">
        <h2 className="text-lg font-semibold text-slate-900 mb-3">What the alert levels mean</h2>
        <ul className="space-y-3 text-sm text-slate-700">
          <li className="flex gap-2">
            <TierBadge tier="Green" />
            <span>{profile.tierGuidance.Green}</span>
          </li>
          <li className="flex gap-2">
            <TierBadge tier="Amber" />
            <span>{profile.tierGuidance.Amber}</span>
          </li>
          <li className="flex gap-2">
            <TierBadge tier="Red" />
            <span>{profile.tierGuidance.Red}</span>
          </li>
        </ul>
        <p className="mt-4 text-xs text-slate-400">Illustrative thresholds for this demo.</p>
      </CardContent>
    </Card>
  );
}
