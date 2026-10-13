import { useMemo, useState } from 'react';
import { Link } from 'react-router';
import { Button, Card, CardContent, Skeleton } from '@databricks/appkit-ui/react';
import { useAppState } from '../AppState';
import { useSites, deriveAlerts, type DerivedAlert, type ProcessedSite } from '../lib/useSites';
import { NotifyModal, type NotifyContext } from '../components/NotifyModal';
import { BRAND, TierBadge } from '../components/tiers';
import { dateTime, minutesBetween, shortDate, timeOnly } from '../lib/domain';
import { amberAlertMessage, forecastRedMessage, genericAlertMessage, redAlertMessage } from '../lib/messages';

function ForecastChip() {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-amber-200 bg-amber-50 px-2.5 py-0.5 text-sm font-medium text-amber-900">
      ▲ Forecast
    </span>
  );
}

function buildMessageForAlert(alert: DerivedAlert, site: ProcessedSite | undefined): string {
  if (site) {
    if (alert.type === 'forecast' && site.forecast.redDate) return forecastRedMessage(site);
    if (site.tierNow === 'Red') return redAlertMessage(site);
    return amberAlertMessage(site);
  }
  return genericAlertMessage(alert.tier, alert.siteName);
}

export function Notifications() {
  const { region, actions, actionsLoading } = useAppState();
  const { sites, loading } = useSites(region);
  const [ctx, setCtx] = useState<NotifyContext | null>(null);
  const [open, setOpen] = useState(false);

  const alerts = useMemo(() => deriveAlerts(sites, actions), [sites, actions]);
  const awaiting = alerts.filter((a) => a.status === 'awaiting officer');

  const notified30d = useMemo(() => {
    // Anchor "now" to the most recent logged action so the window is
    // deterministic against the historical dataset.
    const times = actions.map((a) => new Date(a.created_at).getTime()).filter((t) => !Number.isNaN(t));
    const now = times.length ? Math.max(...times) : 0;
    const cutoff = now - 30 * 86400000;
    const councils = new Set<string>();
    let count = 0;
    for (const a of actions) {
      if (a.action !== 'council_notified' || !a.council_name) continue;
      const t = new Date(a.created_at).getTime();
      if (t >= cutoff) {
        count++;
        councils.add(a.council_name);
      }
    }
    return { count, councils: councils.size };
  }, [actions]);

  const openNotify = (alert: DerivedAlert) => {
    const site = sites.find((s) => s.row.site_id === alert.siteId);
    setCtx({
      alertId: alert.alertId,
      siteId: alert.siteId,
      siteName: alert.siteName,
      council: alert.council,
      tier: alert.tier,
      tierLabel:
        alert.type === 'forecast'
          ? `Forecast advisory · ${alert.siteName}${
              site?.forecast.redDate ? ` · likely Red ${shortDate(site.forecast.redDate)}` : ''
            }${site?.forecast.redProb != null ? ` (${Math.round(site.forecast.redProb * 100)}%)` : ''}`
          : `${alert.tier} alert · ${alert.siteName}`,
      raisedAt: alert.raisedAt,
      message: buildMessageForAlert(alert, site),
    });
    setOpen(true);
  };

  if (loading || actionsLoading) {
    return (
      <div className="space-y-4">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 max-w-2xl">
          <Skeleton className="h-28" />
          <Skeleton className="h-28" />
        </div>
        <Skeleton className="h-80 w-full" />
      </div>
    );
  }

  return (
    <div className="space-y-5">
      <h1 className="text-2xl font-bold text-slate-900">Notifications</h1>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 max-w-2xl">
        <Card className="shadow-none">
          <CardContent className="p-5">
            <div className="text-sm text-slate-500">Awaiting officer action</div>
            <div className="mt-2 text-4xl font-bold text-slate-900">{awaiting.length}</div>
            <div className="mt-2 text-sm text-slate-500">
              {awaiting.map((a) => `${a.siteName} ${a.type === 'forecast' ? 'forecast advisory' : a.tier.toLowerCase()}`).join(' · ') || 'All clear'}
            </div>
          </CardContent>
        </Card>
        <Card className="shadow-none">
          <CardContent className="p-5">
            <div className="text-sm text-slate-500">Councils notified (30 days)</div>
            <div className="mt-2 text-4xl font-bold text-slate-900">{notified30d.count}</div>
            <div className="mt-2 text-sm text-slate-500">across {notified30d.councils} councils</div>
          </CardContent>
        </Card>
      </div>

      <Card className="shadow-none">
        <CardContent className="p-5">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-slate-500 border-b">
                  <th className="pb-2 font-medium">Raised</th>
                  <th className="pb-2 font-medium">Site</th>
                  <th className="pb-2 font-medium">Alert</th>
                  <th className="pb-2 font-medium">Status</th>
                  <th className="pb-2 font-medium">Time to notify</th>
                  <th className="pb-2 font-medium text-right">Action</th>
                </tr>
              </thead>
              <tbody>
                {alerts.map((a) => (
                  <AlertRow key={a.alertId} alert={a} onNotify={() => openNotify(a)} />
                ))}
                {alerts.length === 0 && (
                  <tr>
                    <td colSpan={6} className="py-6 text-center text-slate-500">
                      No alerts in {region}.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          <p className="mt-4 text-xs text-slate-400">
            Notification log: Lakebase Postgres table{' '}
            <code className="rounded bg-slate-100 px-1 py-0.5">algal.alert_actions</code> (read and write).
          </p>
        </CardContent>
      </Card>

      <NotifyModal ctx={ctx} open={open} onOpenChange={setOpen} />
    </div>
  );
}

function AlertRow({ alert, onNotify }: { alert: DerivedAlert; onNotify: () => void }) {
  const last = alert.lastAction;
  const notified = alert.status === 'council_notified';
  const mins = notified && last?.raised_at ? minutesBetween(last.raised_at, last.created_at) : null;

  let statusEl: React.ReactNode;
  if (alert.status === 'council_notified' && last) {
    statusEl = (
      <span className="text-slate-700">
        {last.council_name} notified {timeOnly(last.created_at)}
      </span>
    );
  } else if (alert.status === 'acknowledged' && last) {
    statusEl = <span className="text-slate-600">Acknowledged by {last.actor_name}</span>;
  } else {
    statusEl = <span className="rounded bg-amber-100 px-1.5 py-0.5 text-xs font-medium text-amber-900">awaiting officer</span>;
  }

  return (
    <tr className={`border-b last:border-0 ${alert.status === 'awaiting officer' ? 'bg-amber-50/50' : ''}`}>
      <td className="py-3 pr-4 text-slate-600">{dateTime(alert.raisedAt)}</td>
      <td className="py-3 pr-4 font-semibold text-slate-900">
        <Link to={`/sites/${alert.siteId}`} className="hover:text-[var(--brand)]">
          {alert.siteName}
        </Link>
      </td>
      <td className="py-3 pr-4">
        {alert.tier === 'Forecast' ? <ForecastChip /> : <TierBadge tier={alert.tier as 'Red' | 'Amber' | 'Green'} />}
      </td>
      <td className="py-3 pr-4">{statusEl}</td>
      <td className="py-3 pr-4 text-slate-600">{mins != null ? `${mins} min` : '—'}</td>
      <td className="py-3 text-right">
        {notified ? (
          <Link to={`/sites/${alert.siteId}`} className="text-sm font-medium text-[var(--brand)] hover:underline">
            View
          </Link>
        ) : (
          <Button size="sm" onClick={onNotify} style={{ backgroundColor: BRAND, color: 'white' }}>
            Notify council
          </Button>
        )}
      </td>
    </tr>
  );
}
