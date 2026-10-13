import { Link } from 'react-router';
import { Card, CardContent, Skeleton } from '@databricks/appkit-ui/react';
import { AlertTriangle } from 'lucide-react';
import { useAppState } from '../AppState';
import { useSites, type ProcessedSite } from '../lib/useSites';
import { TierBadge, ForecastChips, TierLegend } from '../components/tiers';
import { RiverSchematic, SchematicLegend } from '../components/RiverSchematic';
import { shortDate, minutesBetween, timeOnly, weekday, n } from '../lib/domain';
import type { AlertAction } from '../lib/api';

function KpiCard({
  label,
  value,
  badge,
  sub,
}: {
  label: string;
  value: string | number;
  badge?: React.ReactNode;
  sub?: string;
}) {
  return (
    <Card className="shadow-none">
      <CardContent className="p-5">
        <div className="text-sm text-slate-500">{label}</div>
        <div className="mt-2 flex items-center gap-3">
          <span className="text-4xl font-bold text-slate-900">{value}</span>
          {badge}
        </div>
        <div className="mt-2 text-sm text-slate-500 min-h-5">{sub}</div>
      </CardContent>
    </Card>
  );
}

function latestNotify(actions: AlertAction[], siteId: string, tier: string): AlertAction | null {
  return (
    actions.find(
      (a) => a.site_id === siteId && a.alert_id === `${siteId}:${tier}` && a.action === 'council_notified',
    ) ?? null
  );
}

export function SitesOverview() {
  const { region, actions } = useAppState();
  const { sites, loading, error } = useSites(region);

  if (error) {
    return (
      <div className="rounded-md border border-red-200 bg-red-50 p-4 text-sm text-red-700">
        Couldn’t load site data: {error}
      </div>
    );
  }

  if (loading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-20 w-full" />
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          {[0, 1, 2, 3].map((i) => (
            <Skeleton key={i} className="h-28 w-full" />
          ))}
        </div>
        <Skeleton className="h-64 w-full" />
        <Skeleton className="h-80 w-full" />
      </div>
    );
  }

  const red = sites.filter((s) => s.tierNow === 'Red');
  const amber = sites.filter((s) => s.tierNow === 'Amber');
  const willRed = sites.filter((s) => s.forecast.willReachRed);

  const topAlert = red[0] ?? amber[0] ?? null;
  const asOf = sites[0]?.forecast.asOf;
  const endDate = sites[0]?.forecast.points.at(-1)?.date;

  const namesWithRedDate = willRed
    .map((s) => `${s.row.site_name}${s.forecast.redDate ? `, ${shortDate(s.forecast.redDate)}` : ''}`)
    .join(' · ');

  return (
    <div className="space-y-5">
      {topAlert && <AlertBanner site={topAlert} notify={latestNotify(actions, topAlert.row.site_id, topAlert.tierNow)} />}

      {/* KPI cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <KpiCard label="Sites monitored" value={sites.length} sub={`${region} region`} />
        <KpiCard
          label="Red now"
          value={red.length}
          badge={red.length > 0 ? <TierBadge tier="Red" /> : undefined}
          sub={red.map((s) => s.row.site_name).join(' · ') || 'None'}
        />
        <KpiCard
          label="Amber now"
          value={amber.length}
          badge={amber.length > 0 ? <TierBadge tier="Amber" /> : undefined}
          sub={amber.map((s) => s.row.site_name).join(' · ') || 'None'}
        />
        <KpiCard
          label="Forecast to reach Red within 7 days"
          value={willRed.length}
          sub={namesWithRedDate || 'None'}
        />
      </div>

      {/* River schematic */}
      <Card className="shadow-none">
        <CardContent className="p-5">
          <div className="flex flex-wrap items-center justify-between gap-3 mb-3">
            <div className="flex items-baseline gap-2">
              <h2 className="text-base font-semibold text-slate-900">River schematic</h2>
              <span className="text-sm text-slate-500">Upstream → downstream, not to scale</span>
            </div>
            <SchematicLegend />
          </div>
          <RiverSchematic sites={sites} />
        </CardContent>
      </Card>

      {/* Sites table */}
      <Card className="shadow-none">
        <CardContent className="p-5">
          <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
            <h2 className="text-base font-semibold text-slate-900">
              Sites · current status and 7-day forecast
            </h2>
            <TierLegend />
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-slate-500 border-b">
                  <th className="pb-2 font-medium">Site</th>
                  <th className="pb-2 font-medium">Now</th>
                  <th className="pb-2 font-medium">
                    Forecast {asOf ? weekday(asOf) : ''} → {endDate ? weekday(endDate) : ''}
                  </th>
                  <th className="pb-2 font-medium">Next change</th>
                  <th className="pb-2 font-medium text-right">Last sample</th>
                </tr>
              </thead>
              <tbody>
                {sites.map((s) => (
                  <SiteRow key={s.row.site_id} site={s} />
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function AlertBanner({ site, notify }: { site: ProcessedSite; notify: AlertAction | null }) {
  const isRed = site.tierNow === 'Red';
  const tone = isRed ? 'border-red-200 bg-red-50 text-red-800' : 'border-amber-200 bg-amber-50 text-amber-900';
  const raised = site.row.sample_date ? `${site.row.sample_date}T07:05:00Z` : new Date().toISOString();
  let notifyText = 'Awaiting officer action.';
  if (notify) {
    const mins = minutesBetween(raised, notify.created_at);
    notifyText = `${notify.council_name} notified at ${timeOnly(notify.created_at)}${
      mins != null ? ` (${mins} min)` : ''
    }.`;
  }
  return (
    <div className={`flex flex-wrap items-center justify-between gap-3 rounded-lg border p-4 ${tone}`}>
      <div className="flex items-center gap-3">
        {isRed ? (
          <span className="inline-block h-3.5 w-3.5 rounded-full bg-red-600" />
        ) : (
          <AlertTriangle className="h-4 w-4 text-amber-600" />
        )}
        <p className="text-sm">
          <span className="font-semibold">
            {site.tierNow} alert — {site.row.site_name}
            {site.row.site_type ? ` (${site.row.site_type.toLowerCase()})` : ''}.
          </span>{' '}
          Raised {timeOnly(raised)} from the validated lab result. {notifyText}
        </p>
      </div>
      <Link
        to={`/sites/${site.row.site_id}`}
        className="rounded-md border border-current/30 bg-white/70 px-3 py-1.5 text-sm font-medium hover:bg-white"
      >
        View alert
      </Link>
    </div>
  );
}

function SiteRow({ site }: { site: ProcessedSite }) {
  const s = site;
  const qa = n(s.row.qa_excluded_7d);
  const highlight = s.forecast.willReachRed;
  return (
    <tr className={`border-b last:border-0 ${highlight ? 'bg-amber-50/60' : ''}`}>
      <td className="py-3 pr-4">
        <Link to={`/sites/${s.row.site_id}`} className="font-semibold text-slate-900 hover:text-[var(--brand)]">
          {s.row.site_name}
        </Link>
        <div className="text-xs text-slate-500">
          {s.row.site_type}
          {s.row.river ? ` · ${s.row.river}` : ''}
        </div>
      </td>
      <td className="py-3 pr-4">
        <div className="flex items-center gap-2">
          <TierBadge tier={s.tierNow} />
          {qa > 0 && (
            <span className="rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-500">
              QA · {qa} excluded
            </span>
          )}
        </div>
      </td>
      <td className="py-3 pr-4">
        <ForecastChips points={s.forecast.points} size="sm" />
      </td>
      <td className={`py-3 pr-4 ${highlight ? 'font-semibold text-red-700' : 'text-slate-600'}`}>
        {s.forecast.nextChange}
      </td>
      <td className="py-3 text-right text-slate-500">
        {s.row.sample_date ? shortDate(new Date(`${s.row.sample_date}T00:00:00Z`)) : '—'}
      </td>
    </tr>
  );
}
