import { useEffect, useMemo } from 'react';
import { NavLink } from 'react-router';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { useAppState } from '../AppState';
import { BRAND } from './tiers';
import { profile } from '../../../shared/profile';

const navLinkClass = ({ isActive }: { isActive: boolean }) =>
  `px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
    isActive ? 'bg-blue-50 text-[var(--brand)]' : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
  }`;

function initials(name: string): string {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return '?';
  return (parts[0][0] + (parts[1]?.[0] ?? '')).toUpperCase();
}

export function Header({ awaitingCount }: { awaitingCount: number }) {
  const { region, setRegion, officer } = useAppState();
  const { data } = useAnalyticsQuery('valleys', {});
  const valleys = useMemo(() => (data ?? []) as Array<{ valley: string; site_count: number }>, [data]);
  const officerName = officer?.name || profile.persona.name;
  const officerRole = officer?.role || profile.persona.role;

  // When no region is stored (or it isn't in this dataset), open on the
  // profile's default region, else the region with the most sites.
  useEffect(() => {
    if (valleys.length === 0 || valleys.some((v) => v.valley === region)) return;
    const preferred = valleys.find((v) => v.valley === profile.defaultRegion);
    const largest = valleys.reduce((a, b) => (Number(b.site_count) > Number(a.site_count) ? b : a));
    setRegion((preferred ?? largest).valley);
  }, [valleys, region, setRegion]);

  return (
    <header className="border-b bg-white px-4 md:px-6 py-2.5 flex items-center gap-4 flex-wrap">
      <div className="flex items-center gap-4">
        {profile.logo && (
          <>
            <img src={profile.logo} alt={profile.organisation} className="h-7 w-auto" />
            <div className="h-6 w-px bg-slate-200" />
          </>
        )}
        <span className="text-lg font-semibold" style={{ color: BRAND }}>
          {profile.productName}
        </span>
        <span
          className="rounded-full border border-amber-200 bg-amber-50 px-2 py-0.5 text-xs font-medium text-amber-700"
          title="This app runs on synthetic demonstration data, not live monitoring feeds."
        >
          Simulated data
        </span>
      </div>

      <nav className="flex items-center gap-1">
        <NavLink to="/" end className={navLinkClass}>
          Sites
        </NavLink>
        <NavLink to="/notifications" className={navLinkClass}>
          <span className="inline-flex items-center gap-1.5">
            Notifications
            {awaitingCount > 0 && (
              <span className="inline-flex h-5 min-w-5 items-center justify-center rounded-full bg-red-600 px-1.5 text-xs font-semibold text-white">
                {awaitingCount}
              </span>
            )}
          </span>
        </NavLink>
        <NavLink to="/ask" className={navLinkClass}>
          Ask Genie
        </NavLink>
      </nav>

      <div className="ml-auto flex items-center gap-4">
        <div className="flex items-center gap-2 rounded-md border bg-white px-2 py-1">
          <span className="text-sm text-slate-500">Region</span>
          <Select value={region} onValueChange={setRegion}>
            <SelectTrigger className="h-7 border-0 shadow-none font-medium focus:ring-0 px-1">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {valleys.map((v) => (
                <SelectItem key={v.valley} value={v.valley}>
                  {v.valley}
                </SelectItem>
              ))}
              {/* Until regions load (or while the data is being rebuilt) show the stored region, if any.
                  Select items can't have an empty value. */}
              {valleys.length === 0 && region && <SelectItem value={region}>{region}</SelectItem>}
            </SelectContent>
          </Select>
        </div>

        <div className="flex items-center gap-2">
          <span
            className="flex h-9 w-9 items-center justify-center rounded-full text-sm font-semibold text-white"
            style={{ backgroundColor: BRAND }}
          >
            {initials(officerName)}
          </span>
          <div className="hidden sm:block text-sm leading-tight">
            <div className="font-medium text-slate-900">{officerName}</div>
            <div className="text-xs text-slate-500">{officerRole}</div>
          </div>
        </div>
      </div>
    </header>
  );
}
