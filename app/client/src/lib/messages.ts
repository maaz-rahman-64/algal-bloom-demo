// Council notification copy. The wording lives in config/profile.json; these
// helpers fill in the site details.
import { fillTemplate, profile } from '../../../shared/profile';
import { n, shortDate } from './domain';
import type { ProcessedSite } from './useSites';

function siteValues(site: ProcessedSite): Record<string, string> {
  return {
    siteName: site.row.site_name,
    river: site.row.river,
    tierNow: site.tierNow,
    biovolume: n(site.row.biovolume).toFixed(1),
    genus: site.row.dominant_genus || 'cyanobacteria',
  };
}

export function redAlertMessage(site: ProcessedSite): string {
  return fillTemplate(profile.messages.redAlert, siteValues(site));
}

/** `fallbackWhen` is used when the forecast has no Red crossing date. */
export function forecastRedMessage(site: ProcessedSite, fallbackWhen = ''): string {
  const when = site.forecast.redDate ? ` from ${shortDate(site.forecast.redDate)}` : fallbackWhen;
  return fillTemplate(profile.messages.forecastRed, { ...siteValues(site), when });
}

export function amberAlertMessage(site: ProcessedSite): string {
  return fillTemplate(profile.messages.amberAlert, siteValues(site));
}

export function genericAlertMessage(tier: string, siteName: string): string {
  return fillTemplate(profile.messages.generic, { tier, siteName });
}
