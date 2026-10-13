// Customer-facing profile: branding, organisation name, demo persona, locale and
// message copy. Everything an organisation-specific variant changes lives in
// config/profile.json, so the app code itself stays generic.
import raw from '../config/profile.json';

export type TierName = 'Green' | 'Amber' | 'Red';

export interface Profile {
  productName: string;
  organisation: string;
  /** Path under client/public, or null for no logo. */
  logo: string | null;
  brandColour: string;
  /** BCP 47 locale used for dates. */
  locale: string;
  /** Region (valley) the app opens on; null opens on the region with the most sites. */
  defaultRegion: string | null;
  persona: { name: string; email: string; role: string };
  messages: {
    redAlert: string;
    forecastRed: string;
    amberAlert: string;
    generic: string;
  };
  tierGuidance: Record<TierName, string>;
}

export const profile: Profile = raw;

/** Replace `{name}` placeholders; `{organisation}` is always available. */
export function fillTemplate(template: string, values: Record<string, string>): string {
  const all: Record<string, string> = { organisation: profile.organisation, ...values };
  return template.replace(/\{(\w+)\}/g, (match, key: string) => all[key] ?? match);
}
