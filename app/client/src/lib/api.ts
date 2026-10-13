// Client helpers for the app's own Express API (identity, config, and the
// persistent notification/acknowledgement log stored in Lakebase).
import type { Thresholds } from './domain';

export interface Recipient {
  name: string;
  role?: string;
  org?: string;
  phone?: string;
}

export interface AlertAction {
  id: number;
  alert_id: string;
  site_id: string;
  site_name: string;
  tier: string;
  action: 'acknowledged' | 'council_notified';
  council_name: string | null;
  recipients: Recipient[] | null;
  message: string | null;
  actor_email: string;
  actor_name: string;
  raised_at: string | null;
  created_at: string;
}

export interface Officer {
  email: string;
  name: string;
  role: string;
}

export interface AppConfig {
  genieSpaceId: string;
  genieEmbedUrl: string;
  workspaceHost: string;
  thresholds: Thresholds;
}

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
  return (await res.json()) as T;
}

export interface CouncilContact {
  council_name: string;
  contact_name: string;
  contact_role: string;
  contact_phone: string;
  contact_email: string;
}

export const fetchCouncilContacts = (council: string) =>
  getJson<CouncilContact[]>(`/api/council-contacts?council=${encodeURIComponent(council)}`);

export const fetchOfficer = () => getJson<Officer>('/api/whoami');
export const fetchConfig = () => getJson<AppConfig>('/api/config');
export const fetchActions = (siteId?: string) =>
  getJson<AlertAction[]>(siteId ? `/api/actions?site_id=${encodeURIComponent(siteId)}` : '/api/actions');

export interface NewActionInput {
  alert_id: string;
  site_id: string;
  site_name: string;
  tier: string;
  action: 'acknowledged' | 'council_notified';
  council_name?: string | null;
  recipients?: Recipient[] | null;
  message?: string | null;
  raised_at?: string | null;
}

export async function logAction(input: NewActionInput): Promise<{ action: AlertAction; savedMs: number }> {
  const res = await fetch('/api/actions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  });
  if (!res.ok) {
    const body = (await res.json().catch(() => ({}))) as { error?: string };
    throw new Error(body.error || `Save failed: ${res.status}`);
  }
  return (await res.json()) as { action: AlertAction; savedMs: number };
}
