import type { QueryResult, QueryResultRow } from 'pg';
import type { HistoricalAlert } from './warehouse';

// Minimal structural view of the AppKit instance that the store needs.
interface LakebaseLike {
  query<T extends QueryResultRow = QueryResultRow>(text: string, values?: unknown[]): Promise<QueryResult<T>>;
}
export interface AppkitWithLakebase {
  lakebase: LakebaseLike;
}

// Shape of a logged action (acknowledge or council notification).
export interface AlertAction {
  id: number;
  alert_id: string;
  site_id: string;
  site_name: string;
  tier: string; // 'Red' | 'Amber' | 'Green' | 'Forecast'
  action: string; // 'acknowledged' | 'council_notified'
  council_name: string | null;
  recipients: unknown; // JSON array of recipient objects
  message: string | null;
  actor_email: string;
  actor_name: string;
  raised_at: string | null; // ISO timestamp the underlying alert was raised
  created_at: string; // ISO timestamp the action was logged
}

export interface NewAction {
  alert_id: string;
  site_id: string;
  site_name: string;
  tier: string;
  action: string;
  council_name?: string | null;
  recipients?: unknown;
  message?: string | null;
  actor_email: string;
  actor_name: string;
  raised_at?: string | null;
}

const SCHEMA = 'algal';

let ready: Promise<void> | null = null;

export function ensureSchema(appkit: AppkitWithLakebase): Promise<void> {
  if (!ready) {
    ready = (async () => {
      await appkit.lakebase.query(`CREATE SCHEMA IF NOT EXISTS ${SCHEMA}`);
      await appkit.lakebase.query(`
        CREATE TABLE IF NOT EXISTS ${SCHEMA}.alert_actions (
          id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          alert_id TEXT NOT NULL,
          site_id TEXT NOT NULL,
          site_name TEXT NOT NULL,
          tier TEXT NOT NULL,
          action TEXT NOT NULL,
          council_name TEXT,
          recipients JSONB,
          message TEXT,
          actor_email TEXT NOT NULL,
          actor_name TEXT NOT NULL,
          raised_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
      await appkit.lakebase.query(
        `CREATE INDEX IF NOT EXISTS idx_alert_actions_site ON ${SCHEMA}.alert_actions (site_id, created_at DESC)`,
      );
    })();
  }
  return ready;
}

// The client keys alerts as `${site_id}:${tier}` (current tier) or
// `${site_id}:Forecast`; anything else (e.g. sensor QA) is history only.
function alertKey(a: HistoricalAlert): string {
  if (a.alert_type === 'forecast_advisory') return `${a.site_id}:Forecast`;
  if (a.alert_type === 'tier_alert') return `${a.site_id}:${a.tier}`;
  return `${a.site_id}:${a.alert_type}`;
}

let seeding: Promise<void> | null = null;

/**
 * Seed an empty log from the generated alert history (already-actioned alerts),
 * so the notification history matches the scenario the data came from. Checked
 * on every call, so clearing the table resets the demo log on the next page
 * load; concurrent calls share one in-flight check so the log is seeded once.
 */
export function seedActionsIfEmpty(
  appkit: AppkitWithLakebase,
  loadHistory: () => Promise<HistoricalAlert[]>,
  actor: { email: string; name: string },
): Promise<void> {
  if (!seeding) {
    seeding = (async () => {
      const { rows } = await appkit.lakebase.query<{ has_rows: boolean }>(
        `SELECT EXISTS (SELECT 1 FROM ${SCHEMA}.alert_actions) AS has_rows`,
      );
      if (rows[0]?.has_rows) return;
      const history = await loadHistory();
      if (history.length === 0) throw new Error('No actioned alerts in the alert history yet');
      for (const a of history) {
        const recipients =
          a.status === 'council_notified' && a.contact_name
            ? [{ name: a.contact_name, role: a.contact_role, org: a.council_name, phone: a.contact_phone }]
            : null;
        await appkit.lakebase.query(
          `INSERT INTO ${SCHEMA}.alert_actions
            (alert_id, site_id, site_name, tier, action, council_name, recipients, message, actor_email, actor_name, raised_at, created_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
          [
            alertKey(a),
            a.site_id,
            a.site_name,
            a.alert_type === 'forecast_advisory' ? 'Forecast' : a.tier,
            a.status,
            a.council_name || null,
            recipients == null ? null : JSON.stringify(recipients),
            a.message || null,
            actor.email,
            actor.name,
            a.raised_at || null,
            a.actioned_at,
          ],
        );
      }
    })().finally(() => {
      seeding = null;
    });
  }
  return seeding;
}

export async function listActions(appkit: AppkitWithLakebase, siteId?: string): Promise<AlertAction[]> {
  const sql = siteId
    ? `SELECT * FROM ${SCHEMA}.alert_actions WHERE site_id = $1 ORDER BY created_at DESC`
    : `SELECT * FROM ${SCHEMA}.alert_actions ORDER BY created_at DESC`;
  const params = siteId ? [siteId] : [];
  const { rows } = await appkit.lakebase.query<AlertAction>(sql, params);
  return rows;
}

export async function insertAction(appkit: AppkitWithLakebase, a: NewAction): Promise<AlertAction> {
  const { rows } = await appkit.lakebase.query<AlertAction>(
    `INSERT INTO ${SCHEMA}.alert_actions
      (alert_id, site_id, site_name, tier, action, council_name, recipients, message, actor_email, actor_name, raised_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
     RETURNING *`,
    [
      a.alert_id,
      a.site_id,
      a.site_name,
      a.tier,
      a.action,
      a.council_name ?? null,
      a.recipients == null ? null : JSON.stringify(a.recipients),
      a.message ?? null,
      a.actor_email,
      a.actor_name,
      a.raised_at ?? null,
    ],
  );
  return rows[0];
}
