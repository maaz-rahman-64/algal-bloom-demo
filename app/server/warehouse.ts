import { createWorkspaceClient } from '@databricks/appkit';

// Council contacts can be served from a dedicated SQL warehouse (separate from
// the analytics warehouse used for everything else). The analytics plugin only
// targets a single warehouse, so these queries run through the SDK's
// statement-execution API against COUNCIL_WAREHOUSE_ID, falling back to the
// analytics warehouse when no dedicated one is configured.
const COUNCIL_WAREHOUSE_ID = process.env.COUNCIL_WAREHOUSE_ID || process.env.DATABRICKS_WAREHOUSE_ID || '';

// Unity Catalog location of the validated tables (set by the bundle).
const IDENTIFIER = /^[A-Za-z0-9_]+$/;
const CATALOG = process.env.ALGAL_CATALOG || '';
const SCHEMA_PREFIX = process.env.ALGAL_SCHEMA_PREFIX || 'water';
if (!IDENTIFIER.test(CATALOG) || !IDENTIFIER.test(SCHEMA_PREFIX)) {
  throw new Error(`Invalid ALGAL_CATALOG / ALGAL_SCHEMA_PREFIX: '${CATALOG}' / '${SCHEMA_PREFIX}'`);
}
const VALIDATED = `${CATALOG}.${SCHEMA_PREFIX}_validated`;

export interface CouncilContact {
  council_name: string;
  contact_name: string;
  contact_role: string;
  contact_phone: string;
  contact_email: string;
}

/** An alert from the generated history that an officer has already actioned. */
export interface HistoricalAlert {
  site_id: string;
  site_name: string;
  alert_type: string;
  tier: string;
  status: string;
  council_name: string;
  message: string;
  raised_at: string;
  actioned_at: string;
  contact_name: string;
  contact_role: string;
  contact_phone: string;
}

const TERMINAL = new Set(['SUCCEEDED', 'FAILED', 'CANCELED', 'CLOSED']);

/**
 * Run a statement on the council warehouse, on behalf of the calling user when a
 * forwarded token is present, and return the rows as string-valued objects.
 * Results are returned INLINE (no external links) to satisfy the app's egress
 * constraints.
 */
async function runStatement(
  userToken: string | undefined,
  statement: string,
  parameters: Array<{ name: string; type: string; value: string }> = [],
): Promise<Array<Record<string, string>>> {
  const client = createWorkspaceClient(userToken ? { token: userToken } : undefined);

  let resp = await client.statementExecution.executeStatement({
    warehouse_id: COUNCIL_WAREHOUSE_ID,
    statement,
    parameters,
    disposition: 'INLINE',
    format: 'JSON_ARRAY',
    wait_timeout: '30s',
    on_wait_timeout: 'CONTINUE',
  });

  // The warehouse may be cold; poll until the statement reaches a terminal state.
  let guard = 0;
  while (resp.statement_id && resp.status?.state && !TERMINAL.has(resp.status.state) && guard < 40) {
    await new Promise((r) => setTimeout(r, 1500));
    resp = await client.statementExecution.getStatement({ statement_id: resp.statement_id });
    guard++;
  }

  if (resp.status?.state !== 'SUCCEEDED') {
    throw new Error(resp.status?.error?.message || `Statement ${resp.status?.state ?? 'did not complete'}`);
  }

  const cols = (resp.manifest?.schema?.columns ?? []).map((c) => c.name ?? '');
  const rows = resp.result?.data_array ?? [];
  return rows.map((r) => {
    const obj: Record<string, string> = {};
    cols.forEach((name, i) => {
      obj[name] = r[i] ?? '';
    });
    return obj;
  });
}

export async function fetchCouncilContacts(userToken: string | undefined, council: string): Promise<CouncilContact[]> {
  const rows = await runStatement(
    userToken,
    `SELECT council_name, contact_name, contact_role, contact_phone, contact_email
     FROM ${VALIDATED}.council_contacts
     WHERE council_name = :council
     ORDER BY contact_name`,
    [{ name: 'council', type: 'STRING', value: council }],
  );
  return rows.map((obj) => ({
    council_name: obj.council_name ?? '',
    contact_name: obj.contact_name ?? '',
    contact_role: obj.contact_role ?? '',
    contact_phone: obj.contact_phone ?? '',
    contact_email: obj.contact_email ?? '',
  }));
}

/**
 * Alerts from the generated alert history that were already notified or
 * acknowledged, with the council's duty contact. Used to seed the app's log, so
 * the log always matches the scenario the data was generated from.
 */
export async function fetchActionedAlerts(userToken: string | undefined): Promise<HistoricalAlert[]> {
  const rows = await runStatement(
    userToken,
    `SELECT a.site_id, s.site_name, a.alert_type, a.tier, a.status, a.council_name, a.message,
            date_format(a.raised_at, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS raised_at,
            date_format(a.actioned_at, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS actioned_at,
            c.contact_name, c.contact_role, c.contact_phone
     FROM ${VALIDATED}.alert_history_seed a
     JOIN ${VALIDATED}.sites s ON s.site_id = a.site_id
     LEFT JOIN ${VALIDATED}.council_contacts c ON c.council_name = a.council_name
     WHERE a.status IN ('council_notified', 'acknowledged') AND a.actioned_at IS NOT NULL
     ORDER BY a.actioned_at`,
  );
  return rows.map((obj) => ({
    site_id: obj.site_id ?? '',
    site_name: obj.site_name ?? '',
    alert_type: obj.alert_type ?? '',
    tier: obj.tier ?? '',
    status: obj.status ?? '',
    council_name: obj.council_name ?? '',
    message: obj.message ?? '',
    raised_at: obj.raised_at ?? '',
    actioned_at: obj.actioned_at ?? '',
    contact_name: obj.contact_name ?? '',
    contact_role: obj.contact_role ?? '',
    contact_phone: obj.contact_phone ?? '',
  }));
}
