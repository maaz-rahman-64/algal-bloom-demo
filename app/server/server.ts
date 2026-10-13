import { createApp, server, analytics, lakebase, genie } from '@databricks/appkit';
import { z } from 'zod';
import { ensureSchema, listActions, insertAction, seedActionsIfEmpty } from './store';
import { fetchActionedAlerts, fetchCouncilContacts } from './warehouse';
import { profile } from '../shared/profile';

// Alert-tier thresholds are bundle variables shared with the pipeline and the
// model; the client gets them from /api/config.
function requiredNumber(name: string): number {
  const value = Number(process.env[name]);
  if (!process.env[name] || !Number.isFinite(value)) {
    throw new Error(`${name} must be set to a number (it is a bundle variable passed in the app config)`);
  }
  return value;
}
const thresholds = {
  amberBiovolume: requiredNumber('ALGAL_AMBER_BIOVOLUME'),
  redBiovolume: requiredNumber('ALGAL_RED_BIOVOLUME'),
  redProbability: requiredNumber('ALGAL_RED_PROBABILITY'),
};

const recipientSchema = z.object({
  name: z.string(),
  role: z.string().optional().default(''),
  org: z.string().optional().default(''),
  phone: z.string().optional().default(''),
});

const newActionSchema = z.object({
  alert_id: z.string().min(1),
  site_id: z.string().min(1),
  site_name: z.string().min(1),
  tier: z.string().min(1),
  action: z.enum(['acknowledged', 'council_notified']),
  council_name: z.string().nullable().optional(),
  recipients: z.array(recipientSchema).nullable().optional(),
  message: z.string().nullable().optional(),
  raised_at: z.string().nullable().optional(),
});

function officer(req: { header: (name: string) => string | undefined }): { email: string; name: string } {
  const email = req.header('x-forwarded-email') || req.header('x-forwarded-user') || '';
  const preferred = req.header('x-forwarded-preferred-username') || '';
  if (!email && !preferred) {
    // Demo persona when running without a forwarded identity.
    return { email: profile.persona.email, name: profile.persona.name };
  }
  const source = preferred || email;
  const local = source.split('@')[0];
  const name = local
    .split(/[._-]+/)
    .filter(Boolean)
    .map((p) => p.charAt(0).toUpperCase() + p.slice(1))
    .join(' ');
  return { email: email || source, name: name || source };
}

await createApp({
  plugins: [server(), analytics(), lakebase(), genie()],
  async onPluginsReady(appkit) {
    await ensureSchema(appkit);

    appkit.server.extend((app) => {
      // Who is signed in (used for the header and to stamp the log).
      app.get('/api/whoami', (req, res) => {
        res.json({ ...officer(req), role: profile.persona.role });
      });

      // Config the client needs to embed the Genie space as an iframe.
      app.get('/api/config', (_req, res) => {
        const host = (process.env.DATABRICKS_WORKSPACE_HOST || '').replace(/\/$/, '');
        const spaceId = process.env.DATABRICKS_GENIE_SPACE_ID || '';
        res.json({
          genieSpaceId: spaceId,
          genieEmbedUrl: host && spaceId ? `${host}/embed/genie/rooms/${spaceId}` : '',
          workspaceHost: host,
          thresholds,
        });
      });

      // Council duty-officer contacts for the Notify modal. Served from a
      // dedicated warehouse (not the analytics one) via the SDK, on behalf of
      // the calling user.
      app.get('/api/council-contacts', async (req, res) => {
        const council = typeof req.query.council === 'string' ? req.query.council : '';
        if (!council) {
          res.json([]);
          return;
        }
        try {
          const token = req.header('x-forwarded-access-token');
          const contacts = await fetchCouncilContacts(token, council);
          res.json(contacts);
        } catch (err) {
          console.error('GET /api/council-contacts failed', err);
          res.status(500).json({ error: 'Failed to load council contacts.' });
        }
      });

      // All logged actions, or just those for one site (?site_id=...).
      app.get('/api/actions', async (req, res) => {
        try {
          const siteId = typeof req.query.site_id === 'string' ? req.query.site_id : undefined;
          // First use: seed the log from the generated alert history (read as the
          // calling user). If the data isn't there yet, serve the empty log.
          const token = req.header('x-forwarded-access-token');
          await seedActionsIfEmpty(appkit, () => fetchActionedAlerts(token), profile.persona).catch((err) =>
            console.warn('Notification log not seeded yet:', err instanceof Error ? err.message : err),
          );
          const actions = await listActions(appkit, siteId);
          res.json(actions);
        } catch (err) {
          console.error('GET /api/actions failed', err);
          res.status(500).json({ error: 'Failed to load the notification log.' });
        }
      });

      // Log an acknowledgement or council notification. Every action is persisted.
      app.post('/api/actions', async (req, res) => {
        const parsed = newActionSchema.safeParse(req.body);
        if (!parsed.success) {
          res.status(400).json({ error: 'Invalid action payload', details: parsed.error.flatten() });
          return;
        }
        try {
          const who = officer(req);
          const started = Date.now();
          const row = await insertAction(appkit, {
            ...parsed.data,
            recipients: parsed.data.recipients ?? null,
            actor_email: who.email,
            actor_name: who.name,
          });
          res.status(201).json({ action: row, savedMs: Date.now() - started });
        } catch (err) {
          console.error('POST /api/actions failed', err);
          res.status(500).json({ error: 'Failed to save the action to Lakebase.' });
        }
      });
    });
  },
}).catch(console.error);
;
