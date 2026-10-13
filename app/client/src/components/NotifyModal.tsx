import { useEffect, useMemo, useState } from 'react';
import {
  Button,
  Checkbox,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  Textarea,
} from '@databricks/appkit-ui/react';
import { toast } from 'sonner';
import { fetchCouncilContacts, logAction, type CouncilContact, type Recipient } from '../lib/api';
import { useAppState } from '../AppState';
import { BRAND } from './tiers';

export interface NotifyContext {
  alertId: string;
  siteId: string;
  siteName: string;
  council: string;
  tierLabel: string; // e.g. "Forecast advisory · likely Red Sat 10 Oct (71%)"
  message: string; // prefilled body
  raisedAt: string | null;
  tier: string; // 'Red' | 'Amber' | 'Forecast'
}

export function NotifyModal({
  ctx,
  open,
  onOpenChange,
}: {
  ctx: NotifyContext | null;
  open: boolean;
  onOpenChange: (v: boolean) => void;
}) {
  const { reloadActions } = useAppState();
  const [contacts, setContacts] = useState<CouncilContact[]>([]);
  const [loadingContacts, setLoadingContacts] = useState(false);
  const council = ctx?.council ?? '';

  // Council contacts come from the dedicated warehouse via /api/council-contacts
  // (which may cold-start, so surface a loading state while it resolves).
  useEffect(() => {
    if (!council) {
      setContacts([]);
      return;
    }
    let active = true;
    setLoadingContacts(true);
    setContacts([]);
    fetchCouncilContacts(council)
      .then((rows) => {
        if (active) setContacts(rows);
      })
      .catch(() => {
        if (active) setContacts([]);
      })
      .finally(() => {
        if (active) setLoadingContacts(false);
      });
    return () => {
      active = false;
    };
  }, [council]);

  const recipients: Recipient[] = useMemo(
    () =>
      contacts.map((c) => ({
        name: c.contact_name,
        role: c.contact_role,
        org: c.council_name,
        phone: c.contact_phone,
      })),
    [contacts],
  );

  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [message, setMessage] = useState('');
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (ctx) setMessage(ctx.message);
  }, [ctx]);
  useEffect(() => {
    const init: Record<string, boolean> = {};
    recipients.forEach((r) => (init[r.name] = true));
    setSelected(init);
  }, [recipients]);

  if (!ctx) return null;

  const chosen = recipients.filter((r) => selected[r.name]);

  const onSend = async () => {
    setSaving(true);
    try {
      const { savedMs } = await logAction({
        alert_id: ctx.alertId,
        site_id: ctx.siteId,
        site_name: ctx.siteName,
        tier: ctx.tier,
        action: 'council_notified',
        council_name: ctx.council,
        recipients: chosen,
        message,
        raised_at: ctx.raisedAt,
      });
      await reloadActions();
      onOpenChange(false);
      toast.success('Council notified and logged', {
        description: `Saved to Lakebase in ${savedMs} ms`,
      });
    } catch (err) {
      toast.error('Could not save the notification', {
        description: err instanceof Error ? err.message : 'Unknown error',
      });
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>Notify {ctx.council}</DialogTitle>
          <DialogDescription>{ctx.tierLabel}</DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div>
            <div className="text-sm font-medium text-slate-700 mb-2">Recipients</div>
            {loadingContacts && <p className="text-sm text-slate-500">Loading recipients…</p>}
            {!loadingContacts && recipients.length === 0 && (
              <p className="text-sm text-slate-500">No contact on file for this council.</p>
            )}
            <div className="space-y-2">
              {recipients.map((r) => (
                <label key={r.name} className="flex items-start gap-2 text-sm">
                  <Checkbox
                    checked={!!selected[r.name]}
                    onCheckedChange={(v) => setSelected((s) => ({ ...s, [r.name]: !!v }))}
                    className="mt-0.5"
                  />
                  <span>
                    <span className="font-medium text-slate-900">{r.name}</span>
                    {r.role ? ` · ${r.role}` : ''}
                    {r.org ? `, ${r.org}` : ''}
                    {r.phone ? ` · ${r.phone}` : ''}
                  </span>
                </label>
              ))}
            </div>
          </div>

          <div>
            <div className="text-sm font-medium text-slate-700 mb-2">Message</div>
            <Textarea rows={6} value={message} onChange={(e) => setMessage(e.target.value)} />
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={saving}>
            Cancel
          </Button>
          <Button
            onClick={() => void onSend()}
            disabled={saving || chosen.length === 0}
            style={{ backgroundColor: BRAND, color: 'white' }}
          >
            {saving ? 'Saving…' : 'Send and log'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
