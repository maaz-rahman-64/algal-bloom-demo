import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react';
import {
  fetchActions,
  fetchConfig,
  fetchOfficer,
  type AlertAction,
  type AppConfig,
  type Officer,
} from './lib/api';
import { configureThresholds } from './lib/domain';

interface AppStateValue {
  region: string;
  setRegion: (r: string) => void;
  officer: Officer | null;
  config: AppConfig | null;
  actions: AlertAction[];
  actionsLoading: boolean;
  reloadActions: () => Promise<void>;
}

const AppStateContext = createContext<AppStateValue | null>(null);

export function AppStateProvider({ children }: { children: ReactNode }) {
  // Empty until the header picks a default region from the data.
  const [region, setRegionState] = useState<string>(() => localStorage.getItem('algal:region') || '');
  const [officer, setOfficer] = useState<Officer | null>(null);
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [configError, setConfigError] = useState(false);
  const [actions, setActions] = useState<AlertAction[]>([]);
  const [actionsLoading, setActionsLoading] = useState(true);

  const reloadActions = useCallback(async () => {
    try {
      const rows = await fetchActions();
      setActions(rows);
    } catch {
      setActions([]);
    } finally {
      setActionsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchOfficer().then(setOfficer).catch(() => setOfficer(null));
    fetchConfig()
      .then((c) => {
        configureThresholds(c.thresholds);
        setConfig(c);
      })
      .catch(() => setConfigError(true));
    void reloadActions();
  }, [reloadActions]);

  const setRegion = useCallback((r: string) => {
    setRegionState(r);
    localStorage.setItem('algal:region', r);
  }, []);

  // The alert thresholds come from the server config, so nothing renders until it arrives.
  if (!config) {
    return (
      <div className="flex min-h-screen items-center justify-center text-sm text-slate-500">
        {configError ? 'Could not load the app configuration. Refresh to try again.' : 'Loading…'}
      </div>
    );
  }

  return (
    <AppStateContext.Provider
      value={{ region, setRegion, officer, config, actions, actionsLoading, reloadActions }}
    >
      {children}
    </AppStateContext.Provider>
  );
}

export function useAppState(): AppStateValue {
  const ctx = useContext(AppStateContext);
  if (!ctx) throw new Error('useAppState must be used within AppStateProvider');
  return ctx;
}
