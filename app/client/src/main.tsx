import {
  ResourceStatusIndicator,
  ResourceStatusProvider,
} from '@databricks/appkit-ui/react';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import App from './App.tsx';
import { ErrorBoundary } from './ErrorBoundary.tsx';
import { profile } from '../../shared/profile';

// Branding comes from config/profile.json: the brand colour is exposed as the
// --brand CSS variable (used as text-[var(--brand)] etc.) and drives the title.
document.documentElement.style.setProperty('--brand', profile.brandColour);
document.title =
  profile.organisation && profile.organisation !== profile.productName
    ? `${profile.productName} · ${profile.organisation}`
    : profile.productName;

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ErrorBoundary>
      {/*
       * Surfaces resource readiness (e.g. SQL warehouse cold-starts) as a
       * single sonner toast across the whole tree. Both are no-ops when
       * nothing's pending; remove them to render the aggregate yourself
       * via useResourceStatus(). Apps that already mount their own
       * <Toaster /> can swap the indicator for useResourceStatusToaster().
       */}
      <ResourceStatusProvider>
        <ResourceStatusIndicator position="bottom-right" richColors />
        <App />
      </ResourceStatusProvider>
    </ErrorBoundary>
  </StrictMode>
);
