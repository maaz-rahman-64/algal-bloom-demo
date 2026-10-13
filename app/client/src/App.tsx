import { createBrowserRouter, RouterProvider, Outlet } from 'react-router';
import { AppStateProvider, useAppState } from './AppState';
import { Header } from './components/Header';
import { useSites, deriveAlerts } from './lib/useSites';
import { SitesOverview } from './pages/SitesOverview';
import { SiteDetail } from './pages/SiteDetail';
import { Notifications } from './pages/Notifications';
import { AskGenie } from './pages/AskGenie';

function Layout() {
  const { region, actions } = useAppState();
  const { sites } = useSites(region);
  const alerts = deriveAlerts(sites, actions);
  const awaitingCount = alerts.filter((a) => a.status === 'awaiting officer').length;

  return (
    <div className="min-h-screen bg-slate-50 flex flex-col">
      <Header awaitingCount={awaitingCount} />
      <main className="flex-1 p-4 md:p-6 max-w-[1400px] w-full mx-auto">
        <Outlet />
      </main>
    </div>
  );
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: '/', element: <SitesOverview /> },
      { path: '/sites/:siteId', element: <SiteDetail /> },
      { path: '/notifications', element: <Notifications /> },
      { path: '/ask', element: <AskGenie /> },
    ],
  },
]);

export default function App() {
  return (
    <AppStateProvider>
      <RouterProvider router={router} />
    </AppStateProvider>
  );
}
