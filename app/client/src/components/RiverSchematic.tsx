import { useNavigate } from 'react-router';
import type { ProcessedSite } from '../lib/useSites';
import type { Tier } from '../lib/domain';
import { n } from '../lib/domain';

const TIER_FILL: Record<Tier, string> = { Green: '#16a34a', Amber: '#f59e0b', Red: '#dc2626' };

function isStorage(t: string): boolean {
  const s = (t || '').toLowerCase();
  return s.includes('reservoir') || s.includes('storage') || s.includes('dam') || s.includes('lake');
}

interface Node {
  site: ProcessedSite;
  x: number;
  y: number;
}

export function RiverSchematic({ sites }: { sites: ProcessedSite[] }) {
  const navigate = useNavigate();
  if (sites.length === 0) return null;

  // Order upstream -> downstream without assuming a flow direction: storages sit
  // at the head of the system, so river sites are ordered by distance from the
  // storages' centroid (nearest first). With no storages, keep the given order.
  const storages = sites.filter((s) => isStorage(s.row.site_type));
  const head =
    storages.length > 0
      ? {
          lat: storages.reduce((sum, s) => sum + n(s.row.latitude), 0) / storages.length,
          lon: storages.reduce((sum, s) => sum + n(s.row.longitude), 0) / storages.length,
        }
      : null;
  const distFromHead = (s: ProcessedSite) =>
    head ? Math.hypot(n(s.row.latitude) - head.lat, n(s.row.longitude) - head.lon) : 0;
  const byDistance = (a: ProcessedSite, b: ProcessedSite) => distFromHead(a) - distFromHead(b);
  // Storages stack top-to-bottom by longitude (display order only).
  const byLon = (a: ProcessedSite, b: ProcessedSite) => n(b.row.longitude) - n(a.row.longitude);
  storages.sort(byLon);
  const river = sites.filter((s) => !isStorage(s.row.site_type)).sort(byDistance);

  const W = 1000;
  const H = 260;
  const riverY = 150;
  const confluenceX = 190;
  const startX = 280;
  const endX = 950;

  const riverNodes: Node[] = river.map((site, i) => ({
    site,
    x: river.length === 1 ? (startX + endX) / 2 : startX + (i * (endX - startX)) / (river.length - 1),
    y: riverY,
  }));

  const storageNodes: Node[] = storages.map((site, i) => ({
    site,
    x: 90,
    y: storages.length === 1 ? riverY : 70 + (i * 160) / (storages.length - 1),
  }));

  const nodeSize = 26;

  const onOpen = (id: string) => {
    void navigate(`/sites/${id}`);
  };

  return (
    <div className="w-full overflow-x-auto">
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full min-w-[720px]" role="img" aria-label="River schematic">
        {/* main river line */}
        <line x1={confluenceX} y1={riverY} x2={endX + 20} y2={riverY} stroke="#5bb8dd" strokeWidth={6} />

        {/* storage feeder curves */}
        {storageNodes.map((sn) => (
          <path
            key={`feed-${sn.site.row.site_id}`}
            d={`M ${sn.x + 20} ${sn.y} C ${confluenceX - 60} ${sn.y}, ${confluenceX - 40} ${riverY}, ${confluenceX} ${riverY}`}
            fill="none"
            stroke="#5bb8dd"
            strokeWidth={5}
          />
        ))}

        <text x={endX + 26} y={riverY - 12} fontSize={13} fill="#64748b">
          flow →
        </text>

        {[...storageNodes, ...riverNodes].map((node) => {
          const { site } = node;
          const tier = site.tierNow;
          const willRed = site.forecast.willReachRed;
          const labelAbove = node.y < riverY || isStorage(site.row.site_type);
          return (
            <g
              key={site.row.site_id}
              className="cursor-pointer"
              onClick={() => onOpen(site.row.site_id)}
            >
              {willRed && (
                <circle cx={node.x} cy={node.y} r={nodeSize / 2 + 7} fill="none" stroke="#dc2626" strokeWidth={2} />
              )}
              <rect
                x={node.x - nodeSize / 2}
                y={node.y - nodeSize / 2}
                width={nodeSize}
                height={nodeSize}
                rx={5}
                fill={TIER_FILL[tier]}
              />
              <text
                x={node.x}
                y={node.y + 5}
                textAnchor="middle"
                fontSize={14}
                fontWeight={700}
                fill="#ffffff"
              >
                {tier[0]}
              </text>
              <text
                x={node.x}
                y={labelAbove ? node.y - nodeSize / 2 - 8 : node.y + nodeSize / 2 + 18}
                textAnchor="middle"
                fontSize={13}
                fontWeight={600}
                fill="#0f172a"
              >
                {site.row.site_name}
              </text>
              {willRed && site.forecast.daysUntilRed != null && (
                <text
                  x={node.x}
                  y={labelAbove ? node.y - nodeSize / 2 - 24 : node.y + nodeSize / 2 + 34}
                  textAnchor="middle"
                  fontSize={12}
                  fontWeight={600}
                  fill="#dc2626"
                >
                  {site.forecast.daysUntilRed === 0
                    ? 'Red today'
                    : `Red in ${site.forecast.daysUntilRed} day${site.forecast.daysUntilRed === 1 ? '' : 's'}`}
                </text>
              )}
              {!willRed && site.row.qa_excluded_7d != null && n(site.row.qa_excluded_7d) > 0 && (
                <text
                  x={node.x}
                  y={node.y + nodeSize / 2 + 34}
                  textAnchor="middle"
                  fontSize={11}
                  fill="#64748b"
                >
                  QA flag
                </text>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

export function SchematicLegend() {
  return (
    <div className="flex flex-wrap items-center gap-4 text-sm text-slate-500">
      <span className="flex items-center gap-1.5">
        <span className="inline-block h-3.5 w-3.5 rounded-full border-2 border-red-600" />
        Forecast to turn Red within 7 days
      </span>
      <span className="flex items-center gap-1.5">
        <span className="inline-block h-3 w-3 rounded-sm bg-slate-700" />
        Storage (dam)
      </span>
      <span className="flex items-center gap-1.5">
        <span className="inline-block h-3 w-3 rounded-full bg-slate-500" />
        River site
      </span>
    </div>
  );
}
