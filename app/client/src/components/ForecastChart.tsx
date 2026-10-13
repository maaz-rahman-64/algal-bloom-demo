import type { ForecastPoint } from '../lib/domain';
import { weekday, getThresholds } from '../lib/domain';

export function ForecastChart({ points }: { points: ForecastPoint[] }) {
  const W = 720;
  const H = 300;
  const left = 46;
  const right = 700;
  const top = 18;
  const bottom = 250;
  const nPts = points.length;

  const x = (i: number) => left + (i * (right - left)) / Math.max(nPts - 1, 1);
  const y = (pct: number) => top + ((100 - pct) / 100) * (bottom - top);

  const { redProbability } = getThresholds();
  const thresholdY = y(redProbability * 100);
  const pts = points.map((p, i) => ({ ...p, px: x(i), py: y(p.prob * 100), pct: Math.round(p.prob * 100) }));
  const linePath = pts.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.px} ${p.py}`).join(' ');
  const areaPath = `${linePath} L ${pts.at(-1)!.px} ${bottom} L ${pts[0].px} ${bottom} Z`;
  const crossing = pts.find((p) => p.tier === 'Red') ?? null;

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" role="img" aria-label="Chance of a Red alert over the next 7 days">
      {/* Red-likely band (>= threshold) */}
      <rect x={left} y={top} width={right - left} height={thresholdY - top} fill="#fee2e2" opacity={0.6} />
      <text x={left + 8} y={top + 18} fontSize={12} fill="#b91c1c" fontWeight={600}>
        Red likely (≥ {Math.round(redProbability * 100)}%)
      </text>

      {/* gridlines + y labels */}
      {[0, 25, 50, 75, 100].map((t) => (
        <g key={t}>
          <line x1={left} y1={y(t)} x2={right} y2={y(t)} stroke="#e2e8f0" strokeWidth={1} />
          <text x={left - 8} y={y(t) + 4} fontSize={11} textAnchor="end" fill="#94a3b8">
            {t}%
          </text>
        </g>
      ))}

      {/* threshold line */}
      <line x1={left} y1={thresholdY} x2={right} y2={thresholdY} stroke="#dc2626" strokeWidth={1.5} strokeDasharray="2 3" />

      {/* area + line */}
      <path d={areaPath} fill="#3b82f6" opacity={0.12} />
      <path d={linePath} fill="none" stroke="#1d4ed8" strokeWidth={2.5} />

      {pts.map((p) => (
        <g key={p.day}>
          <circle cx={p.px} cy={p.py} r={4} fill={p.tier === 'Red' ? '#dc2626' : '#1d4ed8'} />
          <text
            x={p.px}
            y={bottom + 20}
            textAnchor="middle"
            fontSize={12}
            fontWeight={crossing && p.day === crossing.day ? 700 : 400}
            fill="#475569"
          >
            {weekday(p.date)}
          </text>
        </g>
      ))}

      {/* endpoint label */}
      <text x={pts.at(-1)!.px - 4} y={pts.at(-1)!.py - 10} textAnchor="end" fontSize={12} fontWeight={600} fill="#1e293b">
        {pts.at(-1)!.pct}%
      </text>

      {/* crossing annotation */}
      {crossing && (
        <>
          <circle cx={crossing.px} cy={crossing.py} r={6} fill="#dc2626" />
          <text x={crossing.px + 10} y={crossing.py + 22} fontSize={12} fontWeight={700} fill="#0f172a">
            {crossing.pct}% · crosses Red
          </text>
        </>
      )}
    </svg>
  );
}
