import { Circle, Triangle } from 'lucide-react';
import type { Tier, ForecastPoint } from '../lib/domain';
import { profile } from '../../../shared/profile';

export const BRAND = profile.brandColour;

const badgeStyles: Record<Tier, string> = {
  Green: 'bg-green-100 text-green-800 border-green-200',
  Amber: 'bg-amber-100 text-amber-900 border-amber-200',
  Red: 'bg-red-100 text-red-800 border-red-200',
};

const squareStyles: Record<Tier, string> = {
  Green: 'bg-green-600 text-white',
  Amber: 'bg-amber-500 text-white',
  Red: 'bg-red-600 text-white',
};

const dotStyles: Record<Tier, string> = {
  Green: 'text-green-600 fill-green-600',
  Amber: 'text-amber-500 fill-amber-500',
  Red: 'text-red-600 fill-red-600',
};

export function TierBadge({ tier, className = '' }: { tier: Tier; className?: string }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-sm font-medium ${badgeStyles[tier]} ${className}`}
    >
      {tier === 'Amber' ? (
        <Triangle className={`h-3 w-3 ${dotStyles[tier]}`} />
      ) : (
        <Circle className={`h-2.5 w-2.5 ${dotStyles[tier]}`} />
      )}
      {tier}
    </span>
  );
}

export function TierSquare({ tier, size = 'md' }: { tier: Tier; size?: 'sm' | 'md' }) {
  const dims = size === 'sm' ? 'h-6 w-6 text-[11px]' : 'h-7 w-7 text-xs';
  return (
    <span
      className={`inline-flex items-center justify-center rounded font-bold ${dims} ${squareStyles[tier]}`}
      title={tier}
    >
      {tier[0]}
    </span>
  );
}

export function ForecastChips({ points, size = 'md' }: { points: ForecastPoint[]; size?: 'sm' | 'md' }) {
  return (
    <div className="flex flex-wrap gap-1">
      {points.map((p) => (
        <TierSquare key={p.day} tier={p.tier} size={size} />
      ))}
    </div>
  );
}

export function TierLegend() {
  return (
    <div className="flex items-center gap-4 text-sm text-muted-foreground">
      <span className="flex items-center gap-1.5">
        <TierSquare tier="Green" size="sm" /> Green
      </span>
      <span className="flex items-center gap-1.5">
        <TierSquare tier="Amber" size="sm" /> Amber
      </span>
      <span className="flex items-center gap-1.5">
        <TierSquare tier="Red" size="sm" /> Red
      </span>
    </div>
  );
}
