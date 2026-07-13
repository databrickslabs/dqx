import { Suspense, useEffect, useState } from "react";
import { useReducedMotion } from "motion/react";
import type { LucideIcon } from "lucide-react";
import { ArrowDown, ArrowUp, Boxes, HelpCircle, LineChart, Library, Loader2, Minus, Table2 } from "lucide-react";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useTranslation } from "react-i18next";
import { Card, CardContent } from "@/components/ui/card";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { ScoreTrendChart } from "@/components/results/ScoreTrendChart";
import { useGetHomeStatsSuspense } from "@/lib/api";
import { useGlobalResultsEnabled } from "@/hooks/use-global-results-enabled";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { cn } from "@/lib/utils";
import { countUpValue, deltaDirection, deltaPoints, formatCount, formatScorePercent } from "./statFormat";

/**
 * Faithful port of dqlake's `components/home/HomeStats.tsx` — the "At a
 * Glance" stat-card row + overall score trend for the top of the Home
 * page — with the established Phase-2/3 adaptations:
 *
 * - data comes from OUR `GET /api/v1/home/stats` (app-DB counts + the
 *   cached global score and its `dq_score_history` trend/delta), not
 *   dqlake's warehouse-backed home summary — still zero warehouse;
 * - all display text through t() (4 locales);
 * - no idle polling — RESULTS_QUERY_OPTIONS. The score card refreshes via
 *   the run-completion / rule-application invalidations (the
 *   HOME_STATS_PATH_PREFIX registered in `lib/results-invalidation.ts`);
 *   the counts additionally refetch once per Home mount
 *   (`refetchOnMount: "always"` — a single cheap Postgres round-trip, so
 *   creating a rule/table/space elsewhere shows up on the next visit
 *   without wiring every CRUD mutation to this query);
 * - the third card is Table Spaces (our nav unit) instead of dqlake's
 *   warehouse-computed "Checks in place";
 * - the trend's x-axis is the cache recompute instant (`computed_at`
 *   of each history point) rather than dqlake's per-run date — the
 *   history rows ARE the run-completion recomputes, so the shape is
 *   the same signal.
 */

/** Animate a number from 0 → `target` (easeOutCubic) via rAF, powering the
 *  "At a Glance" count-up. The effect is keyed on `target`, so it runs once on
 *  each mount (a fresh Home visit replays the entrance, matching the trend
 *  chart's mount-reveal) and re-runs only when `target` genuinely changes — a
 *  background refetch returning the SAME value leaves `target` unchanged and
 *  does NOT restart the tween. `startDelayMs` staggers each card. Honours
 *  prefers-reduced-motion (final value, no ticking), mirroring `ScoreBox`. */
function useCountUp(target: number, durationMs = 800, startDelayMs = 0): number {
  const reduce = useReducedMotion();
  const [v, setV] = useState(target);
  useEffect(() => {
    if (reduce) {
      setV(target);
      return;
    }
    let raf = 0;
    let startTs = 0;
    const tick = (now: number) => {
      if (!startTs) startTs = now;
      const elapsed = now - startTs - startDelayMs;
      const t = Math.min(1, Math.max(0, elapsed / durationMs));
      setV(countUpValue(target, t));
      if (elapsed < durationMs) raf = requestAnimationFrame(tick);
    };
    setV(0);
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [target, durationMs, startDelayMs, reduce]);
  return reduce ? target : v;
}

/** The four stat cards, in display order. Static chrome (label + icon) so the
 *  loading state can render the cards immediately with an in-place spinner on
 *  the numbers (feels more responsive than skeleton boxes). The last card is
 *  the emphasised inverse card. Icons match our sidebar's per-page icons. */
const CARDS: { key: string; labelKey: string; icon: LucideIcon; inverted?: boolean }[] = [
  { key: "rules", labelKey: "home.stats.rules", icon: Library },
  { key: "tables", labelKey: "home.stats.tables", icon: Table2 },
  { key: "spaces", labelKey: "home.stats.spaces", icon: Boxes },
  { key: "score", labelKey: "home.stats.score", icon: LineChart, inverted: true },
];

/** Direction-of-change badge for the score card: green ▲ / red ▼ / grey =,
 *  comparing the latest cache recompute to the previous one. `delta` is a
 *  fraction (e.g. +0.05 ⇒ +5 percentage points); sub-0.05pp moves read as
 *  flat (the score itself is shown to one decimal) — see `deltaDirection`. */
function DeltaIndicator({ delta }: { delta: number }) {
  const { t } = useTranslation();
  const direction = deltaDirection(delta);
  const points = deltaPoints(delta);
  if (direction === "up") {
    const label = t("home.delta.up", { points });
    return (
      <span className="inline-flex items-center text-emerald-600" title={label}>
        <ArrowUp className="h-5 w-5" aria-label={label} />
      </span>
    );
  }
  if (direction === "down") {
    const label = t("home.delta.down", { points });
    return (
      <span className="inline-flex items-center text-red-600" title={label}>
        <ArrowDown className="h-5 w-5" aria-label={label} />
      </span>
    );
  }
  const label = t("home.delta.flat");
  return (
    <span className="inline-flex items-center text-neutral-500" title={label}>
      <Minus className="h-5 w-5" aria-label={label} />
    </span>
  );
}

/** One stat card: big bold value (or an in-place spinner while loading), a
 *  label, and a lucide icon. The emphasised card (`inverted`) is a solid
 *  black-on-light / white-on-dark card (`bg-foreground text-background`).
 *  `delta` (when given) renders the up/down/flat change badge next to the
 *  value.
 *
 *  The card chrome is always rendered at rest (no per-card fade/slide-in): the
 *  loading and loaded states show the *same* solid cards, so the Suspense
 *  fallback→content swap is seamless (only the number swaps spinner→value).
 *  The entrance animation is the number count-up alone — see `useCountUp` —
 *  which avoids the previous "solid skeleton cards → invisible → fade back in"
 *  flash that read as the section loading twice (B2-89). */
function StatCard({
  label,
  value,
  icon: Icon,
  inverted = false,
  loading = false,
  delta,
  infoText,
}: {
  label: string;
  value?: string;
  icon: LucideIcon;
  inverted?: boolean;
  loading?: boolean;
  delta?: number | null;
  /** When set, renders a small "?" tooltip next to the label. */
  infoText?: string;
}) {
  const { t } = useTranslation();
  return (
    <Card className={cn(inverted && "bg-foreground text-background")}>
      <CardContent className="relative p-5">
        {/* B2-1: the "?" explainer is pinned to the card's top-right corner
            (rather than sitting inline after the label) so a long label +
            the trigger can never push the big score value onto a second
            line. B2-20: only rendered when the global Results tab is enabled
            — the explainer describes a global-results-vs-home divergence
            that's moot when there's no global results screen. */}
        {infoText && (
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <button
                  type="button"
                  className={`absolute right-3 top-3 inline-flex items-center justify-center focus-visible:outline-none ${
                    inverted
                      ? "text-background/70 hover:text-background"
                      : "text-muted-foreground hover:text-foreground"
                  }`}
                  aria-label={infoText}
                >
                  <HelpCircle className="h-3.5 w-3.5" />
                </button>
              </TooltipTrigger>
              <TooltipContent className="max-w-xs text-xs">
                <p>{infoText}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        )}
        <div className="mb-2 flex items-center gap-2">
          <Icon
            className={`h-4 w-4 ${inverted ? "text-background" : "text-muted-foreground"}`}
          />
          <span
            className={`text-xs font-medium uppercase tracking-wide ${
              inverted ? "text-background/80" : "text-muted-foreground"
            }`}
          >
            {label}
          </span>
        </div>
        <div className="flex h-9 items-center gap-2 text-3xl font-semibold tabular-nums sm:h-10 sm:text-4xl">
          {loading ? (
            <Loader2
              className={`h-5 w-5 animate-spin ${inverted ? "text-background/70" : "text-muted-foreground"}`}
              aria-label={t("home.loading")}
            />
          ) : (
            value
          )}
          {!loading && delta != null && <DeltaIndicator delta={delta} />}
        </div>
      </CardContent>
    </Card>
  );
}

function CardGrid({ children }: { children: React.ReactNode }) {
  return <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">{children}</div>;
}

function HomeStatsContent({ sectionLabelClass }: { sectionLabelClass: string }) {
  const { t } = useTranslation();
  const { data } = useGetHomeStatsSuspense({
    query: { select: (d) => d.data, ...RESULTS_QUERY_OPTIONS, refetchOnMount: "always" },
  });
  const { rule_count, monitored_table_count, table_space_count, score, score_delta } = data;
  const trend = data.score_trend ?? [];
  // B2-20: the score "?" explains a global-results-vs-home divergence, so it's
  // only shown when the global Results surface is actually enabled.
  const globalResultsEnabled = useGlobalResultsEnabled();

  // "At a Glance" entrance: a staggered count-up of each number that runs once
  // per mount, so a fresh Home visit replays it (like the trend chart below).
  // Because each count-up is keyed on its target value, the skeleton→data swap
  // and a same-value background refetch within a visit don't restart the tween
  // — only a genuine value change or a fresh mount animates. The card chrome no
  // longer fades in (see `StatCard`), so there is no skeleton→content flash.
  const rulesA = useCountUp(rule_count ?? 0, 800, 0);
  const tablesA = useCountUp(monitored_table_count ?? 0, 800, 70);
  const spacesA = useCountUp(table_space_count ?? 0, 800, 140);
  const scoreA = useCountUp(score == null ? 0 : score * 100, 800, 210);

  const valueFor: Record<string, string> = {
    tables: formatCount(tablesA),
    rules: formatCount(rulesA),
    spaces: formatCount(spacesA),
    score: formatScorePercent(score, scoreA),
  };

  return (
    <div className="space-y-6">
      <section className="space-y-3">
        <h2 className={sectionLabelClass}>{t("home.atAGlance")}</h2>
        <CardGrid>
          {CARDS.map((c) => (
            <StatCard
              key={c.key}
              label={t(c.labelKey)}
              value={valueFor[c.key]}
              icon={c.icon}
              inverted={c.inverted}
              delta={c.key === "score" ? score_delta : undefined}
              infoText={c.key === "score" && globalResultsEnabled ? t("home.stats.scoreInfo") : undefined}
            />
          ))}
        </CardGrid>
      </section>

      {/* Overall score over time. The single-series ScoreTrendChart renders
          the red→green gradient line; ≥2 points only. Below that we show a
          calm placeholder rather than a broken one-point chart. */}
      <section className="space-y-3">
        <h2 className={sectionLabelClass}>{t("home.avgQuality")}</h2>
        {trend.length >= 2 ? (
          // Home renders inside a narrow `max-w-4xl` column. ScoreTrendChart
          // already clamps itself (`min-w-0 overflow-hidden`), but we wrap it
          // in a local `min-w-0` container here too so the responsive chart
          // can never push past the column and wrap/overflow.
          <div className="min-w-0 overflow-hidden">
            <ScoreTrendChart
              data={trend.map((p) => ({ run_date: p.ts, pass_rate: p.score }))}
              animate
            />
          </div>
        ) : (
          <div className="rounded-md border p-6 text-center text-sm text-muted-foreground">
            {t("home.notEnoughHistory")}
          </div>
        )}
      </section>
    </div>
  );
}

/** Loading state: the real card chrome with an in-place spinner on each number,
 *  so the page feels responsive (no skeleton flash). */
function HomeStatsLoading({ sectionLabelClass }: { sectionLabelClass: string }) {
  const { t } = useTranslation();
  return (
    <div className="space-y-6">
      <section className="space-y-3">
        <h2 className={sectionLabelClass}>{t("home.atAGlance")}</h2>
        <CardGrid>
          {CARDS.map((c) => (
            <StatCard
              key={c.key}
              label={t(c.labelKey)}
              icon={c.icon}
              inverted={c.inverted}
              loading
            />
          ))}
        </CardGrid>
      </section>
      <section className="space-y-3">
        <h2 className={sectionLabelClass}>{t("home.avgQuality")}</h2>
        <div className="flex h-[200px] items-center justify-center rounded-md border">
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" aria-label={t("home.loading")} />
        </div>
      </section>
    </div>
  );
}

/** Big stat cards + an overall score trend for the top of the Home page.
 *  `sectionLabelClass` is the shared grey, capitalised subheader style. */
export function HomeStats({
  sectionLabelClass = "text-xs font-medium uppercase tracking-wide text-muted-foreground",
}: {
  sectionLabelClass?: string;
} = {}) {
  const { t } = useTranslation();
  return (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <ErrorBoundary
          onReset={reset}
          fallbackRender={({ resetErrorBoundary }) => (
            <div className="rounded-md border p-4 text-sm text-muted-foreground">
              <p>{t("home.statsError")}</p>
              <button
                type="button"
                className="mt-2 underline"
                onClick={resetErrorBoundary}
              >
                {t("home.tryAgain")}
              </button>
            </div>
          )}
        >
          <Suspense
            fallback={<HomeStatsLoading sectionLabelClass={sectionLabelClass} />}
          >
            <HomeStatsContent sectionLabelClass={sectionLabelClass} />
          </Suspense>
        </ErrorBoundary>
      )}
    </QueryErrorResetBoundary>
  );
}
