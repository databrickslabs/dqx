import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Search, Github } from "lucide-react";

// Deep-link to the pack YAML directory on GitHub so a contributor lands
// exactly where new content packs live.
const MARKETPLACE_PACKS_REPO_URL =
  "https://github.com/databrickslabs/dqx/tree/main/app/src/databricks_labs_dqx_app/backend/marketplace/packs";
import { toast } from "sonner";
import { useQueryClient } from "@tanstack/react-query";
import {
  useListMarketplacePacksSuspense,
  useListCheckFunctions,
  getListMarketplacePacksQueryKey,
} from "@/lib/api";
import type { MarketplacePackOut, MarketplacePacksOut } from "@/lib/api";
import { useLabelDefinitions } from "@/lib/api-custom";
import type { LabelColorDefinition } from "@/components/RegistryRuleBadges";
import selector from "@/lib/selector";
import {
  type MarketplaceFilters,
  ruleMatchesFilters,
  collectIndustries,
  collectRegions,
  toggleRule,
  togglePack,
  selectedCheckDicts,
  formatTagLabel,
  regionTier,
} from "@/lib/marketplace-selection";
import { importChecksAsRegistryDrafts } from "@/lib/import-registry-rules";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { FadeIn } from "@/components/anim/FadeIn";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { DeployDemoRow } from "./DeployDemoRow";
import { PackGroup } from "./PackGroup";

// ---------------------------------------------------------------------------
// Chip filter component
// ---------------------------------------------------------------------------

function FilterChip({
  label,
  active,
  onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "inline-flex items-center rounded-full border px-3 py-1 text-xs font-medium transition-colors",
        active
          ? "border-primary bg-primary text-primary-foreground"
          : "border-border bg-transparent text-muted-foreground hover:border-primary/50 hover:text-foreground",
      )}
    >
      {label}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Inner content — must be rendered inside a Suspense boundary, requires
// checkFunctions to be loaded before preview rows render correctly.
// ---------------------------------------------------------------------------

function MarketplaceContent() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();

  // Packs via Suspense — guaranteed resolved before render.
  // selector() extracts .data so TData = MarketplacePacksOut.
  const { data: packsOut } = useListMarketplacePacksSuspense<MarketplacePacksOut>(
    selector<MarketplacePacksOut>(),
  );

  // Check functions — non-suspense; guard: pass empty array until loaded so
  // preview rows degrade gracefully rather than crashing. Rows that cannot
  // resolve a check function simply skip the RuleLogicDisclosure section.
  const { data: fnResp } = useListCheckFunctions();
  const checkFunctions = fnResp?.data?.functions ?? [];

  // Label definitions drive the coloured dimension/severity markers so the
  // marketplace rows read identically to the rest of the app.
  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = (labelDefsData?.definitions ?? []) as LabelColorDefinition[];

  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [filters, setFilters] = useState<MarketplaceFilters>({
    industry: "all",
    region: "all",
    search: "",
  });
  // Pack accordion — at most one pack open at a time.
  const [expandedPackId, setExpandedPackId] = useState<string | null>(null);
  const [openRuleByPack, setOpenRuleByPack] = useState<Record<string, string | null>>({});
  const [isImporting, setIsImporting] = useState(false);

  const allPacks: MarketplacePackOut[] = packsOut?.packs ?? [];

  const industries = useMemo(() => collectIndustries(allPacks), [allPacks]);
  const regions = useMemo(() => collectRegions(allPacks), [allPacks]);

  const visiblePacks = useMemo(
    () =>
      allPacks
        .map((p) => ({ ...p, rules: p.rules.filter((r) => ruleMatchesFilters(r, filters)) }))
        .filter((p) => p.rules.length > 0),
    [allPacks, filters],
  );

  // While searching, expand every pack with a hit (so matches aren't hidden);
  // otherwise honour the single-open accordion selection.
  const isSearching = filters.search.trim() !== "";
  function isPackExpanded(packId: string): boolean {
    return isSearching ? true : expandedPackId === packId;
  }

  function handleToggleRule(key: string) {
    setSelected((prev) => toggleRule(prev, key));
  }

  function handleTogglePack(packRuleKeys: string[]) {
    setSelected((prev) => togglePack(prev, packRuleKeys));
  }

  function handleTogglePackExpanded(packId: string) {
    // Accordion: open the clicked pack, or close it if it was already open.
    setExpandedPackId((prev) => (prev === packId ? null : packId));
  }

  function handleOpenRule(packId: string, ruleKey: string | null) {
    setOpenRuleByPack((prev) => ({ ...prev, [packId]: ruleKey }));
  }

  async function handleImport() {
    if (isImporting || selected.size === 0) return;
    setIsImporting(true);
    try {
      const dicts = selectedCheckDicts(allPacks, selected);
      // autoApprove publishes each imported rule outright (submit + approve) —
      // the Marketplace is admin-only and its packs are curated, so they arrive
      // approved and ready to apply, never as pending drafts in a review queue.
      const result = await importChecksAsRegistryDrafts({
        checks: dicts,
        checkFunctions,
        t,
        authorKind: "human",
        autoApprove: true,
        source: "marketplace",
      });
      if (result.failed > 0) {
        toast.error(
          t("marketplace.importPartial", {
            saved: result.saved,
            reused: result.reused,
            failed: result.failed,
          }),
        );
      } else {
        toast.success(
          t("marketplace.importDone", { saved: result.saved, reused: result.reused }),
        );
      }
      setSelected(new Set());
      // Refetch so the just-added rules flip to `imported` (disabled) without a
      // manual page reload.
      void queryClient.invalidateQueries({ queryKey: getListMarketplacePacksQueryKey() });
    } catch {
      toast.error(t("marketplace.importError"));
    } finally {
      setIsImporting(false);
    }
  }

  const selectedCount = selected.size;

  return (
    <div className="space-y-6">
      <PageBreadcrumb page={t("marketplace.title")} />

      {/* Page header + right-aligned import action — matches the standard
          page header (registry-rules, runs-history): tracking-tight title,
          muted subtitle, primary action on the right. */}
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">{t("marketplace.title")}</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            {t("marketplace.subtitle")}{" "}
            <a
              href={MARKETPLACE_PACKS_REPO_URL}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 text-foreground underline-offset-2 hover:underline"
            >
              {t("marketplace.contribute")}
              <Github className="h-3.5 w-3.5" aria-hidden />
            </a>
          </p>
        </div>
        <Button
          className="shrink-0"
          disabled={selectedCount === 0 || isImporting}
          onClick={() => void handleImport()}
        >
          {t("marketplace.importSelected", { count: selectedCount })}
        </Button>
      </div>

      {/* Search */}
      <div className="relative max-w-sm">
        <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" aria-hidden />
        <Input
          className="pl-8"
          placeholder={t("marketplace.searchPlaceholder")}
          value={filters.search}
          onChange={(e) => setFilters((prev) => ({ ...prev, search: e.target.value }))}
        />
      </div>

      {/* Filters — industry and region on ONE line. Each group is sized to
          its content (shrink, don't grow) so there's no dead 50/50 gap; when
          the row runs out of room each group shrinks and scrolls INDEPENDENTLY.
          The scrollbar track is always reserved and only the thumb fades in on
          scroll (dq-scroll-auto), so scrolling never reflows the chips. */}
      {(industries.length > 1 || regions.length > 1) && (
        <div className="flex items-center gap-3 overflow-hidden">
          {industries.length > 1 && (
            <div className="flex min-w-0 shrink items-center gap-2">
              <span className="shrink-0 text-xs font-medium text-muted-foreground">
                {t("marketplace.industryLabel")}
              </span>
              <div className="dq-scroll-auto flex h-9 min-w-0 items-center gap-2 overflow-x-auto overflow-y-hidden">
                {industries.map((ind) => (
                  <FilterChip
                    key={ind}
                    label={ind === "all" ? t("marketplace.all") : formatTagLabel(ind)}
                    active={filters.industry === ind}
                    onClick={() => setFilters((prev) => ({ ...prev, industry: ind }))}
                  />
                ))}
              </div>
            </div>
          )}
          {industries.length > 1 && regions.length > 1 && (
            <div className="h-6 w-px shrink-0 bg-border" aria-hidden />
          )}
          {regions.length > 1 && (
            <div className="flex min-w-0 shrink items-center gap-2">
              <span className="shrink-0 text-xs font-medium text-muted-foreground">
                {t("marketplace.regionLabel")}
              </span>
              {/* Tier-ordered (global → macro → country) with a divider between
                  tier groups. */}
              <div className="dq-scroll-auto flex h-9 min-w-0 items-center gap-2 overflow-x-auto overflow-y-hidden">
                {regions.map((reg, i) => {
                  const showDivider = i > 0 && regionTier(reg) !== regionTier(regions[i - 1]);
                  return (
                    <div key={reg} className="flex shrink-0 items-center gap-2">
                      {showDivider && <div className="h-5 w-px bg-border" aria-hidden />}
                      <FilterChip
                        label={reg === "all" ? t("marketplace.all") : formatTagLabel(reg)}
                        active={filters.region === reg}
                        onClick={() => setFilters((prev) => ({ ...prev, region: reg }))}
                      />
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Demo content row — always first */}
      <DeployDemoRow />

      {/* Pack list */}
      <div className="space-y-3">
        {visiblePacks.map((pack) => (
          <PackGroup
            key={pack.id}
            pack={pack}
            selected={selected}
            onToggleRule={handleToggleRule}
            onTogglePack={handleTogglePack}
            openRuleKey={openRuleByPack[pack.id] ?? null}
            onOpenRule={(key) => handleOpenRule(pack.id, key)}
            checkFunctions={checkFunctions}
            labelDefinitions={labelDefinitions}
            expanded={isPackExpanded(pack.id)}
            onToggleExpanded={() => handleTogglePackExpanded(pack.id)}
          />
        ))}
        {visiblePacks.length === 0 && (
          <p className="py-8 text-center text-sm text-muted-foreground">
            {t("marketplace.noResults")}
          </p>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Public export — wraps the inner content in FadeIn + Suspense
// ---------------------------------------------------------------------------

export function MarketplacePage() {
  return (
    <FadeIn>
      <MarketplaceContent />
    </FadeIn>
  );
}
