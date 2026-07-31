import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Search } from "lucide-react";
import { toast } from "sonner";
import { useListMarketplacePacksSuspense, useListCheckFunctions } from "@/lib/api";
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
} from "@/lib/marketplace-selection";
import { importChecksAsRegistryDrafts } from "@/lib/import-registry-rules";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { FadeIn } from "@/components/anim/FadeIn";
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
  const [expandedPacks, setExpandedPacks] = useState<Set<string>>(new Set());
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

  // Auto-expand packs that have search hits
  const effectiveExpanded = useMemo<Set<string>>(() => {
    if (filters.search.trim() !== "") {
      return new Set(visiblePacks.map((p) => p.id));
    }
    return expandedPacks;
  }, [filters.search, visiblePacks, expandedPacks]);

  function handleToggleRule(key: string) {
    setSelected((prev) => toggleRule(prev, key));
  }

  function handleTogglePack(packRuleKeys: string[]) {
    setSelected((prev) => togglePack(prev, packRuleKeys));
  }

  function handleTogglePackExpanded(packId: string) {
    setExpandedPacks((prev) => {
      const next = new Set(prev);
      if (next.has(packId)) next.delete(packId);
      else next.add(packId);
      return next;
    });
  }

  function handleOpenRule(packId: string, ruleKey: string | null) {
    setOpenRuleByPack((prev) => ({ ...prev, [packId]: ruleKey }));
  }

  async function handleImport() {
    if (isImporting || selected.size === 0) return;
    setIsImporting(true);
    try {
      const dicts = selectedCheckDicts(allPacks, selected);
      // alsoSubmit submits each imported rule for review; in the workspace's
      // approvals mode (admin-only marketplace) that publishes them straight to
      // approved rather than leaving drafts — the packs are curated, so they
      // arrive ready to apply, not as pending drafts.
      const result = await importChecksAsRegistryDrafts({
        checks: dicts,
        checkFunctions,
        t,
        authorKind: "human",
        alsoSubmit: true,
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
    } catch {
      toast.error(t("marketplace.importError"));
    } finally {
      setIsImporting(false);
    }
  }

  const selectedCount = selected.size;

  return (
    <div className="space-y-5">
      {/* Page header + right-aligned import action */}
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">{t("marketplace.title")}</h1>
          <p className="mt-1 text-sm text-muted-foreground">{t("marketplace.subtitle")}</p>
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

      {/* Filters — industry + region on one line, in a filled bar, with
          horizontal scroll for overflow so the row never wraps or clips. */}
      {(industries.length > 1 || regions.length > 1) && (
        <div className="flex items-center gap-4 overflow-x-auto rounded-lg border bg-muted/40 px-3 py-2">
          {industries.length > 1 && (
            <div className="flex shrink-0 items-center gap-2">
              <span className="text-xs font-medium text-muted-foreground">
                {t("marketplace.industryLabel")}
              </span>
              {industries.map((ind) => (
                <FilterChip
                  key={ind}
                  label={ind === "all" ? t("marketplace.all") : formatTagLabel(ind)}
                  active={filters.industry === ind}
                  onClick={() => setFilters((prev) => ({ ...prev, industry: ind }))}
                />
              ))}
            </div>
          )}
          {industries.length > 1 && regions.length > 1 && (
            <div className="h-5 w-px shrink-0 bg-border" aria-hidden />
          )}
          {regions.length > 1 && (
            <div className="flex shrink-0 items-center gap-2">
              <span className="text-xs font-medium text-muted-foreground">
                {t("marketplace.regionLabel")}
              </span>
              {regions.map((reg) => (
                <FilterChip
                  key={reg}
                  label={reg === "all" ? t("marketplace.all") : formatTagLabel(reg)}
                  active={filters.region === reg}
                  onClick={() => setFilters((prev) => ({ ...prev, region: reg }))}
                />
              ))}
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
            expanded={effectiveExpanded.has(pack.id)}
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
