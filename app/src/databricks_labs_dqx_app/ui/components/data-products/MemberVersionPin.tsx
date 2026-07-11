/**
 * MemberVersionPin — a small pin / use-latest control for a Data Product
 * member, ported from dqlake's `components/products/MemberVersionPin.tsx`.
 *
 * `pinnedVersion === null` means "use latest" (auto-follow the table's
 * monitor as it publishes new versions). A number pins the member to that
 * published version. The control offers "Latest (vN)" plus vN..v1.
 *
 * IMPORTANT — semantics differ from dqlake: in DQX a pin REALLY executes the
 * frozen version (design spec §3.4, "pin semantics = real execution, not
 * display"). Do NOT port dqlake's "the pin is governance metadata only, the
 * job always runs the current published version" comment — it does not
 * apply here. The stale indicator below still just signals that a newer
 * version exists; it does not change what the pin executes.
 */
import { AlertTriangle, Check } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Badge } from "@/components/ui/badge";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

interface Props {
  /** Current published version of the table's monitor. 0 = never approved. */
  bindingVersion: number;
  /** null = use latest; a number = pinned to that version. */
  pinnedVersion: number | null;
  /** Omit (or pass undefined) to render read-only. */
  onPinChange?: (version: number | null) => void;
}

/** True when a pin points at an older version than the table currently publishes. */
export function isMemberStale(pinnedVersion: number | null, bindingVersion: number): boolean {
  return pinnedVersion !== null && pinnedVersion < bindingVersion;
}

export function MemberVersionPin({ bindingVersion, pinnedVersion, onPinChange }: Props) {
  const { t } = useTranslation();
  const isPinned = pinnedVersion !== null;
  const stale = isMemberStale(pinnedVersion, bindingVersion);

  // No approved version yet: only "latest" is meaningful.
  const hasVersions = bindingVersion > 0;
  const versions = hasVersions ? Array.from({ length: bindingVersion }, (_, i) => bindingVersion - i) : [];

  const label = !hasVersions
    ? t("dataProducts.pinLatest")
    : isPinned
      ? t("dataProducts.pinnedVersioned", { version: pinnedVersion })
      : t("dataProducts.pinLatestVersioned", { version: bindingVersion });

  const badge = (
    // Item 41(b): the outline badge's transparent fill read as invisible
    // against the picker row. Give it a subtle fill that inverts per theme —
    // a faint dark tint in light mode, a faint light tint in dark mode — so
    // the trigger stays legible as a control in both.
    <Badge
      variant="outline"
      className="font-mono text-[10px] shrink-0 gap-1 whitespace-nowrap bg-foreground/5 dark:bg-foreground/15"
    >
      {label}
      {stale && <AlertTriangle className="h-3 w-3 text-amber-500" aria-hidden />}
      {onPinChange && hasVersions && <span aria-hidden>&#x25BE;</span>}
    </Badge>
  );

  const staleNote = stale ? (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex">{badge}</span>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs">
        {t("dataProducts.pinStaleTooltip", { latest: bindingVersion, pinned: pinnedVersion })}
      </TooltipContent>
    </Tooltip>
  ) : (
    badge
  );

  // Read-only (no edit rights, or nothing to pin against yet): badge + stale signal only.
  if (!onPinChange || !hasVersions) {
    return staleNote;
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button type="button" className="focus:outline-none">
          {staleNote}
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onClick={() => onPinChange(null)} className="gap-2">
          {!isPinned ? <Check className="h-3.5 w-3.5" /> : <span className="inline-block w-3.5" />}
          <span>{t("dataProducts.pinUseLatest", { version: bindingVersion })}</span>
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        {versions.map((v) => (
          <DropdownMenuItem key={v} onClick={() => onPinChange(v)} className="gap-2">
            {isPinned && pinnedVersion === v ? (
              <Check className="h-3.5 w-3.5" />
            ) : (
              <span className="inline-block w-3.5" />
            )}
            <span className="font-mono">{t("dataProducts.versionBadge", { version: v })}</span>
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
