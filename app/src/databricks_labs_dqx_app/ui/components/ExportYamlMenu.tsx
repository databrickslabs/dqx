import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { AxiosResponse } from "axios";
import { toast } from "sonner";
import { Download, FileDown, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { downloadExportFile, type ExportOut } from "@/lib/api-custom";
import { extractApiError } from "@/components/apply-rules/shared";

type ExportFetcher = () => Promise<AxiosResponse<ExportOut>>;

interface ExportYamlMenuProps {
  /** Fetches the DQX check-list export — always available. */
  fetchDqx: ExportFetcher;
  /** Fetches the ODCS DataContract export. Omit for DQX-only surfaces (the
   *  table-less rule registry has no physicalName to bind to). When omitted,
   *  the trigger becomes a single-action button (no format menu). */
  fetchOdcs?: ExportFetcher;
  /** Trigger button style. */
  variant?: "outline" | "ghost";
  size?: "sm" | "default";
  /** Icon-only trigger (for table row actions) — otherwise shows a text label. */
  iconOnly?: boolean;
  /** Overrides the default "Export" label on the labeled trigger. */
  label?: string;
  disabled?: boolean;
  className?: string;
}

/**
 * Export-to-YAML control shared by the Rule Registry, Monitored Tables, and
 * Table Spaces surfaces (page-level "Export" button + per-row/detail action).
 *
 * Single-format (registry): renders one button that downloads the DQX YAML.
 * Dual-format (tables / spaces): renders a dropdown offering DQX YAML or ODCS
 * data contract. Fetch happens on click; the returned `content` is downloaded
 * as `filename`. Errors surface as a toast.
 */
export function ExportYamlMenu({
  fetchDqx,
  fetchOdcs,
  variant = "outline",
  size = "sm",
  iconOnly = false,
  label,
  disabled = false,
  className,
}: ExportYamlMenuProps) {
  const { t } = useTranslation();
  const [busy, setBusy] = useState(false);

  const run = async (fetcher: ExportFetcher) => {
    if (busy) return;
    setBusy(true);
    try {
      const res = await fetcher();
      downloadExportFile(res.data);
      toast.success(t("exportYaml.success", { filename: res.data.filename }));
    } catch (err) {
      toast.error(extractApiError(err, t("exportYaml.failed")));
    } finally {
      setBusy(false);
    }
  };

  const icon = busy ? (
    <Loader2 className="h-3.5 w-3.5 animate-spin" />
  ) : (
    <Download className="h-3.5 w-3.5" />
  );
  const triggerLabel = label ?? t("exportYaml.button");
  const ariaLabel = label ?? t("exportYaml.ariaLabel");

  // Single-format (DQX only): a plain button, no menu.
  if (!fetchOdcs) {
    if (iconOnly) {
      return (
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant={variant}
              size={size}
              className={className ?? "h-7 w-7 p-0"}
              aria-label={ariaLabel}
              disabled={disabled || busy}
              onClick={() => void run(fetchDqx)}
            >
              {icon}
            </Button>
          </TooltipTrigger>
          <TooltipContent>{t("exportYaml.ariaLabel")}</TooltipContent>
        </Tooltip>
      );
    }
    return (
      <Button
        variant={variant}
        size={size}
        className={className ? `gap-1.5 ${className}` : "gap-1.5"}
        disabled={disabled || busy}
        onClick={() => void run(fetchDqx)}
      >
        {icon}
        {triggerLabel}
      </Button>
    );
  }

  // Dual-format: a dropdown offering DQX YAML or ODCS data contract.
  const trigger = iconOnly ? (
    <Button
      variant={variant}
      size={size}
      className={className ?? "h-7 w-7 p-0"}
      aria-label={ariaLabel}
      disabled={disabled || busy}
    >
      {icon}
    </Button>
  ) : (
    <Button
      variant={variant}
      size={size}
      className={className ? `gap-1.5 ${className}` : "gap-1.5"}
      disabled={disabled || busy}
    >
      {icon}
      {triggerLabel}
    </Button>
  );

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>{trigger}</DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem className="gap-2" onClick={() => void run(fetchDqx)}>
          <FileDown className="h-3.5 w-3.5" />
          {t("exportYaml.dqxOption")}
        </DropdownMenuItem>
        <DropdownMenuItem className="gap-2" onClick={() => void run(fetchOdcs)}>
          <FileDown className="h-3.5 w-3.5" />
          {t("exportYaml.odcsOption")}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
