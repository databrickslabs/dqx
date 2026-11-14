import { useTranslation } from "react-i18next";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { isImportStatus, type ImportStatus } from "@/components/imports/import-status";

/**
 * Single-line "Import as [Draft | Published]" option row, sized to sit next to
 * the other compact option rows of an import form. *Published* stays visible
 * but disabled for users who can't approve rules, with a tooltip saying why.
 */
export function ImportAsSelect({
  value,
  onChange,
  canPublish,
  disabled,
}: {
  value: ImportStatus;
  onChange: (value: ImportStatus) => void;
  canPublish: boolean;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const publishedLabel = t("rulesBulkImport.options.importAsPublished");

  return (
    <div className="flex items-center gap-2 rounded-lg border p-3">
      <span className="text-sm font-medium">{t("rulesBulkImport.options.importAs")}</span>
      <Select
        value={value}
        onValueChange={(next) => {
          if (isImportStatus(next)) onChange(next);
        }}
        disabled={disabled}
      >
        <SelectTrigger className="ml-auto h-8 w-[110px] text-xs">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="draft">{t("rulesBulkImport.options.importAsDraft")}</SelectItem>
          {canPublish ? (
            <SelectItem value="published">{publishedLabel}</SelectItem>
          ) : (
            <Tooltip>
              {/* A disabled item has pointer-events: none, so the hover target
                  is this wrapper rather than the item itself. */}
              <TooltipTrigger asChild>
                <span className="block cursor-not-allowed">
                  <SelectItem value="published" disabled>
                    {publishedLabel}
                  </SelectItem>
                </span>
              </TooltipTrigger>
              <TooltipContent side="right">
                {t("rulesBulkImport.options.publishNotPermitted")}
              </TooltipContent>
            </Tooltip>
          )}
        </SelectContent>
      </Select>
    </div>
  );
}
