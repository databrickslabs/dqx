/** A read-only SQL cell with icon buttons to copy the SQL and, optionally, run it.
 *
 *  Copy is always offered. Run appears only when the caller passes *onRun*, so
 *  people who can't run the SQL (for example non-administrators) still get the
 *  copyable text to pass on. The result of a run renders directly under the cell
 *  through the *result* slot.
 */
import { useEffect, useRef, useState } from "react";
import { Check, Copy, Loader2, Play } from "lucide-react";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

/** How long the copy icon shows its "Copied" state. */
const COPIED_RESET_MS = 2_000;

/** The part of the browser clipboard the code block writes to. */
export type ClipboardWriter = { writeText(text: string): Promise<void> };

function browserClipboard(): ClipboardWriter | null {
  return typeof navigator !== "undefined" && navigator.clipboard
    ? navigator.clipboard
    : null;
}

/** Copy *sql* to the clipboard; resolves to whether it was copied. */
export async function copySqlToClipboard(
  sql: string,
  clipboard: ClipboardWriter | null = browserClipboard(),
): Promise<boolean> {
  if (!clipboard) return false;
  try {
    await clipboard.writeText(sql);
    return true;
  } catch {
    return false;
  }
}

type SqlCodeBlockProps = {
  sql: string;
  /** Runs the SQL; the Run icon is hidden without it. */
  onRun?: () => void;
  /** Tooltip and accessible name of the Run icon, when the default doesn't say enough. */
  runLabel?: string;
  /** Shows a spinner on the Run icon and disables it. */
  isRunning?: boolean;
  /** Disables the Run icon without a spinner, for example while something else runs. */
  runDisabled?: boolean;
  /** Outcome of the last run, rendered directly under the cell. */
  result?: React.ReactNode;
};

function IconButton({
  label,
  disabled,
  onClick,
  children,
}: {
  label: string;
  disabled?: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          className="size-7 bg-background/80 text-muted-foreground hover:text-foreground"
          aria-label={label}
          disabled={disabled}
          onClick={onClick}
        >
          {children}
        </Button>
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  );
}

export function SqlCodeBlock({
  sql,
  onRun,
  runLabel: customRunLabel,
  isRunning = false,
  runDisabled = false,
  result,
}: SqlCodeBlockProps) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);
  const [copyFailed, setCopyFailed] = useState(false);
  const resetTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(
    () => () => {
      if (resetTimer.current) clearTimeout(resetTimer.current);
    },
    [],
  );

  const handleCopy = async () => {
    const ok = await copySqlToClipboard(sql);
    setCopyFailed(!ok);
    setCopied(ok);
    if (resetTimer.current) clearTimeout(resetTimer.current);
    if (ok) resetTimer.current = setTimeout(() => setCopied(false), COPIED_RESET_MS);
  };

  const copyLabel = copied ? t("common.copied") : t("common.copy");
  const runLabel = customRunLabel ?? t("sqlCodeBlock.run");

  return (
    <div className="space-y-2">
      <div className="relative">
        <pre className="overflow-x-auto whitespace-pre-wrap break-words rounded-md border bg-muted/40 p-3 pr-20 font-mono text-xs leading-relaxed">
          {sql}
        </pre>
        <div className="absolute right-1.5 top-1.5 flex gap-1">
          <IconButton label={copyLabel} onClick={() => void handleCopy()}>
            {copied ? (
              <Check aria-hidden="true" />
            ) : (
              <Copy aria-hidden="true" />
            )}
          </IconButton>
          {onRun && (
            <IconButton
              label={runLabel}
              disabled={isRunning || runDisabled}
              onClick={onRun}
            >
              {isRunning ? (
                <Loader2
                  className="animate-spin motion-reduce:animate-none"
                  aria-hidden="true"
                />
              ) : (
                <Play aria-hidden="true" />
              )}
            </IconButton>
          )}
        </div>
      </div>
      {copyFailed && (
        <p className="text-xs text-muted-foreground">
          {t("sqlCodeBlock.copyFailed")}
        </p>
      )}
      {result}
    </div>
  );
}
