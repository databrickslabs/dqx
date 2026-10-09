import { useTranslation } from "react-i18next";
import { headerTitle } from "@/lib/branding/header";
import { cn } from "@/lib/utils";

interface ThemeMockProps {
  /** Full token map from deriveAllTokens (validated hex values). */
  tokens: Record<string, string>;
  companyName?: string | null;
  logoSrc?: string | null;
  size: "thumb" | "preview";
  className?: string;
}

/** Bars standing in for text in the thumbnail mock. */
function Bar({ color, className }: { color: string; className?: string }) {
  return <span className={cn("block h-1 rounded-full", className)} style={{ background: color }} />;
}

function ThumbMock({ tokens, className }: { tokens: Record<string, string>; className?: string }) {
  return (
    <div
      aria-hidden="true"
      className={cn("flex h-16 flex-col overflow-hidden rounded-md border", className)}
      style={{ background: tokens["--background"], borderColor: tokens["--border"] }}
    >
      <div className="flex h-3 shrink-0 items-center px-1.5" style={{ background: tokens["--header"] }}>
        <Bar color={tokens["--header-foreground"]} className="w-6" />
      </div>
      <div className="flex min-h-0 flex-1">
        <div className="flex w-1/4 flex-col gap-1 p-1" style={{ background: tokens["--sidebar"] }}>
          <span className="block rounded-sm px-0.5 py-0.5" style={{ background: tokens["--sidebar-accent"] }}>
            <Bar color={tokens["--sidebar-accent-foreground"]} />
          </span>
          <Bar color={tokens["--sidebar-foreground"]} className="mx-0.5" />
        </div>
        <div className="flex flex-1 flex-col justify-between p-1.5">
          <div className="space-y-1">
            <Bar color={tokens["--foreground"]} className="w-3/4" />
            <Bar color={tokens["--muted-foreground"]} className="w-1/2" />
          </div>
          <span className="block h-2.5 w-8 rounded-sm" style={{ background: tokens["--primary"] }} />
        </div>
      </div>
    </div>
  );
}

function PreviewMock({
  tokens,
  companyName,
  logoSrc,
  className,
}: {
  tokens: Record<string, string>;
  companyName?: string | null;
  logoSrc?: string | null;
  className?: string;
}) {
  const { t } = useTranslation();
  const { product, company } = headerTitle(companyName);
  return (
    <div
      className={cn("flex h-56 flex-col overflow-hidden rounded-md border text-xs", className)}
      style={{ background: tokens["--background"], color: tokens["--foreground"], borderColor: tokens["--border"] }}
    >
      <div
        className="flex h-10 shrink-0 items-center gap-2 border-b px-3"
        style={{ background: tokens["--header"], color: tokens["--header-foreground"], borderColor: tokens["--border"] }}
      >
        {logoSrc ? (
          <img src={logoSrc} alt="" className="h-5 w-auto max-w-32 object-contain" />
        ) : (
          <img src="/dqx-logo.svg" alt="" className="h-5 w-5" />
        )}
        <span className="truncate text-sm font-semibold">
          {product}
          {company && (
            <>
              <span className="mx-1.5 opacity-60" aria-hidden="true">
                |
              </span>
              {company}
            </>
          )}
        </span>
      </div>
      <div className="flex min-h-0 flex-1">
        <div
          className="flex w-36 shrink-0 flex-col gap-1 border-r p-2"
          style={{
            background: tokens["--sidebar"],
            color: tokens["--sidebar-foreground"],
            borderColor: tokens["--sidebar-border"],
          }}
        >
          <span
            className="truncate rounded-md px-2 py-1.5 font-medium"
            style={{ background: tokens["--sidebar-accent"], color: tokens["--sidebar-accent-foreground"] }}
          >
            {t("config.styling.previewNavItemActive")}
          </span>
          <span className="truncate px-2 py-1.5">{t("config.styling.previewNavItem")}</span>
        </div>
        <div className="min-w-0 flex-1 p-3">
          <div
            className="space-y-2 rounded-md border p-3"
            style={{ background: tokens["--card"], color: tokens["--card-foreground"], borderColor: tokens["--border"] }}
          >
            <div className="flex items-center justify-between gap-2">
              <span className="truncate font-semibold">{t("config.styling.previewCardTitle")}</span>
              <span style={{ color: tokens["--muted-foreground"] }}>98%</span>
            </div>
            <div
              className="truncate rounded-sm px-2 py-1.5"
              style={{ background: tokens["--accent"], color: tokens["--accent-foreground"] }}
            >
              {t("config.styling.previewHoverRow")}
            </div>
            <span
              className="inline-flex h-7 items-center rounded-md px-3 font-medium"
              style={{ background: tokens["--primary"], color: tokens["--primary-foreground"] }}
            >
              {t("config.styling.previewButton")}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}

/** Miniature app rendered from theme tokens; never touches the live theme. */
export function ThemeMock({ tokens, companyName, logoSrc, size, className }: ThemeMockProps) {
  return size === "thumb" ? (
    <ThumbMock tokens={tokens} className={className} />
  ) : (
    <PreviewMock tokens={tokens} companyName={companyName} logoSrc={logoSrc} className={className} />
  );
}
