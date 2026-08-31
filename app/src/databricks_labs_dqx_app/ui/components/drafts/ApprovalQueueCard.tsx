import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { FadeIn } from "@/components/anim/FadeIn";
import type { LucideIcon } from "lucide-react";

/**
 * Shared shell for the three Drafts & Review approval queues (registry rules,
 * monitored tables, table spaces) so they render with a consistent header,
 * loading/error/empty treatment, and scrollable table chrome (Stream J item
 * 13d). Each queue supplies its own icon/title/description, its own table
 * ``head`` (a ``<tr>`` of ``<th>``), and its ``rows`` (``<tbody>`` content).
 */
export function ApprovalQueueCard({
  icon: Icon,
  title,
  description,
  isLoading,
  error,
  errorText,
  isEmpty,
  emptyText,
  emptyIcon: EmptyIcon,
  minWidth = "700px",
  head,
  rows,
}: {
  icon: LucideIcon;
  title: string;
  description: string;
  isLoading: boolean;
  error: boolean;
  errorText: string;
  isEmpty: boolean;
  emptyText: string;
  emptyIcon?: LucideIcon;
  minWidth?: string;
  head: ReactNode;
  rows: ReactNode;
}) {
  const EmptyGlyph = EmptyIcon ?? Icon;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Icon className="h-5 w-5" />
          {title}
        </CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading && (
          <div className="space-y-2">
            {[1, 2].map((i) => (
              <Skeleton key={i} className="h-14 w-full" />
            ))}
          </div>
        )}

        {!isLoading && error && <p className="text-destructive text-sm">{errorText}</p>}

        {!isLoading && !error && isEmpty && (
          <div className="flex flex-col items-center justify-center py-10 text-center">
            <div className="w-14 h-14 rounded-full bg-muted flex items-center justify-center mb-4">
              <EmptyGlyph className="h-7 w-7 text-muted-foreground" />
            </div>
            <p className="text-muted-foreground text-sm">{emptyText}</p>
          </div>
        )}

        {!isLoading && !error && !isEmpty && (
          <FadeIn duration={0.3}>
            <div className="border rounded-lg overflow-x-auto">
              <table className="w-full text-sm" style={{ minWidth }}>
                <thead>
                  <tr className="border-b bg-muted/50">{head}</tr>
                </thead>
                <tbody>{rows}</tbody>
              </table>
            </div>
          </FadeIn>
        )}
      </CardContent>
    </Card>
  );
}

/** Shared column-header cell for the approval queue tables. */
export function ApprovalTh({ children, align = "left" }: { children: ReactNode; align?: "left" | "right" }) {
  return (
    <th className={`${align === "right" ? "text-right" : "text-left"} p-3 font-medium whitespace-nowrap`}>
      {children}
    </th>
  );
}

/** Read-only marker shown in the actions column when the user can't approve. */
export function ReadOnlyActionHint({ author, currentUserEmail }: { author?: string; currentUserEmail?: string }) {
  const { t } = useTranslation();
  const isAuthor =
    !!author && !!currentUserEmail && author.toLowerCase() === currentUserEmail.toLowerCase();
  return (
    <span className="text-[11px] text-muted-foreground italic">
      {isAuthor ? t("rulesDrafts.registryYourSubmission") : t("rulesDrafts.registryReadOnly")}
    </span>
  );
}
