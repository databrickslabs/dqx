import { createFileRoute, useNavigate, useSearch } from "@tanstack/react-router";
import { useCallback, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { Braces, MoreVertical } from "lucide-react";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import { FadeIn } from "@/components/anim/FadeIn";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { getListRegistryRulesQueryKey } from "@/lib/api";
import { useLabelDefinitions } from "@/lib/api-custom";
import { useUnsavedGuard } from "@/hooks/use-unsaved-guard";
import {
  RegistryRuleFormDialog,
  type PageTab,
} from "@/components/RegistryRuleFormDialog";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/_sidebar/registry-rules/new")({
  validateSearch: (search: Record<string, unknown>): { tab?: string } => ({
    tab: typeof search.tab === "string" ? search.tab : undefined,
  }),
  component: RegistryRuleCreatePage,
});

function RegistryRuleCreatePage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { tab } = useSearch({ from: "/_sidebar/registry-rules/new" });

  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = useMemo(() => labelDefsData?.definitions ?? [], [labelDefsData]);

  const [isDirty, setIsDirty] = useState(false);
  // "As JSON" edit-in-place dialog — opened from the ⋮ overflow menu below and
  // applied straight into the form (B2-114). Drives RegistryRuleFormDialog's
  // controlled `jsonDialogOpen` prop, mirroring the existing-rule detail page.
  const [formJsonDialogOpen, setFormJsonDialogOpen] = useState(false);
  // Set right before a successful save navigates us away, so the guard
  // doesn't fire a spurious "unsaved changes" prompt on our own redirect.
  const justSavedRef = useRef(false);
  const { blocker } = useUnsavedGuard({ hasUnsavedChanges: isDirty, bypassRef: justSavedRef });

  const backToList = useCallback(() => navigate({ to: "/registry-rules" }), [navigate]);

  const handleActiveTabChange = useCallback(
    (nextTab: PageTab) => {
      navigate({
        to: "/registry-rules/new",
        search: (prev) => ({ ...prev, tab: nextTab }),
      });
    },
    [navigate],
  );

  const handleSaved = useCallback(
    (ruleId?: string) => {
      justSavedRef.current = true;
      queryClient.invalidateQueries({ queryKey: getListRegistryRulesQueryKey() });
      if (ruleId) {
        navigate({ to: "/registry-rules/$ruleId", params: { ruleId } });
      } else {
        backToList();
      }
    },
    [queryClient, navigate, backToList],
  );

  // Page title, passed to the form as `headerTitle` so it renders on the LEFT
  // of the same top-right header row as the Save/Submit actions — matching the
  // existing-rule detail page and the Monitored Table / Table Space headers
  // (B2-112). The rule's own name is an input the user types on the About tab;
  // this heading anchors the row so the actions sit inline with it instead of
  // floating alone against an empty left slot.
  const headerTitle = (
    <div className="flex flex-wrap items-center gap-2 min-w-0">
      <h1 className="text-2xl font-semibold tracking-tight leading-none truncate">
        {t("rulesRegistry.createTitle")}
      </h1>
    </div>
  );

  // ⋮ overflow menu, passed DOWN into the form's page-variant header so it
  // renders inline, immediately after the Save/Submit buttons, in ONE
  // top-right action row — mirroring the existing-rule detail page's header
  // (B2-114). "As JSON" now lives here rather than as a standalone button on
  // the Implementation tab.
  const headerActions = (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="ghost"
          size="sm"
          className="h-8 w-8 p-0"
          aria-label={t("rulesRegistry.actionsMenuLabel")}
        >
          <MoreVertical className="h-4 w-4" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onClick={() => setFormJsonDialogOpen(true)} className="gap-2">
          <Braces className="h-3.5 w-3.5" />
          {t("rulesRegistry.actionAsJson")}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );

  return (
    <FadeIn>
      <div className="space-y-6">
        <PageBreadcrumb items={[{ label: t("rulesRegistry.title"), to: "/registry-rules" }]} page={t("rulesRegistry.newRule")} />

        <RegistryRuleFormDialog
          variant="page"
          open
          onOpenChange={(next) => {
            // A successful save already redirected us (to the list, or to
            // the newly created rule's detail page, via `handleSaved`) —
            // don't let the dialog's own close-on-save also fire a second,
            // competing navigation back to the list.
            if (!next && !justSavedRef.current) backToList();
          }}
          editingRule={null}
          viewingRule={null}
          labelDefinitions={labelDefinitions}
          onSaved={handleSaved}
          activeTab={tab as PageTab | undefined}
          onActiveTabChange={handleActiveTabChange}
          onDirtyChange={setIsDirty}
          jsonDialogOpen={formJsonDialogOpen}
          onJsonDialogOpenChange={setFormJsonDialogOpen}
          headerActions={headerActions}
          headerTitle={headerTitle}
        />
      </div>

      <AlertDialog open={blocker.status === "blocked"}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("common.unsavedChanges")}</AlertDialogTitle>
            <AlertDialogDescription>{t("rulesRegistry.unsavedChangesDescription")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => blocker.reset?.()}>{t("common.stayOnPage")}</AlertDialogCancel>
            <AlertDialogAction
              className={cn("bg-destructive text-white hover:bg-destructive/90")}
              onClick={() => blocker.proceed?.()}
            >
              {t("rulesRegistry.discardAndLeave")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </FadeIn>
  );
}
