import { useTranslation } from "react-i18next";
import { Check, ChevronLeft, ChevronRight, MoreHorizontal, RotateCcw, Star } from "lucide-react";
import type { SavedViewOut } from "@/lib/api";
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
import * as DropdownMenuPrimitive from "@radix-ui/react-dropdown-menu";
import {
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuPortal,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";
import { RenameViewDialog } from "./RenameViewDialog";
import { DisabledReason, SaveViewDialog } from "./SaveViewDialog";
import { TruncatedLabel } from "./TruncatedLabel";
import { myDefaultTarget, rowActions, saveDisabledReason, type ActionState } from "./saved-views";
import type { SavedViewsMenuConfig } from "./useSavedViews";

/** A dialog the Saved Views page asks its host to open (dialogs live outside the menu). */
export type SavedViewsDialogRequest =
  | { kind: "saveAsNew" }
  | { kind: "rename"; view: SavedViewOut }
  | { kind: "delete"; view: SavedViewOut };

/** Failures are already reported with a toast by the hook. */
const ignoreFailure = () => undefined;

function UnsavedDot() {
  const { t } = useTranslation();
  return (
    <span
      role="img"
      aria-label={t("savedViews.unsavedChanges")}
      className="h-1.5 w-1.5 shrink-0 rounded-full bg-amber-500"
    />
  );
}

/** The "Saved Views ›" row at the top of the Edit View menu. */
export function SavedViewsEntry({ config, onOpen }: { config: SavedViewsMenuConfig; onOpen: () => void }) {
  const { t } = useTranslation();
  return (
    <>
      <DropdownMenuItem
        className="gap-1 text-xs font-medium"
        onSelect={(e) => {
          e.preventDefault();
          onOpen();
        }}
      >
        <span className="shrink-0">{t("savedViews.title")}</span>
        <span className="ml-auto flex min-w-0 items-center gap-1.5 font-normal text-muted-foreground">
          {config.activeView && <TruncatedLabel text={config.activeView.name} className="min-w-0" />}
          {config.dirty && <UnsavedDot />}
        </span>
        <ChevronRight className="text-muted-foreground" />
      </DropdownMenuItem>
      <DropdownMenuSeparator />
    </>
  );
}

function ActionItem({
  state,
  label,
  destructive,
  onSelect,
}: {
  state: ActionState;
  label: string;
  destructive?: boolean;
  onSelect: () => void;
}) {
  const { t } = useTranslation();
  return (
    <DisabledReason reason={state.reasonKey ? t(state.reasonKey) : null} className="block">
      <DropdownMenuItem
        // aria-disabled (not disabled) keeps the item in the arrow-key order, so the
        // reason tooltip opens on keyboard focus too; onSelect ignores the pick.
        aria-disabled={!state.enabled || undefined}
        variant={destructive ? "destructive" : "default"}
        className="text-xs"
        onSelect={(e) => (state.enabled ? onSelect() : e.preventDefault())}
      >
        {label}
      </DropdownMenuItem>
    </DisabledReason>
  );
}

function ViewRow({
  view,
  config,
  onRequest,
}: {
  view: SavedViewOut;
  config: SavedViewsMenuConfig;
  onRequest: (request: SavedViewsDialogRequest) => void;
}) {
  const { t } = useTranslation();
  const active = config.activeView?.id === view.id;
  const actions = rowActions(view, config.isAdmin, config.effectiveDefaultId);
  return (
    <div className={cn("flex items-center rounded-sm", active && "bg-muted")}>
      <DropdownMenuItem
        className="min-w-0 flex-1 gap-2 text-xs"
        onSelect={(e) => {
          e.preventDefault();
          config.switchTo(view.id);
        }}
      >
        <span className="flex h-4 w-4 shrink-0 items-center justify-center">
          {active && <Check className="h-3.5 w-3.5" />}
        </span>
        <TruncatedLabel text={view.name} className="min-w-0" />
        {active && config.dirty && <UnsavedDot />}
      </DropdownMenuItem>
      <DropdownMenuItem
        aria-label={view.starred ? t("savedViews.unstar") : t("savedViews.star")}
        className="shrink-0 px-1.5"
        onSelect={(e) => {
          e.preventDefault();
          config.setStar(view.id, !view.starred).catch(ignoreFailure);
        }}
      >
        <Star className={cn(view.starred ? "fill-amber-500 text-amber-500" : "text-muted-foreground/60")} />
      </DropdownMenuItem>
      <DropdownMenuSub>
        <DropdownMenuPrimitive.SubTrigger
          aria-label={t("savedViews.rowActions")}
          className="flex shrink-0 cursor-default select-none items-center rounded-sm px-1.5 py-1.5 text-muted-foreground outline-none focus:bg-accent focus:text-foreground data-[state=open]:bg-accent data-[state=open]:text-foreground"
        >
          <MoreHorizontal className="h-4 w-4" />
        </DropdownMenuPrimitive.SubTrigger>
        <DropdownMenuPortal>
          <DropdownMenuSubContent className="min-w-44">
            {actions.rename && (
              <ActionItem
                state={actions.rename}
                label={t("savedViews.rename")}
                onSelect={() => onRequest({ kind: "rename", view })}
              />
            )}
            <ActionItem
              state={actions.setMyDefault}
              label={t("savedViews.setMyDefault")}
              onSelect={() => {
                config.setMyDefault(myDefaultTarget(view)).catch(ignoreFailure);
              }}
            />
            {actions.setOrgDefault && (
              <ActionItem
                state={actions.setOrgDefault}
                label={t("savedViews.setOrgDefault")}
                onSelect={() => {
                  config.setOrgDefault(view.id).catch(ignoreFailure);
                }}
              />
            )}
            {actions.share && (
              <ActionItem
                state={actions.share}
                label={t("savedViews.share")}
                onSelect={() => {
                  config.share(view.id).catch(ignoreFailure);
                }}
              />
            )}
            {actions.delete && (
              <>
                <DropdownMenuSeparator />
                <ActionItem
                  state={actions.delete}
                  label={t("savedViews.delete")}
                  destructive
                  onSelect={() => onRequest({ kind: "delete", view })}
                />
              </>
            )}
          </DropdownMenuSubContent>
        </DropdownMenuPortal>
      </DropdownMenuSub>
    </div>
  );
}

function Section({
  heading,
  views,
  config,
  onRequest,
}: {
  heading: string;
  views: SavedViewOut[];
  config: SavedViewsMenuConfig;
  onRequest: (request: SavedViewsDialogRequest) => void;
}) {
  if (views.length === 0) return null;
  return (
    <div role="group" aria-label={heading}>
      <DropdownMenuLabel className="px-2 pb-0.5 pt-1.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
        {heading}
      </DropdownMenuLabel>
      {views.map((view) => (
        <ViewRow key={view.id} view={view} config={config} onRequest={onRequest} />
      ))}
    </div>
  );
}

export interface SavedViewsPanelProps {
  config: SavedViewsMenuConfig;
  onBack: () => void;
  onRequest: (request: SavedViewsDialogRequest) => void;
}

/** The drill-in Saved Views page of the Edit View menu. */
export function SavedViewsPanel({ config, onBack, onRequest }: SavedViewsPanelProps) {
  const { t } = useTranslation();
  const save = saveDisabledReason(config.activeView, config.dirty);
  return (
    <>
      <DropdownMenuItem
        className="gap-1 text-xs font-medium"
        onSelect={(e) => {
          e.preventDefault();
          onBack();
        }}
      >
        <ChevronLeft />
        {t("savedViews.back")}
      </DropdownMenuItem>
      <DropdownMenuSeparator />
      {config.loading ? (
        <p className="px-2 py-1.5 text-xs text-muted-foreground">{t("common.loading")}</p>
      ) : (
        <div className="max-h-80 overflow-y-auto">
          <Section heading={t("savedViews.myViews")} views={config.sections.mine} config={config} onRequest={onRequest} />
          <Section
            heading={t("savedViews.organisation")}
            views={config.sections.org}
            config={config}
            onRequest={onRequest}
          />
        </div>
      )}
      <DropdownMenuSeparator />
      {/* Footer actions are menu items (styled as buttons) so they stay reachable with the arrow keys. */}
      <div className="flex items-center gap-1.5 px-0.5 pb-0.5 pt-1">
        <DropdownMenuItem
          disabled={!config.dirty}
          className="h-7 gap-1.5 px-2 text-xs [&>svg]:h-3.5 [&>svg]:w-3.5"
          onSelect={(e) => {
            e.preventDefault();
            config.reset();
          }}
        >
          <RotateCcw />
          {t("savedViews.reset")}
        </DropdownMenuItem>
        <span className="flex-1" />
        <DropdownMenuItem
          disabled={config.loading}
          className="h-7 rounded-md border bg-background px-2 text-xs shadow-xs"
          onSelect={() => onRequest({ kind: "saveAsNew" })}
        >
          {t("savedViews.saveAsNew")}
        </DropdownMenuItem>
        <DisabledReason reason={save.reasonKey ? t(save.reasonKey) : null} className="inline-flex">
          <DropdownMenuItem
            aria-disabled={save.disabled || undefined}
            className="h-7 rounded-md bg-primary px-2.5 text-xs font-medium text-primary-foreground focus:bg-primary/90 focus:text-primary-foreground"
            onSelect={(e) => {
              e.preventDefault();
              if (!save.disabled) config.save().catch(ignoreFailure);
            }}
          >
            {t("savedViews.save")}
          </DropdownMenuItem>
        </DisabledReason>
      </div>
    </>
  );
}

export interface SavedViewsDialogsProps {
  config: SavedViewsMenuConfig;
  request: SavedViewsDialogRequest | null;
  onClose: () => void;
}

/** Save as new, Rename and Delete dialogs for the Saved Views page. */
export function SavedViewsDialogs({ config, request, onClose }: SavedViewsDialogsProps) {
  const { t } = useTranslation();
  const views = [...config.sections.mine, ...config.sections.org];
  const deleting = request?.kind === "delete" ? request.view : null;
  return (
    <>
      <SaveViewDialog
        open={request?.kind === "saveAsNew"}
        onOpenChange={(open) => !open && onClose()}
        isAdmin={config.isAdmin}
        views={views}
        onSave={config.saveAsNew}
      />
      <RenameViewDialog
        view={request?.kind === "rename" ? request.view : null}
        views={views}
        onClose={onClose}
        onRename={config.rename}
      />
      <AlertDialog open={deleting !== null} onOpenChange={(open) => !open && onClose()}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("savedViews.deleteTitle")}</AlertDialogTitle>
            <AlertDialogDescription>
              {deleting ? t("savedViews.deleteConfirm", { name: deleting.name }) : null}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => {
                if (deleting) config.remove(deleting.id).catch(ignoreFailure);
              }}
            >
              {t("common.delete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}
