/**
 * PermissionsTab — a Unity-Catalog-style permissions surface reused by the
 * three detail pages (registry rule, monitored table, table space).
 *
 * Renders:
 *  - an optional Steward section (a `PrincipalPicker` that stores the picked
 *    principal's display name), when `showSteward` is set;
 *  - the workspace users-group default grant (SELECT + APPLY), rendered like
 *    any grant, muted, and editable by grant-managers (narrow or revoke it);
 *  - the owner/creator default grant (ALL PRIVILEGES), rendered muted but
 *    editable/revocable by grant-managers (narrow it or revoke it entirely);
 *    the workspace admin/approver backstop keeps the object manageable;
 *  - the direct + inherited grants, inherited rows shown muted and locked;
 *  - a "Grant permission" control (owners/admins/approvers only) that opens a
 *    dialog with a principal picker, privilege checkboxes, and (except for
 *    registry rules — see below) an inherit toggle.
 *
 * Grants require a saved object id — they key on `object_id`, which doesn't
 * exist until the object is first saved. When `objectId` is empty (e.g. a
 * rule/table/space still being created), the Permissions section renders an
 * empty shell with a "save first" message instead of the grants table.
 *
 * Registry rules sit at the bottom of the object hierarchy — there's
 * nothing beneath a rule to inherit a grant to — so the inherit toggle in the
 * add/edit dialog is omitted for `objectType === "registry_rule"`. The
 * backend still accepts the `inherit` field for rules; this is a UI-only
 * omission. The "Inheritance" column has been removed from the table entirely
 * for all object types; the inherit toggle in the dialog is kept.
 */
import { useLayoutEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { HelpTooltip } from "@/components/HelpTooltip";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import {
  Loader2,
  Pencil,
  Plus,
  Trash2,
  User as UserIcon,
  Users,
} from "lucide-react";
import {
  useListObjectGrants,
  useSetObjectGrant,
  useRemoveObjectGrant,
  getListObjectGrantsQueryKey,
  type ObjectGrantsOut,
  type ObjectGrantOut,
} from "@/lib/api";
import { PrincipalPicker, type PickedPrincipal } from "@/components/permissions/PrincipalPicker";
import { cn } from "@/lib/utils";
import {
  PRIV_SELECT,
  PRIV_MODIFY,
  PRIV_APPLY,
  PRIV_EXECUTE,
  PRIV_ALL,
  isUsersGroupGrant,
  isOwnerDefaultGrant,
  isAllPrivileges,
  initialGrantInherit,
  privilegeTagLabel,
  grantsEmptyColSpan,
  hasSavedObject,
  forceSelectWhenOthers,
} from "@/components/permissions/permissions-utils";

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

interface Props {
  objectType: "registry_rule" | "monitored_table" | "data_product";
  objectId: string;
  showSteward?: boolean;
  steward?: string;
  onStewardChange?: (name: string) => void;
  canEditSteward?: boolean;
  stewardSuggestion?: { displayName: string; onPick: () => void } | null;
}

// Privilege tags render as the canonical Unity-Catalog-style grant
// keywords (SELECT, MODIFY, APPLY, ALL PRIVILEGES) — full caps, monospaced
// — rather than a humanized paraphrase, matching how grants render
// everywhere else in the platform. Not translated: these are grant
// keywords, not prose.
const PRIVILEGE_TAG_CLASS = "font-mono text-[10px] uppercase tracking-wide";

function PrivilegeBadges({ privileges }: { privileges: string[] }) {
  if (privileges.length === 0) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  if (isAllPrivileges(privileges)) {
    return (
      <Badge variant="secondary" className={PRIVILEGE_TAG_CLASS}>
        {privilegeTagLabel(PRIV_ALL)}
      </Badge>
    );
  }
  return (
    <div className="flex flex-wrap gap-1">
      {privileges.map((p) => (
        <Badge key={p} variant="outline" className={PRIVILEGE_TAG_CLASS}>
          {privilegeTagLabel(p)}
        </Badge>
      ))}
    </div>
  );
}

function PrincipalCell({
  name,
  type,
  muted,
}: {
  name: string;
  type: string;
  muted?: boolean;
}) {
  const { t } = useTranslation();
  const isGroup = type === "group";
  return (
    <div className="flex items-center gap-2">
      {isGroup ? (
        <Users className={cn("h-4 w-4 shrink-0", muted ? "text-muted-foreground/60" : "text-muted-foreground")} />
      ) : (
        <UserIcon className={cn("h-4 w-4 shrink-0", muted ? "text-muted-foreground/60" : "text-muted-foreground")} />
      )}
      <span className="truncate" title={name}>
        {name}
      </span>
      <Badge variant="outline" className="shrink-0 text-[10px] capitalize">
        {isGroup ? t("permissions.group") : t("permissions.user")}
      </Badge>
    </div>
  );
}

interface GrantDraft {
  principal: PickedPrincipal | null;
  view: boolean;
  modify: boolean;
  apply: boolean;
  execute: boolean;
  inherit: boolean;
}

function GrantDialog({
  open,
  onOpenChange,
  editing,
  objectType,
  defaultInherit,
  showInherit,
  saving,
  onSave,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  editing: ObjectGrantOut | null;
  objectType: Props["objectType"];
  defaultInherit: boolean;
  showInherit: boolean;
  saving: boolean;
  onSave: (draft: GrantDraft) => void;
}) {
  // The users-group row may be saved with no privileges — that revokes the
  // day-one default (the per-object "revoked" marker). Any other principal
  // must keep at least one privilege.
  const allowEmpty = !!editing && isUsersGroupGrant(editing);
  const { t } = useTranslation();
  // Inheritance flows down the object hierarchy, so what "inherit" grants
  // access to differs by object type: a monitored table pushes access to its
  // underlying rules; a table space pushes it to its member tables and their
  // rules. Registry rules never show this toggle (nothing beneath them).
  const inheritHintKey =
    objectType === "monitored_table"
      ? "permissions.inheritToggleHintMonitoredTable"
      : objectType === "data_product"
        ? "permissions.inheritToggleHintTableSpace"
        : "permissions.inheritToggleHint";
  const [draft, setDraft] = useState<GrantDraft>({
    principal: null,
    view: true,
    modify: false,
    apply: false,
    execute: false,
    inherit: defaultInherit,
  });

  // Seed the draft whenever the dialog opens (create vs edit an existing grant).
  // The draft state persists across opens, so a new-grant dialog would briefly
  // paint the previous/stale toggle state before an effect could reset it.
  // `useLayoutEffect` reseeds synchronously *before* paint, so the "inherit to
  // child objects" toggle shows the admin `permissions_default_inherit` default
  // the instant the dialog appears (B2-122) — no transient off-state flash.
  useLayoutEffect(() => {
    if (!open) return;
    if (editing) {
      const privs = editing.privileges ?? [];
      const all = isAllPrivileges(privs);
      setDraft({
        principal: {
          principal_id: editing.principal_id,
          principal_type: editing.principal_type,
          principal_name: editing.principal_name ?? editing.principal_id,
        },
        view: all || privs.includes(PRIV_SELECT),
        modify: all || privs.includes(PRIV_MODIFY),
        apply: all || privs.includes(PRIV_APPLY),
        execute: all || privs.includes(PRIV_EXECUTE),
        inherit: initialGrantInherit(editing, defaultInherit),
      });
    } else {
      setDraft({
        principal: null,
        view: true,
        modify: false,
        apply: false,
        execute: false,
        inherit: initialGrantInherit(null, defaultInherit),
      });
    }
  }, [open, editing, defaultInherit]);

  const noPrivileges = !draft.view && !draft.modify && !draft.apply && !draft.execute;
  const lockSelect = draft.modify || draft.apply || draft.execute;
  const canSave = !!draft.principal && (!noPrivileges || allowEmpty) && !saving;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            {editing ? t("permissions.editPermission") : t("permissions.grantPermission")}
          </DialogTitle>
        </DialogHeader>
        <div className="space-y-4">
          <div className="space-y-1.5">
            <Label>{t("permissions.principal")}</Label>
            <PrincipalPicker
              value={draft.principal}
              disabled={!!editing}
              onSelect={(p) =>
                setDraft((d) => ({
                  ...d,
                  principal: {
                    principal_id: p.workspace_principal_id,
                    principal_type: p.kind,
                    principal_name: p.display_name,
                  },
                }))
              }
              onClear={() => setDraft((d) => ({ ...d, principal: null }))}
              className="w-full"
            />
          </div>

          <div className="space-y-2">
            <Label>{t("permissions.privileges")}</Label>
            {/* Grid columns (checkbox · privilege keyword · hint) keep every
                hint on a shared left edge regardless of keyword width, and the
                hints are non-selectable so drag-selecting text in the dialog
                doesn't highlight the explanatory copy. */}
            <div className="space-y-2">
              <label className="grid grid-cols-[auto_5rem_1fr] items-center gap-2 text-sm">
                <Checkbox
                  checked={draft.view}
                  disabled={lockSelect}
                  onCheckedChange={(c) => {
                    if (lockSelect) return;
                    setDraft((d) => ({ ...d, view: c === true }));
                  }}
                />
                <span className={PRIVILEGE_TAG_CLASS}>{privilegeTagLabel(PRIV_SELECT)}</span>
                <span className="select-none text-xs text-muted-foreground">{t("permissions.viewHint")}</span>
              </label>
              <label className="grid grid-cols-[auto_5rem_1fr] items-center gap-2 text-sm">
                <Checkbox
                  checked={draft.modify}
                  onCheckedChange={(c) =>
                    setDraft((d) => forceSelectWhenOthers({ ...d, modify: c === true }))
                  }
                />
                <span className={PRIVILEGE_TAG_CLASS}>{privilegeTagLabel(PRIV_MODIFY)}</span>
                <span className="select-none text-xs text-muted-foreground">{t("permissions.modifyHint")}</span>
              </label>
              <label className="grid grid-cols-[auto_5rem_1fr] items-center gap-2 text-sm">
                <Checkbox
                  checked={draft.apply}
                  onCheckedChange={(c) =>
                    setDraft((d) => forceSelectWhenOthers({ ...d, apply: c === true }))
                  }
                />
                <span className={PRIVILEGE_TAG_CLASS}>{privilegeTagLabel(PRIV_APPLY)}</span>
                <span className="select-none text-xs text-muted-foreground">{t("permissions.applyHint")}</span>
              </label>
              {objectType !== "registry_rule" && (
                <label className="grid grid-cols-[auto_5rem_1fr] items-center gap-2 text-sm">
                  <Checkbox
                    checked={draft.execute}
                    onCheckedChange={(c) =>
                      setDraft((d) => forceSelectWhenOthers({ ...d, execute: c === true }))
                    }
                  />
                  <span className={PRIVILEGE_TAG_CLASS}>{privilegeTagLabel(PRIV_EXECUTE)}</span>
                  <span className="select-none text-xs text-muted-foreground">{t("permissions.executeHint")}</span>
                </label>
              )}
            </div>
          </div>

          {showInherit && (
            <div className="flex items-center justify-between rounded-md border p-3">
              <div className="space-y-0.5 pr-4">
                <Label htmlFor="grant-inherit" className="text-sm">
                  {t("permissions.inheritToggleLabel")}
                </Label>
                <p className="text-[11px] text-muted-foreground">{t(inheritHintKey)}</p>
              </div>
              <Switch
                id="grant-inherit"
                checked={draft.inherit}
                onCheckedChange={(c) => setDraft((d) => ({ ...d, inherit: c }))}
              />
            </div>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={saving}>
            {t("permissions.cancel")}
          </Button>
          <Button onClick={() => onSave(draft)} disabled={!canSave}>
            {saving && <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />}
            {t("permissions.save")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export function PermissionsTab({
  objectType,
  objectId,
  showSteward = false,
  steward = "",
  onStewardChange,
  canEditSteward = false,
  stewardSuggestion = null,
}: Props) {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const hasObject = hasSavedObject(objectId);
  // Rules are the bottom of the object hierarchy — there's nothing beneath
  // them to inherit grants to, so the inherit toggle in the add/edit dialog
  // is omitted for registry rules. The backend still accepts the `inherit`
  // field on this object type; only the UI hides the toggle here.
  const isRule = objectType === "registry_rule";
  const emptyStateKey =
    objectType === "registry_rule"
      ? "permissions.emptyStateRule"
      : objectType === "monitored_table"
        ? "permissions.emptyStateTable"
        : "permissions.emptyStateTableSpace";

  const [dialogOpen, setDialogOpen] = useState(false);
  const [editing, setEditing] = useState<ObjectGrantOut | null>(null);
  const [removingId, setRemovingId] = useState<string | null>(null);

  const { data: grantsData, isLoading } = useListObjectGrants(objectType, objectId, {
    query: { select: (d) => d.data, enabled: hasObject },
  });

  const setMut = useSetObjectGrant({ mutation: { onError: () => {} } });
  const removeMut = useRemoveObjectGrant({ mutation: { onError: () => {} } });

  const invalidate = () =>
    qc.invalidateQueries({ queryKey: getListObjectGrantsQueryKey(objectType, objectId) });

  const data: ObjectGrantsOut | undefined = grantsData;
  const grants = data?.grants ?? [];
  const canManage = data?.can_manage ?? false;
  const defaultInherit = data?.default_inherit ?? false;

  const openAdd = () => {
    setEditing(null);
    setDialogOpen(true);
  };
  const openEdit = (grant: ObjectGrantOut) => {
    setEditing(grant);
    setDialogOpen(true);
  };

  const handleSave = async (draft: GrantDraft) => {
    if (!draft.principal) return;
    const privileges: string[] = [];
    if (draft.view) privileges.push(PRIV_SELECT);
    if (draft.modify) privileges.push(PRIV_MODIFY);
    if (draft.apply) privileges.push(PRIV_APPLY);
    if (draft.execute) privileges.push(PRIV_EXECUTE);
    try {
      await setMut.mutateAsync({
        objectType,
        objectId,
        data: {
          principal_id: draft.principal.principal_id,
          principal_type: draft.principal.principal_type,
          principal_name: draft.principal.principal_name,
          privileges,
          inherit: draft.inherit,
        },
      });
      invalidate();
      setDialogOpen(false);
      toast.success(t("permissions.grantSaved"));
    } catch (e) {
      toast.error(extractApiError(e, t("permissions.grantSaveFailed")), { duration: 6000 });
    }
  };

  const handleRemove = async (grant: ObjectGrantOut) => {
    setRemovingId(grant.principal_id);
    try {
      await removeMut.mutateAsync({ objectType, objectId, principalId: grant.principal_id });
      invalidate();
      toast.success(t("permissions.grantRemoved"));
    } catch (e) {
      toast.error(extractApiError(e, t("permissions.grantRemoveFailed")), { duration: 6000 });
    } finally {
      setRemovingId(null);
    }
  };

  const stewardValue: PickedPrincipal | null = steward
    ? { principal_id: "", principal_type: "user", principal_name: steward }
    : null;

  return (
    <div className="space-y-6 max-w-3xl">
      {showSteward && (
        <section className="flex flex-col gap-3">
          <div className="flex items-center gap-1.5">
            <p className="text-sm font-medium leading-none">{t("permissions.ownerLabel")}</p>
            <HelpTooltip text={t("permissions.ownerHelp")} />
          </div>
          {canEditSteward && onStewardChange ? (
            <PrincipalPicker
              value={stewardValue}
              suggestion={stewardSuggestion}
              onSelect={(p) => onStewardChange(p.display_name)}
              onClear={() => onStewardChange("")}
            />
          ) : steward ? (
            <p className="text-sm">{steward}</p>
          ) : (
            <p className="text-sm text-muted-foreground italic">{t("permissions.ownerNone")}</p>
          )}
        </section>
      )}

      {hasObject ? (
        <section className="space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-1.5">
              <p className="text-sm font-medium leading-none">{t("permissions.title")}</p>
            </div>
            {canManage && (
              <Button size="sm" onClick={openAdd} className="gap-1.5">
                <Plus className="h-3.5 w-3.5" />
                {t("permissions.grantPermission")}
              </Button>
            )}
          </div>

          {isLoading ? (
            <div className="space-y-2">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
            </div>
          ) : (
            <div className="rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>{t("permissions.principal")}</TableHead>
                    <TableHead>{t("permissions.privileges")}</TableHead>
                    <TableHead>{t("permissions.grantedBy")}</TableHead>
                    {canManage && <TableHead className="w-[80px] text-right" />}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {grants.length === 0 ? (
                    <TableRow>
                      <TableCell
                        colSpan={grantsEmptyColSpan(isRule, canManage)}
                        className="text-center text-sm text-muted-foreground py-6"
                      >
                        {t("permissions.noGrants")}
                      </TableCell>
                    </TableRow>
                  ) : (
                    grants.map((grant) => {
                      const inherited = grant.inherited ?? false;
                      // The workspace users-group default is rendered like any
                      // grant, muted, labelled, and editable by grant-managers
                      // (edit it to narrow SELECT/APPLY or revoke entirely).
                      const usersGroup = isUsersGroupGrant(grant);
                      // The owner/creator default is also muted and labelled but,
                      // like the users-group default, is editable/revocable by
                      // grant-managers — revoking materializes an explicit marker
                      // that overrides the creator's implicit ALL PRIVILEGES.
                      const ownerDefault = isOwnerDefaultGrant(grant);
                      const isDefault = grant.is_default ?? false;
                      const muted = inherited || usersGroup || ownerDefault;
                      const name = usersGroup
                        ? t("permissions.allUsers")
                        : (grant.principal_name ?? grant.principal_id);
                      return (
                        <TableRow
                          key={grant.principal_id}
                          className={cn(inherited && "opacity-60", (usersGroup || ownerDefault) && "bg-muted/30")}
                        >
                          <TableCell>
                            <div className="flex items-center gap-2">
                              <PrincipalCell name={name} type={grant.principal_type} muted={muted} />
                              {isDefault && (
                                <>
                                  <span className="text-xs text-muted-foreground">
                                    {ownerDefault ? t("permissions.ownerNote") : t("permissions.defaultNote")}
                                  </span>
                                  <HelpTooltip
                                    text={
                                      ownerDefault
                                        ? t("permissions.ownerGrantHint")
                                        : t("permissions.defaultGrantHint")
                                    }
                                  />
                                </>
                              )}
                            </div>
                          </TableCell>
                          <TableCell>
                            <PrivilegeBadges privileges={grant.privileges ?? []} />
                          </TableCell>
                          <TableCell>
                            <span className="text-xs text-muted-foreground">
                              {isDefault ? t("permissions.grantedBySystem") : (grant.grantor ?? "—")}
                            </span>
                          </TableCell>
                          {canManage && (
                            <TableCell className="text-right">
                              {/* Inherited rows are managed on the parent object —
                                  read-only here. Every other row (direct grants,
                                  the users-group default, and the owner row) is
                                  editable and deletable. Owners can be removed too;
                                  workspace admins/approvers keep access regardless,
                                  so the object can't be orphaned. */}
                              {!inherited && (
                                <div className="flex items-center justify-end gap-1">
                                  <Button
                                    variant="ghost"
                                    size="icon"
                                    className="h-7 w-7"
                                    onClick={() => openEdit(grant)}
                                    aria-label={t("permissions.editPermission")}
                                  >
                                    <Pencil className="h-3.5 w-3.5" />
                                  </Button>
                                  <Tooltip>
                                    <TooltipTrigger asChild>
                                      <Button
                                        variant="ghost"
                                        size="icon"
                                        className="h-7 w-7 text-destructive hover:text-destructive"
                                        onClick={() => handleRemove(grant)}
                                        disabled={removingId === grant.principal_id}
                                        aria-label={t("permissions.revokePermission")}
                                      >
                                        {removingId === grant.principal_id ? (
                                          <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                        ) : (
                                          <Trash2 className="h-3.5 w-3.5" />
                                        )}
                                      </Button>
                                    </TooltipTrigger>
                                    <TooltipContent>{t("permissions.revokePermission")}</TooltipContent>
                                  </Tooltip>
                                </div>
                              )}
                            </TableCell>
                          )}
                        </TableRow>
                      );
                    })
                  )}
                </TableBody>
              </Table>
            </div>
          )}
        </section>
      ) : (
        <section className="space-y-3">
          <div className="flex items-center gap-1.5">
            <p className="text-sm font-medium leading-none">{t("permissions.title")}</p>
          </div>
          <div className="rounded-md border border-dashed py-10 text-center text-sm text-muted-foreground">
            {t(emptyStateKey)}
          </div>
        </section>
      )}

      <GrantDialog
        open={dialogOpen}
        onOpenChange={setDialogOpen}
        editing={editing}
        objectType={objectType}
        defaultInherit={defaultInherit}
        showInherit={!isRule}
        saving={setMut.isPending}
        onSave={handleSave}
      />
    </div>
  );
}
