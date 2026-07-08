/**
 * PermissionsTab — a Unity-Catalog-style permissions surface reused by the
 * three detail pages (registry rule, monitored table, table space).
 *
 * Renders:
 *  - an optional Steward section (a `PrincipalPicker` that stores the picked
 *    principal's display name), when `showSteward` is set;
 *  - a read-only "All users" baseline row;
 *  - the direct + inherited grants, inherited rows shown muted and locked;
 *  - an "Add permission" control (owners/admins/approvers only) that opens a
 *    dialog with a principal picker, privilege checkboxes, and an inherit
 *    toggle.
 *
 * Grants require a saved object id; when `objectId` is empty (e.g. a rule
 * still being created) only the Steward section renders.
 */
import { useEffect, useState } from "react";
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
import {
  KeyRound,
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

const PRIV_SELECT = "SELECT";
const PRIV_MODIFY = "MODIFY";
const PRIV_APPLY = "APPLY";
const PRIV_ALL = "ALL_PRIVILEGES";

function isAllPrivileges(privileges: string[]): boolean {
  if (privileges.includes(PRIV_ALL)) return true;
  return [PRIV_SELECT, PRIV_MODIFY, PRIV_APPLY].every((p) => privileges.includes(p));
}

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

function PrivilegeBadges({ privileges }: { privileges: string[] }) {
  const { t } = useTranslation();
  if (privileges.length === 0) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  if (isAllPrivileges(privileges)) {
    return <Badge variant="secondary">{t("permissions.allPrivileges")}</Badge>;
  }
  const label = (p: string): string => {
    switch (p) {
      case PRIV_SELECT:
        return t("permissions.view");
      case PRIV_MODIFY:
        return t("permissions.modify");
      case PRIV_APPLY:
        return t("permissions.apply");
      default:
        return p;
    }
  };
  return (
    <div className="flex flex-wrap gap-1">
      {privileges.map((p) => (
        <Badge key={p} variant="outline" className="text-xs">
          {label(p)}
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
  inherit: boolean;
}

function GrantDialog({
  open,
  onOpenChange,
  editing,
  defaultInherit,
  saving,
  onSave,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  editing: ObjectGrantOut | null;
  defaultInherit: boolean;
  saving: boolean;
  onSave: (draft: GrantDraft) => void;
}) {
  const { t } = useTranslation();
  const [draft, setDraft] = useState<GrantDraft>({
    principal: null,
    view: true,
    modify: false,
    apply: false,
    inherit: defaultInherit,
  });

  // Seed the draft whenever the dialog opens (create vs edit an existing grant).
  useEffect(() => {
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
        inherit: editing.inherit ?? defaultInherit,
      });
    } else {
      setDraft({ principal: null, view: true, modify: false, apply: false, inherit: defaultInherit });
    }
  }, [open, editing, defaultInherit]);

  const noPrivileges = !draft.view && !draft.modify && !draft.apply;
  const canSave = !!draft.principal && !noPrivileges && !saving;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            {editing ? t("permissions.editPermission") : t("permissions.addPermission")}
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
            <div className="space-y-2">
              <label className="flex items-center gap-2 text-sm">
                <Checkbox
                  checked={draft.view}
                  onCheckedChange={(c) => setDraft((d) => ({ ...d, view: c === true }))}
                />
                <span>{t("permissions.view")}</span>
                <span className="text-xs text-muted-foreground">{t("permissions.viewHint")}</span>
              </label>
              <label className="flex items-center gap-2 text-sm">
                <Checkbox
                  checked={draft.modify}
                  onCheckedChange={(c) => setDraft((d) => ({ ...d, modify: c === true }))}
                />
                <span>{t("permissions.modify")}</span>
                <span className="text-xs text-muted-foreground">{t("permissions.modifyHint")}</span>
              </label>
              <label className="flex items-center gap-2 text-sm">
                <Checkbox
                  checked={draft.apply}
                  onCheckedChange={(c) => setDraft((d) => ({ ...d, apply: c === true }))}
                />
                <span>{t("permissions.apply")}</span>
                <span className="text-xs text-muted-foreground">{t("permissions.applyHint")}</span>
              </label>
            </div>
          </div>

          <div className="flex items-center justify-between rounded-md border p-3">
            <div className="space-y-0.5 pr-4">
              <Label htmlFor="grant-inherit" className="text-sm">
                {t("permissions.inheritToggleLabel")}
              </Label>
              <p className="text-[11px] text-muted-foreground">{t("permissions.inheritToggleHint")}</p>
            </div>
            <Switch
              id="grant-inherit"
              checked={draft.inherit}
              onCheckedChange={(c) => setDraft((d) => ({ ...d, inherit: c }))}
            />
          </div>
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
  const hasObject = objectId.length > 0;

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
  const baseline = data?.baseline_privileges ?? [];
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
            <p className="text-sm font-medium leading-none">{t("permissions.stewardLabel")}</p>
            <HelpTooltip text={t("permissions.stewardHelp")} />
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
            <p className="text-sm text-muted-foreground italic">{t("permissions.stewardNone")}</p>
          )}
        </section>
      )}

      {hasObject && (
        <section className="space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-1.5">
              <KeyRound className="h-4 w-4 text-muted-foreground" />
              <p className="text-sm font-medium leading-none">{t("permissions.title")}</p>
            </div>
            {canManage && (
              <Button size="sm" onClick={openAdd} className="gap-1.5">
                <Plus className="h-3.5 w-3.5" />
                {t("permissions.addPermission")}
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
                    <TableHead>{t("permissions.inheritance")}</TableHead>
                    <TableHead>{t("permissions.grantedBy")}</TableHead>
                    {canManage && <TableHead className="w-[80px] text-right" />}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {/* Baseline "All users" row — read-only. */}
                  <TableRow className="bg-muted/30">
                    <TableCell>
                      <div className="flex items-center gap-2">
                        <Users className="h-4 w-4 shrink-0 text-muted-foreground" />
                        <span>{t("permissions.allUsers")}</span>
                        <span className="text-xs text-muted-foreground">{t("permissions.defaultNote")}</span>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {baseline.length === 0 ? (
                          <span className="text-xs text-muted-foreground">—</span>
                        ) : (
                          baseline.map((p) => (
                            <Badge key={p} variant="outline" className="text-xs text-muted-foreground">
                              {p === PRIV_SELECT
                                ? t("permissions.view")
                                : p === PRIV_APPLY
                                  ? t("permissions.apply")
                                  : p === PRIV_MODIFY
                                    ? t("permissions.modify")
                                    : p}
                            </Badge>
                          ))
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <span className="text-xs text-muted-foreground">—</span>
                    </TableCell>
                    <TableCell>
                      <span className="text-xs text-muted-foreground">—</span>
                    </TableCell>
                    {canManage && <TableCell />}
                  </TableRow>

                  {grants.length === 0 ? (
                    <TableRow>
                      <TableCell
                        colSpan={canManage ? 5 : 4}
                        className="text-center text-sm text-muted-foreground py-6"
                      >
                        {t("permissions.noGrants")}
                      </TableCell>
                    </TableRow>
                  ) : (
                    grants.map((grant) => {
                      const inherited = grant.inherited ?? false;
                      return (
                        <TableRow key={grant.principal_id} className={cn(inherited && "opacity-60")}>
                          <TableCell>
                            <PrincipalCell
                              name={grant.principal_name ?? grant.principal_id}
                              type={grant.principal_type}
                              muted={inherited}
                            />
                          </TableCell>
                          <TableCell>
                            <PrivilegeBadges privileges={grant.privileges ?? []} />
                          </TableCell>
                          <TableCell>
                            {inherited ? (
                              <span className="text-xs text-muted-foreground italic">
                                {t("permissions.inheritedFrom", {
                                  type: grant.inherited_from_type ?? "",
                                })}
                              </span>
                            ) : grant.inherit ? (
                              <span className="text-xs">{t("permissions.inherits")}</span>
                            ) : (
                              <span className="text-xs text-muted-foreground">—</span>
                            )}
                          </TableCell>
                          <TableCell>
                            <span className="text-xs text-muted-foreground">{grant.grantor ?? "—"}</span>
                          </TableCell>
                          {canManage && (
                            <TableCell className="text-right">
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
                                  <Button
                                    variant="ghost"
                                    size="icon"
                                    className="h-7 w-7 text-destructive hover:text-destructive"
                                    onClick={() => handleRemove(grant)}
                                    disabled={removingId === grant.principal_id}
                                    aria-label={t("permissions.removePermission")}
                                  >
                                    {removingId === grant.principal_id ? (
                                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                    ) : (
                                      <Trash2 className="h-3.5 w-3.5" />
                                    )}
                                  </Button>
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
      )}

      <GrantDialog
        open={dialogOpen}
        onOpenChange={setDialogOpen}
        editing={editing}
        defaultInherit={defaultInherit}
        saving={setMut.isPending}
        onSave={handleSave}
      />
    </div>
  );
}
