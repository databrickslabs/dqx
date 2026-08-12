/**
 * OwnerGrantDialog — confirmation popup shown when the user picks a new
 * owner on a registry rule or collection.
 *
 * Behaviour:
 *  - Explains that saving will grant the new owner ALL_PRIVILEGES.
 *  - Optionally shows a tickbox to also remove the OLD owner's grant —
 *    only when the old owner currently holds an ALL_PRIVILEGES grant on
 *    this object (detected by the caller and passed via `showRemoveOld`).
 *  - Clicking Confirm closes the dialog; clicking Cancel leaves the owner
 *    picker in its original state.
 *
 * The actual grant writes happen at save time (in the caller's save handler),
 * not inside this dialog.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";

export interface OwnerGrantDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Display name of the newly picked owner. */
  newOwnerName: string;
  /** Display name of the old owner (empty string when there was none). */
  oldOwnerName: string;
  /** "rule" | "collection" — drives the copy of the body sentence. */
  objectKind: "rule" | "collection";
  /**
   * Whether to offer the "remove old owner's grant" tickbox.  Pass `true`
   * only when the old owner currently holds an ALL_PRIVILEGES grant on this
   * object so the offer is meaningful.
   */
  showRemoveOld: boolean;
  /**
   * Called when the user confirms.
   * `removeOld`: whether the old owner's ALL_PRIVILEGES grant should also
   * be revoked (only ever `true` when `showRemoveOld` is true and the user
   * checked the box).
   */
  onConfirm: (removeOld: boolean) => void;
}

export function OwnerGrantDialog({
  open,
  onOpenChange,
  newOwnerName,
  oldOwnerName,
  objectKind,
  showRemoveOld,
  onConfirm,
}: OwnerGrantDialogProps) {
  const { t } = useTranslation();
  const [removeOld, setRemoveOld] = useState(false);

  // Reset the tickbox whenever the dialog opens so a previous selection
  // doesn't bleed into a subsequent pick.
  // (useLayoutEffect isn't needed here — the checkbox is always hidden on
  // first render of each open, so there's no transient state flash.)
  const handleOpenChange = (next: boolean) => {
    if (!next) setRemoveOld(false);
    onOpenChange(next);
  };

  const handleConfirm = () => {
    const shouldRemove = showRemoveOld && removeOld;
    setRemoveOld(false);
    onConfirm(shouldRemove);
  };

  const bodyKey =
    objectKind === "rule"
      ? "permissions.ownerGrantDialogBodyRule"
      : "permissions.ownerGrantDialogBodyCollection";

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{t("permissions.ownerGrantDialogTitle")}</DialogTitle>
        </DialogHeader>

        <div className="space-y-4 text-sm">
          <p>{t(bodyKey, { name: newOwnerName })}</p>

          {showRemoveOld && oldOwnerName && (
            <label className="flex items-start gap-2 cursor-pointer">
              <Checkbox
                id="remove-old-owner-grant"
                checked={removeOld}
                onCheckedChange={(c) => setRemoveOld(c === true)}
                className="mt-0.5 shrink-0"
              />
              <Label htmlFor="remove-old-owner-grant" className="font-normal leading-snug cursor-pointer">
                {t("permissions.ownerGrantDialogRemoveOld", { name: oldOwnerName })}
              </Label>
            </label>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => handleOpenChange(false)}>
            {t("common.cancel")}
          </Button>
          <Button onClick={handleConfirm}>
            {t("permissions.ownerGrantDialogConfirm")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
