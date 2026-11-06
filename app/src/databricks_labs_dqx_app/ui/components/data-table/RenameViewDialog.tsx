import type { FormEvent } from "react";
import { useTranslation } from "react-i18next";
import type { SavedViewOut } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { useViewNameState, ViewNameField } from "./SaveViewDialog";

export interface RenameViewDialogProps {
  /** The view being renamed; the dialog is open while set. */
  view: SavedViewOut | null;
  /** Every view on the page, for the duplicate-name check. */
  views: readonly SavedViewOut[];
  onClose: () => void;
  onRename: (id: string, name: string) => Promise<void>;
}

/** Rename a saved view, prefilled with its current name. */
export function RenameViewDialog({ view, views, onClose, onRename }: RenameViewDialogProps) {
  const { t } = useTranslation();
  const open = view !== null;
  const nameState = useViewNameState(view?.name ?? "", open, view?.scope ?? "user", views, view?.id);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!view) return;
    const ok = await nameState.submit(() => onRename(view.id, nameState.name));
    if (ok) onClose();
  };

  return (
    <Dialog open={open} onOpenChange={(next) => !next && onClose()}>
      <DialogContent className="sm:max-w-sm">
        <form onSubmit={handleSubmit} className="space-y-4">
          <DialogHeader>
            <DialogTitle>{t("savedViews.renameTitle")}</DialogTitle>
          </DialogHeader>
          <ViewNameField value={nameState.name} onChange={nameState.changeName} error={nameState.error} />
          <DialogFooter>
            <Button type="button" variant="outline" size="sm" onClick={onClose}>
              {t("common.cancel")}
            </Button>
            <Button type="submit" size="sm" disabled={nameState.pending}>
              {t("savedViews.save")}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
