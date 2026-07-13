import { useTranslation } from "react-i18next";
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

// Direction of a guarded authoring-mode switch that would discard or
// translate work. Ported from dqlake's ModeSwitchDialog, generalized to the
// three DQX modes (dqx_native / lowcode / sql). A switch is only guarded when
// the source mode has real content that the target can't preserve losslessly.
export type ModeSwitchDirection =
  | "LOWCODE_TO_SQL"
  | "LOWCODE_TO_NATIVE"
  | "SQL_TO_LOWCODE"
  | "SQL_TO_NATIVE"
  | "NATIVE_TO_LOWCODE"
  | "NATIVE_TO_SQL";

type Props = {
  open: boolean;
  direction: ModeSwitchDirection | null;
  onCancel: () => void;
  onConfirm: () => void;
};

export function ModeSwitchDialog({ open, direction, onCancel, onConfirm }: Props) {
  const { t } = useTranslation();
  if (!direction) return null;
  const title = t(`rulesRegistry.modeSwitch.${direction}.title`);
  const body = t(`rulesRegistry.modeSwitch.${direction}.body`);
  const confirm = t(`rulesRegistry.modeSwitch.${direction}.confirm`);
  return (
    <AlertDialog
      open={open}
      onOpenChange={(o) => {
        if (!o) onCancel();
      }}
    >
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          <AlertDialogDescription>{body}</AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel onClick={onCancel}>{t("common.cancel")}</AlertDialogCancel>
          <AlertDialogAction onClick={onConfirm}>{confirm}</AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
