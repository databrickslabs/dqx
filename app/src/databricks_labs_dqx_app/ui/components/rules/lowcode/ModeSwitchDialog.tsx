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

/** The three authoring modes a Rules Registry rule can be built in. */
export type RegistryEditMode = "dqx_native" | "lowcode" | "sql";

const DIRECTIONS: Record<string, ModeSwitchDirection> = {
  "lowcode>sql": "LOWCODE_TO_SQL",
  "lowcode>dqx_native": "LOWCODE_TO_NATIVE",
  "sql>lowcode": "SQL_TO_LOWCODE",
  "sql>dqx_native": "SQL_TO_NATIVE",
  "dqx_native>lowcode": "NATIVE_TO_LOWCODE",
  "dqx_native>sql": "NATIVE_TO_SQL",
};

/**
 * The guarded-switch direction for a *from* -> *to* authoring-mode change, or
 * `null` when there is nothing to guard — either the mode is unchanged, or the
 * source mode holds no content the target can't preserve (*sourceHasContent*
 * false), so the switch can proceed silently without a confirm dialog.
 *
 * Note: the SQL <-> Native / Low-Code <-> Native transitions always clear the
 * target-incompatible body, so they're guarded whenever the source has
 * content; Low-Code <-> SQL is guarded too because hand-edited SQL can't be
 * rebuilt into structured conditions.
 */
export function modeSwitchDirection(
  from: RegistryEditMode,
  to: RegistryEditMode,
  sourceHasContent: boolean,
): ModeSwitchDirection | null {
  if (from === to || !sourceHasContent) return null;
  return DIRECTIONS[`${from}>${to}`] ?? null;
}

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
