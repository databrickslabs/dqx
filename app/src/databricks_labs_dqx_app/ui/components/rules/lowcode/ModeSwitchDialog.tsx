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
  | "LOWCODE_TO_NATIVE"
  | "SQL_TO_NATIVE"
  | "NATIVE_TO_LOWCODE"
  | "NATIVE_TO_SQL";

/** The three authoring modes a Rules Registry rule can be built in. */
export type RegistryEditMode = "dqx_native" | "lowcode" | "sql";

// Low-Code <-> SQL is deliberately ABSENT: those two are authoring surfaces for
// one rule type, presented as tabs, and both directions preserve their side's
// state (the builder's rows compile into the editor, the editor's text is left
// alone once hand-edited, and the AST is cached for the way back). A tab that
// interrupts with a confirm dialog reads as a warning about losing work that
// isn't actually lost.
const DIRECTIONS: Record<string, ModeSwitchDirection> = {
  "lowcode>dqx_native": "LOWCODE_TO_NATIVE",
  "sql>dqx_native": "SQL_TO_NATIVE",
  "dqx_native>lowcode": "NATIVE_TO_LOWCODE",
  "dqx_native>sql": "NATIVE_TO_SQL",
};

/**
 * The guarded-switch direction for a *from* -> *to* authoring-mode change, or
 * `null` when there is nothing to guard — either the mode is unchanged, the pair
 * is unguarded by design (see {@link DIRECTIONS}), or the source mode holds no
 * content the target can't preserve (*sourceHasContent* false), so the switch can
 * proceed silently without a confirm dialog.
 *
 * Note: the transitions to and from Native always clear the target-incompatible
 * body, so they stay guarded whenever the source has content.
 */
export function modeSwitchDirection(
  from: RegistryEditMode,
  to: RegistryEditMode,
  sourceHasContent: boolean,
): ModeSwitchDirection | null {
  if (from === to || !sourceHasContent) return null;
  return DIRECTIONS[`${from}>${to}`] ?? null;
}

/**
 * Whether *from* -> *to* moves between the two authoring SURFACES of one custom
 * condition (visual builder <-> SQL) rather than replacing the rule's body.
 *
 * The counterpart to {@link DIRECTIONS}: this is exactly the pair left unguarded
 * there, and for the same reason. Nothing is being discarded, so nothing that
 * describes the body — the granularity choice and its merge keys included —
 * should be reset on the way across, or a round trip through the other tab would
 * quietly undo the author's settings.
 */
export function isCustomSurfaceHop(from: RegistryEditMode, to: RegistryEditMode): boolean {
  return (from === "lowcode" && to === "sql") || (from === "sql" && to === "lowcode");
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
