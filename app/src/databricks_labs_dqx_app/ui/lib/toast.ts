import { toast } from "sonner";
import i18n from "@/lib/i18n";

/**
 * Show an error toast with a "Copy" action that copies the message text to
 * the clipboard (B2-30). Error messages are often long, paste-into-a-bug-
 * report strings, so every error toast gets a copy affordance on top of the
 * dismiss X enabled globally on the `<Toaster>`.
 *
 * Uses the i18next instance directly (not the `useTranslation` hook) so it can
 * be called from non-component contexts — notably the QueryClient
 * MutationCache in `main.tsx`.
 */
export function errorToast(message: string): void {
  toast.error(message, {
    action: {
      label: i18n.t("common.copy"),
      onClick: () => {
        // `navigator.clipboard` is undefined in insecure contexts — no-op
        // rather than throw if it (or the write) isn't available.
        if (!navigator.clipboard) return;
        void navigator.clipboard.writeText(message).then(
          () => toast.success(i18n.t("common.copied")),
          () => {
            /* clipboard write rejected — nothing to do */
          },
        );
      },
    },
  });
}
