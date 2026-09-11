import { Loader2 } from "lucide-react";
import { useTranslation } from "react-i18next";

/**
 * Full-screen "Loading DQX Studio" spinner shown during app bootstrap. Rendered
 * by both AuthGuard (auth handshake) and SetupGate (setup-readiness check) so the
 * two phases read as one continuous load instead of a flash or a blank screen.
 */
export function StudioLoadingScreen() {
  const { t } = useTranslation();
  return (
    <div className="flex items-center justify-center min-h-screen bg-background">
      <div className="text-center space-y-4">
        <Loader2 className="h-12 w-12 animate-spin text-primary mx-auto" />
        <div className="text-lg font-medium">{t("auth.loadingMessage")}</div>
      </div>
    </div>
  );
}
