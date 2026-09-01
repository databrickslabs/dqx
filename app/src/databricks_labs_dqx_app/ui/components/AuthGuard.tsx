import { useEffect, useRef, useState } from "react";
import axios from "axios";
import { Loader2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import { currentUser } from "@/lib/api";

interface AuthGuardProps {
  children: React.ReactNode;
}

/**
 * AuthGuard component that waits for Databricks authentication to be ready
 * before rendering the app. This handles the initial OAuth flow timing issue
 * where the X-Forwarded-Access-Token header is not immediately available.
 *
 * Uses the orval-generated currentUser() function so the URL stays in sync
 * with the backend OpenAPI spec.
 */
export function AuthGuard({ children }: AuthGuardProps) {
  const { t } = useTranslation();
  const tRef = useRef(t);
  const [isAuthReady, setIsAuthReady] = useState(false);
  const [retryCount, setRetryCount] = useState(0);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    tRef.current = t;
  }, [t]);

  useEffect(() => {
    let cancelled = false;
    let timeoutId: NodeJS.Timeout;

    const checkAuth = async () => {
      try {
        await currentUser({ timeout: 10000 });

        if (!cancelled) {
          setIsAuthReady(true);
        }
      } catch (err) {
        if (cancelled) return;

        if (retryCount < 15) {
          const delay = Math.min(1000 * Math.pow(1.3, retryCount), 3000);

          timeoutId = setTimeout(() => {
            if (!cancelled) {
              setRetryCount((prev) => prev + 1);
            }
          }, delay);
        } else {
          const errorMessage = axios.isAxiosError(err)
            ? err.response?.status === 401
              ? tRef.current("auth.timeoutMessage")
              : tRef.current("auth.serverErrorMessage", {
                  status: err.response?.status ?? "",
                  statusText: err.response?.statusText || err.message,
                })
            : err instanceof Error
            ? err.message
            : tRef.current("auth.unknownError");

          setError(
            `${errorMessage}${tRef.current("auth.errorSuffix")}`
          );
        }
      }
    };

    if (!isAuthReady && !error) {
      // Fire the first attempt immediately — the X-Forwarded-Access-Token
      // header is typically already present by the time React mounts, and
      // the 401-retry backoff below already covers the case where it isn't
      // yet. A fixed 500ms pre-delay here used to push out first paint of
      // the real app for every user, even ones with the token ready
      // instantly (item 2 load-time investigation).
      checkAuth();
    }

    return () => {
      cancelled = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    };
  }, [retryCount, isAuthReady, error]);

  // Show error state
  if (error) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-background">
        <div className="text-center space-y-4 p-8 max-w-md">
          <div className="text-destructive text-lg font-semibold">
            {t("auth.errorTitle")}
          </div>
          <p className="text-muted-foreground">{error}</p>
          <button
            onClick={() => window.location.reload()}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90"
          >
            {t("auth.refreshPage")}
          </button>
        </div>
      </div>
    );
  }

  // Show loading state while waiting for auth
  if (!isAuthReady) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-background">
        <div className="text-center space-y-4">
          <Loader2 className="h-12 w-12 animate-spin text-primary mx-auto" />
          <div className="text-lg font-medium">{t("auth.loadingMessage")}</div>
        </div>
      </div>
    );
  }

  // Auth is ready, render the app
  return <>{children}</>;
}

