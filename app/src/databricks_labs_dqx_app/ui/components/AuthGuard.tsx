import { useEffect, useState } from "react";
import axios from "axios";
import { Loader2 } from "lucide-react";
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
  const [isAuthReady, setIsAuthReady] = useState(false);
  const [retryCount, setRetryCount] = useState(0);
  const [error, setError] = useState<string | null>(null);

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
              ? "Authentication timeout. The authentication flow did not complete."
              : `Server error (${err.response?.status}): ${err.response?.statusText || err.message}`
            : err instanceof Error
            ? err.message
            : "Unknown connection error";
          
          setError(
            `${errorMessage}\n\nPlease refresh the page or contact your administrator if the problem persists.`
          );
        }
      }
    };

    if (!isAuthReady && !error) {
      // Add a small delay before first attempt (500ms) to let page fully load
      // For retries, use exponential backoff calculated in error handler
      const initialDelay = retryCount === 0 ? 500 : 0;
      
      if (initialDelay > 0) {
        timeoutId = setTimeout(() => {
          if (!cancelled) {
            checkAuth();
          }
        }, initialDelay);
      } else {
        checkAuth();
      }
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
            Authentication Error
          </div>
          <p className="text-muted-foreground">{error}</p>
          <button
            onClick={() => window.location.reload()}
            className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90"
          >
            Refresh Page
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
          <div className="text-lg font-medium">Initializing DQX Studio...</div>
          <p className="text-sm text-muted-foreground">
            Setting up your workspace connection
          </p>
        </div>
      </div>
    );
  }

  // Auth is ready, render the app
  return <>{children}</>;
}

