import { useEffect, useState } from "react";
import axios from "axios";
import { Loader2 } from "lucide-react";

interface AuthGuardProps {
  children: React.ReactNode;
}

/**
 * AuthGuard component that waits for Databricks authentication to be ready
 * before rendering the app. This handles the initial OAuth flow timing issue
 * where the X-Forwarded-Access-Token header is not immediately available.
 * 
 * The component polls /api/current-user (which requires authentication) until
 * it succeeds, confirming that the OAuth flow has completed and the app can
 * make authenticated API calls.
 */
export function AuthGuard({ children }: AuthGuardProps) {
  const [isAuthReady, setIsAuthReady] = useState(false);
  const [retryCount, setRetryCount] = useState(0);
  const [error, setError] = useState<string | null>(null);

  // Log when AuthGuard mounts
  console.log("[DQX Auth] AuthGuard mounted - blocking app render until auth confirmed");

  useEffect(() => {
    let cancelled = false;
    let timeoutId: NodeJS.Timeout;
    
    const checkAuth = async () => {
      try {
        console.log(`[DQX Auth] Checking authentication (attempt ${retryCount + 1}/15)...`);
        
        // Make a request to an endpoint that REQUIRES authentication
        // Using /api/current-user because it requires the OBO token
        const response = await axios.get("/api/current-user", {
          // Ensure credentials are included
          withCredentials: true,
          // Add timeout
          timeout: 10000,
        });
        
        if (!cancelled) {
          console.log("[DQX Auth] ✓ Authentication ready - logged in as:", response.data.user_name);
          setIsAuthReady(true);
        }
      } catch (err) {
        if (cancelled) return;

        // Log detailed error info
        console.error("[DQX Auth] Error checking authentication:", {
          error: err,
          isAxiosError: axios.isAxiosError(err),
          status: axios.isAxiosError(err) ? err.response?.status : null,
          statusText: axios.isAxiosError(err) ? err.response?.statusText : null,
          message: err instanceof Error ? err.message : String(err),
        });

        // Retry on ANY error with exponential backoff
        if (retryCount < 15) {
          const delay = Math.min(1000 * Math.pow(1.3, retryCount), 3000);
          
          const errorType = axios.isAxiosError(err)
            ? err.response?.status === 401
              ? "401 Unauthorized"
              : err.response
              ? `HTTP ${err.response.status}`
              : "Network error"
            : "Unknown error";
          
          console.log(
            `[DQX Auth] ${errorType} - retrying in ${delay}ms (attempt ${retryCount + 1}/15)...`
          );
          
          timeoutId = setTimeout(() => {
            if (!cancelled) {
              setRetryCount((prev) => prev + 1);
            }
          }, delay);
        } else {
          // Give up after 15 attempts
          console.error("[DQX Auth] Failed after 15 attempts");
          
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
          <div className="text-lg font-medium">Initializing DQX...</div>
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

