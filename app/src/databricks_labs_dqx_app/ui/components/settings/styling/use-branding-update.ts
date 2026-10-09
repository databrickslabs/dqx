import { useCallback } from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { AxiosResponse } from "axios";
import type { TFunction } from "i18next";
import { toast } from "sonner";
import { getGetBrandingQueryKey, type BrandingOut } from "@/lib/api";
import { extractApiError } from "@/lib/api-error";
import {
  applyStyleSheet,
  clearBrandingCache,
  snapshotFromApi,
  toStyleSheet,
  writeBrandingCache,
} from "@/lib/branding";

/** Seeds the branding query with a mutation response and applies it to the page straight away. */
export function useBrandingUpdate(): {
  applyResponse: (response: AxiosResponse<BrandingOut>) => void;
  applyReset: (response: AxiosResponse<BrandingOut>) => void;
} {
  const queryClient = useQueryClient();
  const applyResponse = useCallback(
    (response: AxiosResponse<BrandingOut>) => {
      queryClient.setQueryData(getGetBrandingQueryKey(), response);
      const snapshot = snapshotFromApi(response.data);
      applyStyleSheet(toStyleSheet(snapshot.overrides));
      writeBrandingCache(snapshot);
    },
    [queryClient],
  );
  const applyReset = useCallback(
    (response: AxiosResponse<BrandingOut>) => {
      queryClient.setQueryData(getGetBrandingQueryKey(), response);
      clearBrandingCache();
      applyStyleSheet("");
    },
    [queryClient],
  );
  return { applyResponse, applyReset };
}

export function toastSaveError(t: TFunction, err: unknown): void {
  toast.error(t("config.styling.saveFailed", { detail: extractApiError(err, t("common.unknownError")) }));
}
