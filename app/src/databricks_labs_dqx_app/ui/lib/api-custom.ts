/**
 * Custom API hooks for endpoints not yet in the auto-generated api.ts.
 * These will be replaced by orval-generated hooks once the OpenAPI spec is regenerated.
 */
import { useMutation, useQuery } from "@tanstack/react-query";
import type { UseMutationOptions, UseMutationResult, UseQueryOptions, UseQueryResult } from "@tanstack/react-query";
import * as axios from "axios";
import type { AxiosError, AxiosRequestConfig, AxiosResponse } from "axios";
import type { RuleCatalogEntryOut, RunStatusOut } from "./api";

export interface BatchSaveRulesIn {
  table_fqns: string[];
  checks: Array<{ [key: string]: unknown }>;
}

export interface BatchSaveRulesOut {
  saved: RuleCatalogEntryOut[];
  failed: Array<{ table_fqn: string; error: string }>;
}

export const batchSaveRules = (
  body: BatchSaveRulesIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<BatchSaveRulesOut>> => {
  return axios.default.post(`/api/v1/rules/batch`, body, options);
};

export const useBatchSaveRules = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof batchSaveRules>>,
      TError,
      { data: BatchSaveRulesIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof batchSaveRules>>,
  TError,
  { data: BatchSaveRulesIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};

  const mutationFn = (props: { data: BatchSaveRulesIn }) => {
    return batchSaveRules(props.data, axiosOptions);
  };

  return useMutation({ mutationFn, mutationKey: ["batchSaveRules"], ...mutationOptions });
};

// ---------------------------------------------------------------------------
// Batch run from catalog (reads approved rules from Delta table)
// ---------------------------------------------------------------------------

export interface BatchRunFromCatalogIn {
  table_fqns: string[];
  sample_size?: number;
}

export interface DryRunSubmitOutCustom {
  run_id: string;
  job_run_id: number;
  view_fqn: string;
  /** Source table FQN this run was submitted for — added so callers can
   *  associate each submitted run with its table by value instead of by
   *  request-array position (batch submission skips tables that fail
   *  validation, which shifts index-based lookups out of alignment). */
  table_fqn?: string | null;
}

export interface BatchRunFromCatalogOut {
  submitted: DryRunSubmitOutCustom[];
  errors: string[];
}

export const batchRunFromCatalog = (
  body: BatchRunFromCatalogIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<BatchRunFromCatalogOut>> => {
  return axios.default.post(`/api/v1/dryrun/batch-from-catalog`, body, options);
};

export const useBatchRunFromCatalog = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof batchRunFromCatalog>>,
      TError,
      { data: BatchRunFromCatalogIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof batchRunFromCatalog>>,
  TError,
  { data: BatchRunFromCatalogIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};

  const mutationFn = (props: { data: BatchRunFromCatalogIn }) => {
    return batchRunFromCatalog(props.data, axiosOptions);
  };

  return useMutation({ mutationFn, mutationKey: ["batchRunFromCatalog"], ...mutationOptions });
};

// ---------------------------------------------------------------------------
// Filter tables by required columns
// ---------------------------------------------------------------------------

export interface FilterTablesByColumnsIn {
  required_columns: string[];
  table_fqns: string[];
}

export interface FilterTablesByColumnsOut {
  matching: string[];
  not_matching: string[];
  errors: Array<{ table_fqn: string; error: string }>;
}

export const filterTablesByColumns = (
  body: FilterTablesByColumnsIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<FilterTablesByColumnsOut>> => {
  return axios.default.post(`/api/v1/discovery/filter-tables-by-columns`, body, options);
};

export const useFilterTablesByColumns = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof filterTablesByColumns>>,
      TError,
      { data: FilterTablesByColumnsIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof filterTablesByColumns>>,
  TError,
  { data: FilterTablesByColumnsIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};

  const mutationFn = (props: { data: FilterTablesByColumnsIn }) => {
    return filterTablesByColumns(props.data, axiosOptions);
  };

  return useMutation({ mutationFn, mutationKey: ["filterTablesByColumns"], ...mutationOptions });
};

// ---------------------------------------------------------------------------
// Validation (dry-run) history
// ---------------------------------------------------------------------------

export interface ValidationRunSummaryOut {
  run_id: string;
  source_table_fqn: string;
  status: string | null;
  requesting_user: string | null;
  canceled_by: string | null;
  run_type: string | null;
  updated_at: string | null;
  sample_size: number | null;
  total_rows: number | null;
  valid_rows: number | null;
  invalid_rows: number | null;
  error_rows: number | null;
  warning_rows: number | null;
  created_at: string | null;
  error_message: string | null;
  checks: Record<string, unknown>[];
  review_status?: string | null;
  review_status_is_default?: boolean;
  review_status_updated_by?: string | null;
  review_status_updated_at?: string | null;
}

export const listValidationRuns = (
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<ValidationRunSummaryOut[]>> => {
  return axios.default.get(`/api/v1/dryrun/runs`, options);
};

export const getListValidationRunsQueryKey = () =>
  [`/api/v1/dryrun/runs`] as const;

export const useListValidationRuns = <
  TData = Awaited<ReturnType<typeof listValidationRuns>>,
  TError = AxiosError<unknown>,
>(
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof listValidationRuns>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  const queryKey = queryOptions?.queryKey ?? getListValidationRunsQueryKey();

  const queryFn = () => listValidationRuns(axiosOptions);

  return useQuery({ queryKey, queryFn, ...queryOptions }) as UseQueryResult<TData, TError>;
};

// ---------------------------------------------------------------------------
// Cancel dry run
// ---------------------------------------------------------------------------

export const cancelDryRun = (
  runId: string,
  params: { job_run_id: number },
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<{ status: string; run_id: string }>> => {
  return axios.default.post(`/api/v1/dryrun/runs/${runId}/cancel`, null, {
    ...options,
    params,
  });
};

// ---------------------------------------------------------------------------
// Get dry run status (with optional query params for skip-history runs)
// ---------------------------------------------------------------------------

export const getDryRunStatusCustom = (
  runId: string,
  params?: { job_run_id?: number; view_fqn?: string },
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RunStatusOut>> => {
  return axios.default.get(`/api/v1/dryrun/runs/${runId}/status`, {
    ...options,
    params,
  });
};

// ---------------------------------------------------------------------------
// Cancel profiler run
// ---------------------------------------------------------------------------

export const cancelProfileRun = (
  runId: string,
  params: { job_run_id: number; view_fqn?: string },
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<{ status: string; run_id: string }>> => {
  return axios.default.post(`/api/v1/profiler/runs/${runId}/cancel`, null, {
    ...options,
    params,
  });
};

// ---------------------------------------------------------------------------
// Check duplicates
// ---------------------------------------------------------------------------

export interface CheckDuplicatesIn {
  table_fqn: string;
  checks: Array<{ [key: string]: unknown }>;
  exclude_rule_id?: string;
  exclude_rule_ids?: string[];
}

export interface CheckDuplicatesOut {
  duplicates: Array<{ [key: string]: unknown }>;
}

export const checkDuplicates = (
  body: CheckDuplicatesIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<CheckDuplicatesOut>> => {
  return axios.default.post(`/api/v1/rules/check-duplicates`, body, options);
};

export const useCheckDuplicates = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof checkDuplicates>>,
      TError,
      { data: CheckDuplicatesIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof checkDuplicates>>,
  TError,
  { data: CheckDuplicatesIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};

  const mutationFn = (props: { data: CheckDuplicatesIn }) => {
    return checkDuplicates(props.data, axiosOptions);
  };

  return useMutation({ mutationFn, mutationKey: ["checkDuplicates"], ...mutationOptions });
};

// ---------------------------------------------------------------------------
// Backfill missing rule_ids
// ---------------------------------------------------------------------------

export const backfillRuleIds = (
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<{ repaired: number }>> => {
  return axios.default.post(`/api/v1/rules/backfill-ids`, null, options);
};

// ---------------------------------------------------------------------------
// Delete rule by rule_id
// ---------------------------------------------------------------------------

export const deleteRuleById = (
  ruleId: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<{ status: string; rule_id: string }>> => {
  return axios.default.delete(`/api/v1/rules/${ruleId}`, options);
};

export const useDeleteRuleById = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof deleteRuleById>>,
      TError,
      { ruleId: string },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof deleteRuleById>>,
  TError,
  { ruleId: string },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};

  const mutationFn = (props: { ruleId: string }) => {
    return deleteRuleById(props.ruleId, axiosOptions);
  };

  return useMutation({ mutationFn, mutationKey: ["deleteRuleById"], ...mutationOptions });
};

// ---------------------------------------------------------------------------
// Status transitions by rule_id
// ---------------------------------------------------------------------------

export const submitRuleForApproval = (
  ruleId: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RuleCatalogEntryOut>> => {
  return axios.default.post(`/api/v1/rules/${ruleId}/submit`, null, options);
};

export const approveRule = (
  ruleId: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RuleCatalogEntryOut>> => {
  return axios.default.post(`/api/v1/rules/${ruleId}/approve`, null, options);
};

export const rejectRule = (
  ruleId: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RuleCatalogEntryOut>> => {
  return axios.default.post(`/api/v1/rules/${ruleId}/reject`, null, options);
};

export const revokeRule = (
  ruleId: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RuleCatalogEntryOut>> => {
  return axios.default.post(`/api/v1/rules/${ruleId}/revoke`, null, options);
};

// ---------------------------------------------------------------------------
// Validate checks
// ---------------------------------------------------------------------------

export interface ValidateChecksIn {
  checks: Array<{ [key: string]: unknown }>;
}

export interface ValidateChecksOut {
  valid: boolean;
  errors: string[];
}

export const validateChecks = (
  body: ValidateChecksIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<ValidateChecksOut>> => {
  return axios.default.post(`/api/v1/rules/validate-checks`, body, options);
};

export const useValidateChecks = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof validateChecks>>,
      TError,
      { data: ValidateChecksIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof validateChecks>>,
  TError,
  { data: ValidateChecksIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};

  const mutationFn = (props: { data: ValidateChecksIn }) => {
    return validateChecks(props.data, axiosOptions);
  };

  return useMutation({ mutationFn, mutationKey: ["validateChecks"], ...mutationOptions });
};

// ---------------------------------------------------------------------------
// Comments
// ---------------------------------------------------------------------------

export interface AddCommentIn {
  entity_type: string;
  entity_id: string;
  comment: string;
}

export interface CommentOut {
  comment_id: string;
  entity_type: string;
  entity_id: string;
  user_email: string;
  comment: string;
  created_at: string | null;
}

export const addComment = (
  body: AddCommentIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<CommentOut>> => {
  return axios.default.post(`/api/v1/comments`, body, options);
};

export const useAddComment = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof addComment>>,
      TError,
      { data: AddCommentIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof addComment>>,
  TError,
  { data: AddCommentIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};

  const mutationFn = (props: { data: AddCommentIn }) => {
    return addComment(props.data, axiosOptions);
  };

  return useMutation({ mutationFn, mutationKey: ["addComment"], ...mutationOptions });
};

export const listComments = (
  entityType: string,
  entityId: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<CommentOut[]>> => {
  return axios.default.get(`/api/v1/comments`, {
    ...options,
    params: { entity_type: entityType, entity_id: entityId },
  });
};

export const getListCommentsQueryKey = (entityType: string, entityId: string) =>
  [`/api/v1/comments`, entityType, entityId] as const;

export const useListComments = <
  TData = Awaited<ReturnType<typeof listComments>>,
  TError = AxiosError<unknown>,
>(
  entityType: string,
  entityId: string,
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof listComments>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  const queryKey = queryOptions?.queryKey ?? getListCommentsQueryKey(entityType, entityId);

  const queryFn = () => listComments(entityType, entityId, axiosOptions);

  return useQuery({
    queryKey,
    queryFn,
    enabled: !!entityType && !!entityId,
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

export const deleteComment = (
  commentId: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<{ status: string; comment_id: string }>> => {
  return axios.default.delete(`/api/v1/comments/${commentId}`, options);
};

export const useDeleteComment = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof deleteComment>>,
      TError,
      { commentId: string },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof deleteComment>>,
  TError,
  { commentId: string },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};

  const mutationFn = (props: { commentId: string }) => {
    return deleteComment(props.commentId, axiosOptions);
  };

  return useMutation({ mutationFn, mutationKey: ["deleteComment"], ...mutationOptions });
};

// ---------------------------------------------------------------------------
// Quarantine API
// ---------------------------------------------------------------------------

export interface QuarantineRecordOut {
  quarantine_id: string;
  run_id: string;
  source_table_fqn: string;
  requesting_user: string | null;
  row_data: Record<string, unknown> | null;
  errors: unknown[] | null;
  warnings: unknown[] | null;
  created_at: string | null;
}

export interface QuarantineListOut {
  records: QuarantineRecordOut[];
  total_count: number;
  offset: number;
  limit: number;
}

export interface QuarantineQueryParams {
  offset?: number;
  limit?: number;
  /** Filter to rows that failed only this check (matches errors or warnings). */
  check_name?: string;
}

export const listQuarantineRecords = (
  runId: string,
  params?: QuarantineQueryParams,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<QuarantineListOut>> => {
  return axios.default.get(`/api/v1/quarantine/runs/${runId}`, {
    ...options,
    params,
  });
};

export const getQuarantineRecordsQueryKey = (
  runId: string,
  offset?: number,
  limit?: number,
  checkName?: string,
) => [`/api/v1/quarantine/runs`, runId, offset, limit, checkName ?? null] as const;

export const useListQuarantineRecords = <
  TData = Awaited<ReturnType<typeof listQuarantineRecords>>,
  TError = AxiosError<unknown>,
>(
  runId: string,
  params?: QuarantineQueryParams,
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof listQuarantineRecords>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  const queryKey =
    queryOptions?.queryKey ??
    getQuarantineRecordsQueryKey(
      runId,
      params?.offset,
      params?.limit,
      params?.check_name,
    );

  const queryFn = () => listQuarantineRecords(runId, params, axiosOptions);

  return useQuery({
    queryKey,
    queryFn,
    enabled: !!runId,
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

export const getQuarantineCount = (
  runId: string,
  params?: { check_name?: string },
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<{ count: number }>> => {
  return axios.default.get(`/api/v1/quarantine/runs/${runId}/count`, {
    ...options,
    params,
  });
};

export const getQuarantineCountQueryKey = (runId: string, checkName?: string) =>
  [`/api/v1/quarantine/runs/count`, runId, checkName ?? null] as const;

export const useQuarantineCount = <
  TData = Awaited<ReturnType<typeof getQuarantineCount>>,
  TError = AxiosError<unknown>,
>(
  runId: string,
  params?: { check_name?: string },
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getQuarantineCount>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  const queryKey =
    queryOptions?.queryKey ?? getQuarantineCountQueryKey(runId, params?.check_name);
  const queryFn = () => getQuarantineCount(runId, params, axiosOptions);
  return useQuery({
    queryKey,
    queryFn,
    enabled: !!runId,
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

export const exportQuarantineRecords = (
  runId: string,
  format: "csv" | "json" | "xlsx" = "csv",
  checkName?: string,
): void => {
  const params = new URLSearchParams({ format });
  if (checkName) params.set("check_name", checkName);
  window.open(`/api/v1/quarantine/runs/${runId}/export?${params.toString()}`, "_blank");
};

// ---------------------------------------------------------------------------
// Metrics API
// ---------------------------------------------------------------------------

export interface MetricSnapshotOut {
  metric_id: string;
  run_id: string;
  source_table_fqn: string;
  run_type: string | null;
  total_rows: number | null;
  valid_rows: number | null;
  invalid_rows: number | null;
  pass_rate: number | null;
  error_breakdown: Array<{ error: string; count: number }> | null;
  requesting_user: string | null;
  created_at: string | null;
}

export interface MetricsSummaryOut {
  source_table_fqn: string;
  latest_pass_rate: number | null;
  latest_run_id: string | null;
  latest_run_type: string | null;
  latest_created_at: string | null;
}

export const getMetricsTrend = (
  tableFqn: string,
  params?: { limit?: number },
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<MetricSnapshotOut[]>> => {
  return axios.default.get(`/api/v1/metrics/${encodeURIComponent(tableFqn)}`, {
    ...options,
    params,
  });
};

export const getMetricsTrendQueryKey = (tableFqn: string) =>
  [`/api/v1/metrics/trend`, tableFqn] as const;

export const useMetricsTrend = <
  TData = Awaited<ReturnType<typeof getMetricsTrend>>,
  TError = AxiosError<unknown>,
>(
  tableFqn: string,
  params?: { limit?: number },
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getMetricsTrend>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  const queryKey = queryOptions?.queryKey ?? getMetricsTrendQueryKey(tableFqn);
  const queryFn = () => getMetricsTrend(tableFqn, params, axiosOptions);
  return useQuery({
    queryKey,
    queryFn,
    enabled: !!tableFqn,
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

export const getMetricsSummary = (
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<MetricsSummaryOut[]>> => {
  return axios.default.get(`/api/v1/metrics`, options);
};

export const getMetricsSummaryQueryKey = () => [`/api/v1/metrics/summary`] as const;

export const useMetricsSummary = <
  TData = Awaited<ReturnType<typeof getMetricsSummary>>,
  TError = AxiosError<unknown>,
>(
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getMetricsSummary>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  const queryKey = queryOptions?.queryKey ?? getMetricsSummaryQueryKey();
  const queryFn = () => getMetricsSummary(axiosOptions);
  return useQuery({
    queryKey,
    queryFn,
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

// ---------------------------------------------------------------------------
// Timezone setting
// ---------------------------------------------------------------------------

export interface TimezoneOut {
  timezone: string;
}

export interface TimezoneIn {
  timezone: string;
}

export const getTimezone = (
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<TimezoneOut>> =>
  axios.default.get("/api/v1/config/timezone", options);

export const getTimezoneQueryKey = () => ["timezone"] as const;

export const useTimezone = <
  TData = Awaited<ReturnType<typeof getTimezone>>["data"],
  TError = AxiosError<unknown>,
>(
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getTimezone>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  return useQuery({
    queryKey: queryOptions?.queryKey ?? getTimezoneQueryKey(),
    queryFn: () => getTimezone(axiosOptions),
    select: ((resp: Awaited<ReturnType<typeof getTimezone>>) => resp.data) as never,
    staleTime: 5 * 60 * 1000,
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

export const saveTimezone = (
  body: TimezoneIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<TimezoneOut>> =>
  axios.default.put("/api/v1/config/timezone", body, options);

export const useSaveTimezone = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof saveTimezone>>,
      TError,
      { data: TimezoneIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof saveTimezone>>,
  TError,
  { data: TimezoneIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};
  return useMutation({
    mutationFn: ({ data }: { data: TimezoneIn }) => saveTimezone(data, axiosOptions),
    ...mutationOptions,
  });
};

// ---------------------------------------------------------------------------
// Label definitions (admin-managed catalog of label keys + allowed values).
// The reserved key ``weight`` drives the weight selector on rule pages.
// ---------------------------------------------------------------------------

export interface LabelDefinition {
  key: string;
  description?: string | null;
  values: string[];
  allow_custom_values: boolean;
  /** Optional value → "#RRGGBB" color map for badge rendering. */
  value_colors?: Record<string, string> | null;
  /** Optional value → short description map (e.g. per-dimension explanations). */
  value_descriptions?: Record<string, string> | null;
  /**
   * Optional value → DQX criticality ("warn" | "error") map. Only meaningful
   * on the reserved ``severity`` key: the materializer reads it to decide
   * which criticality a registry rule's effective severity renders as (see
   * `registry_models.resolve_criticality`); unmapped values fall back to the
   * built-in defaults. Pruned to keys present in ``values`` on save.
   */
  value_criticality?: Record<string, string> | null;
  /**
   * True for reserved, pre-seeded keys (e.g. the Rules Registry
   * ``dimension``/``severity`` tags). Such keys cannot be deleted or
   * renamed via `saveLabelDefinitions`, though their values, colors, and
   * description may still be freely edited. Authoritative from the
   * server — a client cannot grant/strip this flag via the save payload.
   */
  is_builtin?: boolean;
}

export interface LabelDefinitionsOut {
  definitions: LabelDefinition[];
}

export interface LabelDefinitionsIn {
  definitions: LabelDefinition[];
}

export const getLabelDefinitions = (
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<LabelDefinitionsOut>> =>
  axios.default.get("/api/v1/config/label-definitions", options);

export const getLabelDefinitionsQueryKey = () => ["label-definitions"] as const;

export const useLabelDefinitions = <
  TData = Awaited<ReturnType<typeof getLabelDefinitions>>["data"],
  TError = AxiosError<unknown>,
>(
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getLabelDefinitions>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  return useQuery({
    queryKey: queryOptions?.queryKey ?? getLabelDefinitionsQueryKey(),
    queryFn: () => getLabelDefinitions(axiosOptions),
    select: ((resp: Awaited<ReturnType<typeof getLabelDefinitions>>) => resp.data) as never,
    staleTime: 5 * 60 * 1000,
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

export const saveLabelDefinitions = (
  body: LabelDefinitionsIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<LabelDefinitionsOut>> =>
  axios.default.put("/api/v1/config/label-definitions", body, options);

export const useSaveLabelDefinitions = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof saveLabelDefinitions>>,
      TError,
      { data: LabelDefinitionsIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof saveLabelDefinitions>>,
  TError,
  { data: LabelDefinitionsIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};
  return useMutation({
    mutationFn: ({ data }: { data: LabelDefinitionsIn }) => saveLabelDefinitions(data, axiosOptions),
    ...mutationOptions,
  });
};

// ---------------------------------------------------------------------------
// Retention settings — global vs. quarantine-specific DELETE windows
// surfaced for the admin Configuration page. Mirrors
// ``backend/routes/v1/config.py``.
// ---------------------------------------------------------------------------

export interface RetentionSettingsOut {
  retention_days: number;
  quarantine_retention_days: number;
  retention_days_default: number;
  quarantine_retention_days_default: number;
  retention_days_min: number;
  retention_days_max: number;
  retention_days_set: boolean;
  quarantine_retention_days_set: boolean;
}

export interface RetentionSettingsIn {
  retention_days?: number | null;
  quarantine_retention_days?: number | null;
}

export const getRetentionSettings = (
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RetentionSettingsOut>> =>
  axios.default.get("/api/v1/config/retention", options);

export const getRetentionSettingsQueryKey = () => ["retention-settings"] as const;

export const useRetentionSettings = <
  TData = Awaited<ReturnType<typeof getRetentionSettings>>["data"],
  TError = AxiosError<unknown>,
>(
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getRetentionSettings>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  return useQuery({
    queryKey: queryOptions?.queryKey ?? getRetentionSettingsQueryKey(),
    queryFn: () => getRetentionSettings(axiosOptions),
    select: ((resp: Awaited<ReturnType<typeof getRetentionSettings>>) => resp.data) as never,
    staleTime: 5 * 60 * 1000,
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

export const saveRetentionSettings = (
  body: RetentionSettingsIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RetentionSettingsOut>> =>
  axios.default.put("/api/v1/config/retention", body, options);

export const useSaveRetentionSettings = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof saveRetentionSettings>>,
      TError,
      { data: RetentionSettingsIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof saveRetentionSettings>>,
  TError,
  { data: RetentionSettingsIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};
  return useMutation({
    mutationFn: ({ data }: { data: RetentionSettingsIn }) => saveRetentionSettings(data, axiosOptions),
    ...mutationOptions,
  });
};

// ---------------------------------------------------------------------------
// Workspace host — used to build deep links into the Databricks workspace UI
// (e.g. Unity Catalog explorer pages). Accessible to all authenticated users;
// the linked pages enforce the caller's own permissions on arrival.
// ---------------------------------------------------------------------------

export interface WorkspaceHostOut {
  workspace_host: string;
}

export const getWorkspaceHost = (
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<WorkspaceHostOut>> =>
  axios.default.get("/api/v1/config/workspace-host", options);

export const getWorkspaceHostQueryKey = () => ["workspace-host"] as const;

export const useWorkspaceHost = <
  TData = Awaited<ReturnType<typeof getWorkspaceHost>>["data"],
  TError = AxiosError<unknown>,
>(
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getWorkspaceHost>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  return useQuery({
    queryKey: queryOptions?.queryKey ?? getWorkspaceHostQueryKey(),
    queryFn: () => getWorkspaceHost(axiosOptions),
    select: ((resp: Awaited<ReturnType<typeof getWorkspaceHost>>) => resp.data) as never,
    // The host is fixed for the lifetime of the app container — cache hard.
    staleTime: Infinity,
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

// ---------------------------------------------------------------------------
// Embedded dashboard (Insights page). The dashboard ID can be set by an
// admin via the Configuration page; when unset, the backend falls back to
// the env-provided DQX_DEFAULT_DASHBOARD_ID (so the bundle can ship a
// starter dashboard). ``is_set`` distinguishes admin override from env
// default in the UI.
// ---------------------------------------------------------------------------

export interface EmbeddedDashboardOut {
  dashboard_id: string;
  title: string | null;
  workspace_host: string;
  is_set: boolean;
  is_default: boolean;
}

export interface EmbeddedDashboardIn {
  dashboard_id: string;
  title?: string | null;
}

export const getEmbeddedDashboard = (
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<EmbeddedDashboardOut>> =>
  axios.default.get("/api/v1/config/embedded-dashboard", options);

export const getEmbeddedDashboardQueryKey = () => ["embedded-dashboard"] as const;

export const useEmbeddedDashboard = <
  TData = Awaited<ReturnType<typeof getEmbeddedDashboard>>["data"],
  TError = AxiosError<unknown>,
>(
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getEmbeddedDashboard>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  return useQuery({
    queryKey: queryOptions?.queryKey ?? getEmbeddedDashboardQueryKey(),
    queryFn: () => getEmbeddedDashboard(axiosOptions),
    select: ((resp: Awaited<ReturnType<typeof getEmbeddedDashboard>>) => resp.data) as never,
    staleTime: 60 * 1000,
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

export const saveEmbeddedDashboard = (
  body: EmbeddedDashboardIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<EmbeddedDashboardOut>> =>
  axios.default.put("/api/v1/config/embedded-dashboard", body, options);

export const useSaveEmbeddedDashboard = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof saveEmbeddedDashboard>>,
      TError,
      { data: EmbeddedDashboardIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof saveEmbeddedDashboard>>,
  TError,
  { data: EmbeddedDashboardIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};
  return useMutation({
    mutationFn: ({ data }: { data: EmbeddedDashboardIn }) => saveEmbeddedDashboard(data, axiosOptions),
    ...mutationOptions,
  });
};

export const deleteEmbeddedDashboard = (
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<EmbeddedDashboardOut>> =>
  axios.default.delete("/api/v1/config/embedded-dashboard", options);

export const useDeleteEmbeddedDashboard = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof deleteEmbeddedDashboard>>,
      TError,
      void,
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof deleteEmbeddedDashboard>>,
  TError,
  void,
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};
  return useMutation({
    mutationFn: () => deleteEmbeddedDashboard(axiosOptions),
    ...mutationOptions,
  });
};

// ---------------------------------------------------------------------------
// Run review statuses — admin-managed catalogue of values surfaced as the
// per-run review dropdown on the Runs detail page and as a multi-select
// filter on the Runs History page. Exactly one entry is flagged
// ``is_default`` (backend invariant); that value is what the listing
// endpoint returns virtually for unreviewed runs.
//
// The ``color`` field carries a design-system token name (gray, amber,
// green, blue, red, purple, ...) that the UI maps to a tailwind palette
// so we can rebrand without touching backend data.
// ---------------------------------------------------------------------------

export interface RunReviewStatusOption {
  value: string;
  description: string;
  color: string;
  is_default: boolean;
}

export interface RunReviewStatusesOut {
  statuses: RunReviewStatusOption[];
}

export interface RunReviewStatusesIn {
  statuses: RunReviewStatusOption[];
}

export const getRunReviewStatuses = (
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RunReviewStatusesOut>> =>
  axios.default.get("/api/v1/config/run-review-statuses", options);

export const getRunReviewStatusesQueryKey = () => ["run-review-statuses"] as const;

export const useRunReviewStatuses = <
  TData = Awaited<ReturnType<typeof getRunReviewStatuses>>["data"],
  TError = AxiosError<unknown>,
>(
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getRunReviewStatuses>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  return useQuery({
    queryKey: queryOptions?.queryKey ?? getRunReviewStatusesQueryKey(),
    queryFn: () => getRunReviewStatuses(axiosOptions),
    select: ((resp: Awaited<ReturnType<typeof getRunReviewStatuses>>) => resp.data) as never,
    // The dropdown is rendered in three places (Config card, Runs
    // detail, Runs History filter) — a 5-min stale window keeps the
    // navigation fast while letting admin saves propagate naturally
    // through React Query's invalidation on the mutation hook below.
    staleTime: 5 * 60 * 1000,
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

export const saveRunReviewStatuses = (
  body: RunReviewStatusesIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RunReviewStatusesOut>> =>
  axios.default.put("/api/v1/config/run-review-statuses", body, options);

export const useSaveRunReviewStatuses = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof saveRunReviewStatuses>>,
      TError,
      { data: RunReviewStatusesIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof saveRunReviewStatuses>>,
  TError,
  { data: RunReviewStatusesIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};
  return useMutation({
    mutationFn: ({ data }: { data: RunReviewStatusesIn }) => saveRunReviewStatuses(data, axiosOptions),
    ...mutationOptions,
  });
};

// ---------------------------------------------------------------------------
// Per-run review status — set/clear/get/history endpoints for the dropdown
// + audit list on the Runs detail page. ``is_default`` distinguishes the
// virtual catalogue default (no row in dq_run_review_status) from an
// explicit value — the UI uses it to render "(auto)" hints and skip
// meaningless updated_by/updated_at metadata.
// ---------------------------------------------------------------------------

export interface RunReviewStatusOut {
  run_id: string;
  status: string;
  updated_by: string | null;
  updated_at: string | null;
  is_default: boolean;
}

export interface SetRunReviewStatusIn {
  status: string;
}

export interface RunReviewStatusHistoryEntry {
  run_id: string;
  status: string;
  previous_status: string | null;
  changed_by: string;
  changed_at: string | null;
}

export interface RunReviewStatusHistoryOut {
  history: RunReviewStatusHistoryEntry[];
}

export const getRunReviewStatus = (
  runId: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RunReviewStatusOut>> =>
  axios.default.get(`/api/v1/runs/${encodeURIComponent(runId)}/review-status`, options);

export const getRunReviewStatusQueryKey = (runId: string) =>
  ["run-review-status", runId] as const;

export const useRunReviewStatus = <
  TData = Awaited<ReturnType<typeof getRunReviewStatus>>["data"],
  TError = AxiosError<unknown>,
>(
  runId: string,
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getRunReviewStatus>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  return useQuery({
    queryKey: queryOptions?.queryKey ?? getRunReviewStatusQueryKey(runId),
    queryFn: () => getRunReviewStatus(runId, axiosOptions),
    select: ((resp: Awaited<ReturnType<typeof getRunReviewStatus>>) => resp.data) as never,
    enabled: Boolean(runId),
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};

export const setRunReviewStatus = (
  runId: string,
  body: SetRunReviewStatusIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RunReviewStatusOut>> =>
  axios.default.put(`/api/v1/runs/${encodeURIComponent(runId)}/review-status`, body, options);

export const useSetRunReviewStatus = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof setRunReviewStatus>>,
      TError,
      { runId: string; data: SetRunReviewStatusIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof setRunReviewStatus>>,
  TError,
  { runId: string; data: SetRunReviewStatusIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};
  return useMutation({
    mutationFn: ({ runId, data }: { runId: string; data: SetRunReviewStatusIn }) =>
      setRunReviewStatus(runId, data, axiosOptions),
    ...mutationOptions,
  });
};

export const clearRunReviewStatus = (
  runId: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RunReviewStatusOut>> =>
  axios.default.delete(`/api/v1/runs/${encodeURIComponent(runId)}/review-status`, options);

export const useClearRunReviewStatus = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof clearRunReviewStatus>>,
      TError,
      { runId: string },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof clearRunReviewStatus>>,
  TError,
  { runId: string },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};
  return useMutation({
    mutationFn: ({ runId }: { runId: string }) => clearRunReviewStatus(runId, axiosOptions),
    ...mutationOptions,
  });
};

export const getRunReviewStatusHistory = (
  runId: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<RunReviewStatusHistoryOut>> =>
  axios.default.get(`/api/v1/runs/${encodeURIComponent(runId)}/review-status/history`, options);

export const getRunReviewStatusHistoryQueryKey = (runId: string) =>
  ["run-review-status-history", runId] as const;

export const useRunReviewStatusHistory = <
  TData = Awaited<ReturnType<typeof getRunReviewStatusHistory>>["data"],
  TError = AxiosError<unknown>,
>(
  runId: string,
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getRunReviewStatusHistory>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  return useQuery({
    queryKey: queryOptions?.queryKey ?? getRunReviewStatusHistoryQueryKey(runId),
    queryFn: () => getRunReviewStatusHistory(runId, axiosOptions),
    select: ((resp: Awaited<ReturnType<typeof getRunReviewStatusHistory>>) => resp.data) as never,
    enabled: Boolean(runId),
    ...queryOptions,
  }) as UseQueryResult<TData, TError>;
};
