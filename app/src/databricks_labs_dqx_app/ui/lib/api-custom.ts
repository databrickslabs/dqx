/**
 * Custom API hooks for endpoints not yet in the auto-generated api.ts.
 * These will be replaced by orval-generated hooks once the OpenAPI spec is regenerated.
 */
import { useMutation, useQuery } from "@tanstack/react-query";
import type { UseMutationOptions, UseMutationResult, UseQueryOptions, UseQueryResult } from "@tanstack/react-query";
import * as axios from "axios";
import type { AxiosError, AxiosRequestConfig, AxiosResponse } from "axios";
import type { RuleCatalogEntryOut, RunStatusOut, ProfileRunIn, BatchProfileRunIn } from "./api";
import { submitProfileRun, submitBatchProfileRun } from "./api";

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
// Table preview — sample rows for the Preview Data block
// ---------------------------------------------------------------------------

export interface TablePreviewOut {
  columns: string[];
  rows: Record<string, string | null>[];
  row_count: number;
}

export const getTablePreview = (
  catalog: string,
  schema: string,
  table: string,
  limit = 10,
  filterQuery?: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<TablePreviewOut>> => {
  const params: Record<string, unknown> = { limit };
  if (filterQuery && filterQuery.trim()) params.filter_query = filterQuery.trim();
  return axios.default.get(
    `/api/v1/discovery/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/preview`,
    { ...options, params },
  );
};

// ---------------------------------------------------------------------------
// Preview dry run — run checks inline against preview rows (no job submission)
// ---------------------------------------------------------------------------

export interface PreviewDryRunIn {
  table_fqn: string;
  checks: Array<Record<string, unknown>>;
  rows: Record<string, unknown>[];
}

export interface PreviewRowResult {
  row: Record<string, unknown>;
  errors: string[];
  warnings: string[];
}

export interface PreviewDryRunOut {
  table_fqn: string;
  total_rows: number;
  error_rows: number;
  warning_rows: number;
  pass_rows: number;
  rows: PreviewRowResult[];
}

export const runDryRunOnPreview = (
  body: PreviewDryRunIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<PreviewDryRunOut>> => {
  return axios.default.post(`/api/v1/dryrun/run-on-preview`, body, options);
};

export interface TableDryRunIn {
  table_fqn: string;
  checks: Array<Record<string, unknown>>;
  sample_size?: number;
}

export const runDryRunOnTable = (
  body: TableDryRunIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<PreviewDryRunOut>> => {
  return axios.default.post(`/api/v1/dryrun/run-on-table`, body, options);
};

// ---------------------------------------------------------------------------
// Push rules to Delta table
// ---------------------------------------------------------------------------

export interface PushToTableIn {
  checks: Array<Record<string, unknown>>;
  target_table: string;
  run_config_name: string;
  mode?: "append" | "overwrite";
}

export interface PushToTableOut {
  message: string;
  pushed_count: number;
}

export const pushRulesToTable = (
  body: PushToTableIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<PushToTableOut>> => {
  return axios.default.post(`/api/v1/rules/push-to-table`, body, options);
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

export const listQuarantineRecords = (
  runId: string,
  params?: { offset?: number; limit?: number },
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<QuarantineListOut>> => {
  return axios.default.get(`/api/v1/quarantine/runs/${runId}`, {
    ...options,
    params,
  });
};

export const getQuarantineRecordsQueryKey = (runId: string, offset?: number, limit?: number) =>
  [`/api/v1/quarantine/runs`, runId, offset, limit] as const;

export const useListQuarantineRecords = <
  TData = Awaited<ReturnType<typeof listQuarantineRecords>>,
  TError = AxiosError<unknown>,
>(
  runId: string,
  params?: { offset?: number; limit?: number },
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof listQuarantineRecords>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  const queryKey =
    queryOptions?.queryKey ?? getQuarantineRecordsQueryKey(runId, params?.offset, params?.limit);

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
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<{ count: number }>> => {
  return axios.default.get(`/api/v1/quarantine/runs/${runId}/count`, options);
};

export const getQuarantineCountQueryKey = (runId: string) =>
  [`/api/v1/quarantine/runs/count`, runId] as const;

export const useQuarantineCount = <
  TData = Awaited<ReturnType<typeof getQuarantineCount>>,
  TError = AxiosError<unknown>,
>(
  runId: string,
  options?: {
    query?: Partial<UseQueryOptions<Awaited<ReturnType<typeof getQuarantineCount>>, TError, TData>>;
    axios?: AxiosRequestConfig;
  },
): UseQueryResult<TData, TError> => {
  const { query: queryOptions, axios: axiosOptions } = options ?? {};
  const queryKey = queryOptions?.queryKey ?? getQuarantineCountQueryKey(runId);
  const queryFn = () => getQuarantineCount(runId, axiosOptions);
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
): void => {
  const url = `/api/v1/quarantine/runs/${runId}/export?format=${format}`;
  window.open(url, "_blank");
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
// Profiler run mutations (orval generates these as useQuery; wrap as useMutation)
// ---------------------------------------------------------------------------

export const useSubmitProfileRunMutation = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof submitProfileRun>>,
      TError,
      { data: ProfileRunIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof submitProfileRun>>,
  TError,
  { data: ProfileRunIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};
  return useMutation({
    mutationFn: ({ data }: { data: ProfileRunIn }) => submitProfileRun(data, axiosOptions),
    mutationKey: ["submitProfileRun"],
    ...mutationOptions,
  });
};

// ---------------------------------------------------------------------------
// Alerts — notification channel management
// ---------------------------------------------------------------------------

export interface AlertChannelIn {
  name: string;
  webhook_url: string;
  trigger: "all_runs" | "manual_only" | "scheduled_only";
  enabled: boolean;
  notify_dry_runs: boolean;
}

export interface AlertChannelOut {
  channel_id: string;
  name: string;
  webhook_url: string;
  trigger: string;
  enabled: boolean;
  notify_dry_runs: boolean;
}

export interface DryRunNotifyPayload {
  source_table_fqn: string;
  total_rows?: number | null;
  valid_rows?: number | null;
  error_rows?: number | null;
  warning_rows?: number | null;
  status?: string;
}

export interface NotifyRunsIn {
  run_ids: string[];
  trigger?: string;
}

export interface NotifyRunsOut {
  notified: number;
  skipped: number;
  errors: string[];
}

export const listAlertChannels = (
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<AlertChannelOut[]>> => {
  return axios.default.get(`/api/v1/alerts/channels`, options);
};

export const getAlertChannelsQueryKey = () => ["alerts", "channels"] as const;

export const useListAlertChannels = <TError = AxiosError<unknown>>(
  options?: UseQueryOptions<AlertChannelOut[], TError>,
): UseQueryResult<AlertChannelOut[], TError> => {
  return useQuery({
    queryKey: getAlertChannelsQueryKey(),
    queryFn: () => listAlertChannels().then((r) => r.data),
    ...options,
  });
};

export const createAlertChannel = (
  body: AlertChannelIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<AlertChannelOut>> => {
  return axios.default.post(`/api/v1/alerts/channels`, body, options);
};

export const updateAlertChannel = (
  channelId: string,
  body: AlertChannelIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<AlertChannelOut>> => {
  return axios.default.put(`/api/v1/alerts/channels/${channelId}`, body, options);
};

export const deleteAlertChannel = (
  channelId: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<void>> => {
  return axios.default.delete(`/api/v1/alerts/channels/${channelId}`, options);
};

export const testAlertWebhook = (
  webhookUrl: string,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<{ success: boolean; message: string }>> => {
  return axios.default.post(`/api/v1/alerts/test`, { webhook_url: webhookUrl }, options);
};

export const notifyDryRunResult = (
  payload: DryRunNotifyPayload,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<NotifyRunsOut>> => {
  return axios.default.post(`/api/v1/alerts/notify-result`, payload, options);
};

export const notifyRuns = (
  body: NotifyRunsIn,
  options?: AxiosRequestConfig,
): Promise<AxiosResponse<NotifyRunsOut>> => {
  return axios.default.post(`/api/v1/alerts/notify`, body, options);
};

export const useSubmitBatchProfileRunMutation = <
  TError = AxiosError<unknown>,
  TContext = unknown,
>(
  options?: {
    mutation?: UseMutationOptions<
      Awaited<ReturnType<typeof submitBatchProfileRun>>,
      TError,
      { data: BatchProfileRunIn },
      TContext
    >;
    axios?: AxiosRequestConfig;
  },
): UseMutationResult<
  Awaited<ReturnType<typeof submitBatchProfileRun>>,
  TError,
  { data: BatchProfileRunIn },
  TContext
> => {
  const { mutation: mutationOptions, axios: axiosOptions } = options ?? {};
  return useMutation({
    mutationFn: ({ data }: { data: BatchProfileRunIn }) => submitBatchProfileRun(data, axiosOptions),
    mutationKey: ["submitBatchProfileRun"],
    ...mutationOptions,
  });
};
