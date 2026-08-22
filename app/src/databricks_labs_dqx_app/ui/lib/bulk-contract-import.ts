import type { ColumnOut, RuleSlot } from "@/lib/api";
import { buildSlotMapping } from "@/lib/slot-mapping";

/**
 * Bulk contract import — pure, unit-testable helpers.
 *
 * The orchestration itself (sequencing generate → bulk-register → batch-import
 * → apply) lives in {@link ../components/registry-rules/BulkContractImportWorkspace}
 * because it needs React Query / the axios client. Everything that can be a
 * plain function — FQN resolution, auto-map classification, chunking, and
 * per-contract result aggregation — is factored out here so it can be tested
 * without a DOM or a running backend.
 */

/**
 * `batchImportRegistryRules` blocks the uvicorn worker for the whole request
 * (documented DoS constraint, capped server-side at `BATCH_IMPORT_MAX_RULES`).
 * We chunk each per-schema import to this size to stay under that cap and keep
 * each request bounded.
 */
export const BULK_IMPORT_CHUNK_SIZE = 500;

/** True when `name` is a fully-qualified `catalog.schema.table` (exactly three non-empty parts). */
export function isThreePartFqn(name: string): boolean {
  const parts = name.split(".");
  return parts.length === 3 && parts.every((p) => p.trim() !== "");
}

/**
 * Resolve one ODCS schema to a Unity Catalog table FQN.
 *
 * - If the contract's `physical_name` is already a 3-part FQN, use it verbatim
 *   (the contract fully specifies where the table lives).
 * - Otherwise place it under the batch-level target `catalog.schema`, using the
 *   `physical_name` as the table component when present, else the ODCS
 *   `schema_name`. A dotted-but-not-3-part `physical_name` (e.g. `schema.table`)
 *   collapses to its last segment so we never emit an invalid >3-part FQN.
 *
 * Returns `null` when the FQN can't be formed (missing target catalog/schema or
 * an empty table name) so the caller can report the schema as unresolvable
 * rather than register a malformed FQN.
 */
export function resolveTableFqn(args: {
  schemaName: string;
  physicalName: string | null | undefined;
  targetCatalog: string;
  targetSchema: string;
}): string | null {
  const phys = (args.physicalName ?? "").trim();
  if (phys && isThreePartFqn(phys)) return phys;

  const catalog = args.targetCatalog.trim();
  const schema = args.targetSchema.trim();
  // Collapse any dotted (non-FQN) physical name to its final segment; fall back
  // to the ODCS schema name when no physical name was supplied.
  const rawTable = phys || args.schemaName;
  const table = rawTable.includes(".") ? (rawTable.split(".").pop() ?? "").trim() : rawTable.trim();

  if (!catalog || !schema || !table) return null;
  return `${catalog}.${schema}.${table}`;
}

/**
 * How a single generated rule can be applied to its resolved table, given the
 * rule's slots and (when known) the target table's columns.
 *
 * - `whole-table`: no slots — applies with an empty mapping group, no columns
 *   needed.
 * - `auto`: every slot best-effort name+family matches a column
 *   ({@link buildSlotMapping} succeeds).
 * - `manual`: at least one slot can't be auto-matched → needs a human to map it
 *   from the table's Apply Rules tab.
 * - `unknown`: the table's columns couldn't be inspected (doesn't exist yet /
 *   no permission), so coverage can't be computed in preview.
 */
export type RuleMapStatus = "whole-table" | "auto" | "manual" | "unknown";

export function classifyRuleMapping(slots: RuleSlot[], columns: ColumnOut[] | null): RuleMapStatus {
  if (slots.length === 0) return "whole-table";
  if (columns === null) return "unknown";
  return buildSlotMapping(slots, columns) === null ? "manual" : "auto";
}

/** Splits `items` into consecutive chunks of at most `size` (defaults to {@link BULK_IMPORT_CHUNK_SIZE}). */
export function chunk<T>(items: T[], size: number = BULK_IMPORT_CHUNK_SIZE): T[][] {
  if (size <= 0) throw new Error("chunk size must be positive");
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

/** A table a user still needs to visit (to finish manual mapping or after approval). */
export interface AttentionTable {
  fqn: string;
  /** null when the table couldn't be registered/resolved, so there's no binding to deep-link to. */
  bindingId: string | null;
  /** Rules staged awaiting approval (auto-apply once approved). */
  pending: number;
  /** Rules that need manual column mapping in the Apply Rules tab. */
  needsMapping: number;
}

/** Outcome of processing one (contract, schema) group during execute. */
export interface SchemaOutcome {
  contractId: string;
  filename: string;
  tableFqn: string | null;
  bindingId: string | null;
  /** 1 when this schema's table was newly registered (0 if skipped/existing/invalid). */
  registered: number;
  /** 1 when this schema's table was already monitored (in the register call's
   *  `skipped_existing`), so it was left untouched rather than newly registered. */
  alreadyMonitored: number;
  /** Registry rules successfully created (drafted/submitted) for this schema. */
  created: number;
  /** Rules matched to an existing active rule (skip_duplicates) — reused, not created. */
  reused: number;
  /** Created rules auto-applied to the table (approved + auto-mappable). */
  applied: number;
  /** Created rules left awaiting approval before they can be applied. */
  pending: number;
  /** Approved rules whose slots couldn't be auto-mapped → need manual mapping. */
  needsMapping: number;
  /** Rules that failed to create or apply. */
  failed: number;
  errors: string[];
}

/** Roll a flat list of per-schema outcomes up into one summary row per contract file. */
export interface ContractResult {
  contractId: string;
  filename: string;
  registered: number;
  alreadyMonitored: number;
  created: number;
  reused: number;
  applied: number;
  pending: number;
  needsMapping: number;
  failed: number;
  errors: string[];
  /** Tables with pending-approval or needs-manual-mapping rules, for deep-links. */
  attentionTables: AttentionTable[];
}

export function aggregateByContract(outcomes: SchemaOutcome[]): ContractResult[] {
  const byContract = new Map<string, ContractResult>();
  for (const o of outcomes) {
    let row = byContract.get(o.contractId);
    if (!row) {
      row = {
        contractId: o.contractId,
        filename: o.filename,
        registered: 0,
        alreadyMonitored: 0,
        created: 0,
        reused: 0,
        applied: 0,
        pending: 0,
        needsMapping: 0,
        failed: 0,
        errors: [],
        attentionTables: [],
      };
      byContract.set(o.contractId, row);
    }
    row.registered += o.registered;
    row.alreadyMonitored += o.alreadyMonitored;
    row.created += o.created;
    row.reused += o.reused;
    row.applied += o.applied;
    row.pending += o.pending;
    row.needsMapping += o.needsMapping;
    row.failed += o.failed;
    if (o.errors.length > 0) row.errors.push(...o.errors);
    if ((o.pending > 0 || o.needsMapping > 0) && o.tableFqn) {
      row.attentionTables.push({
        fqn: o.tableFqn,
        bindingId: o.bindingId,
        pending: o.pending,
        needsMapping: o.needsMapping,
      });
    }
  }
  return [...byContract.values()];
}
