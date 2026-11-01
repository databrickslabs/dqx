import type { CreateRegistryRuleIn, RegistryRuleOut, RuleDefinition } from "@/lib/api";
import { chunk } from "@/lib/bulk-contract-import";
import { previewBatchImportRegistryRules, type BatchImportRuleMatch } from "@/lib/api-custom";

/** An existing rule that runs the same check as some imported rules, with different settings. */
export interface SimilarRuleChoice {
  rule: RegistryRuleOut;
  /** Imported rules (indexes into the previewed list) this rule could replace. */
  inputIndexes: number[];
}

/** Looks up every imported rule against the registry, chunked to the batch-import cap. */
export async function previewSimilarRules(
  rules: CreateRegistryRuleIn[],
  generalizeSlots: boolean,
): Promise<BatchImportRuleMatch[]> {
  const matches: BatchImportRuleMatch[] = [];
  let offset = 0;
  for (const part of chunk(rules)) {
    const resp = await previewBatchImportRegistryRules({
      rules: part,
      generalize_slots: generalizeSlots,
    });
    for (const match of resp.data.matches ?? []) {
      matches.push({ ...match, index: match.index + offset });
    }
    offset += part.length;
  }
  return matches;
}

/** Imported rules with no identical rule, grouped under their best similar candidate:
 *  the first one whose settings don't conflict with the import, else the top-ranked one. */
export function groupSimilarRules(
  matches: BatchImportRuleMatch[],
  inputs: Pick<CreateRegistryRuleIn, "definition">[],
): SimilarRuleChoice[] {
  const byRule = new Map<string, SimilarRuleChoice>();
  for (const match of matches) {
    const similar = match.exact ? [] : match.similar ?? [];
    const imported = inputs[match.index]?.definition;
    const candidate = similar.find((r) => !hasConflictingSettings(r.definition, imported)) ?? similar[0];
    if (!candidate) continue;
    const choice = byRule.get(candidate.rule_id);
    if (choice) choice.inputIndexes.push(match.index);
    else byRule.set(candidate.rule_id, { rule: candidate, inputIndexes: [match.index] });
  }
  return [...byRule.values()];
}

function setParameters(definition: Pick<RuleDefinition, "parameters"> | undefined): Map<string, string> {
  return new Map(
    (definition?.parameters ?? [])
      .filter((p) => p.value !== null && p.value !== undefined && p.value !== "")
      .map((p) => [p.name, JSON.stringify(p.value)]),
  );
}

/** Whether both rules set some parameter, to different values (e.g. `case: upper` vs `case: lower`). */
export function hasConflictingSettings(
  a: Pick<RuleDefinition, "parameters"> | undefined,
  b: Pick<RuleDefinition, "parameters"> | undefined,
): boolean {
  const right = setParameters(b);
  return [...setParameters(a)].some(([name, value]) => right.has(name) && right.get(name) !== value);
}

/** Similar rules to reuse by default: those whose settings only add to or omit the imported ones. */
export function defaultReuseRuleIds(
  choices: SimilarRuleChoice[],
  inputs: Pick<CreateRegistryRuleIn, "definition">[],
): Set<string> {
  return new Set(
    choices
      .filter(
        (c) => !c.inputIndexes.some((i) => hasConflictingSettings(c.rule.definition, inputs[i]?.definition)),
      )
      .map((c) => c.rule.rule_id),
  );
}

/** "trim_strings: true, limit: 0" — or null when every parameter is left at its default. */
export function parameterSummary(
  definition: Pick<RuleDefinition, "parameters"> | undefined,
): string | null {
  const parts = (definition?.parameters ?? [])
    .filter((p) => p.value !== null && p.value !== undefined && p.value !== "")
    .map((p) => `${p.name}: ${typeof p.value === "string" ? p.value : JSON.stringify(p.value)}`);
  return parts.length > 0 ? parts.join(", ") : null;
}
