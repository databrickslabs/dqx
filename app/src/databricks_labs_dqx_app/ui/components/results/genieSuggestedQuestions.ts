// Ported from dqlake's `components/results/genieSuggestedQuestions.ts` (the
// spec — keep questions/categories/preamble semantics aligned with it).
// Sanctioned deviations:
// - Category labels are i18n KEYS (translated at render time) — they are pure
//   panel chrome, never sent to Genie.
// - The QUESTIONS themselves stay canonical ENGLISH both on the chips and in
//   the message sent to Genie: the SP-owned space is taught these exact
//   English questions (curated example SQLs + instructions — P3.9), so they
//   behave like proper nouns of the space, the same way chart series keys are
//   pinned English (the ledger's P2.2 precedent). Translating the display
//   while sending English would show the user a question that differs from
//   the one asked; showing and sending the same English string is the
//   faithful, least-surprising behaviour.
// - `buildContextPreamble` accepts the product's member-table FQNs: this app
//   has no product-level view in the space, so the preamble carries the
//   member tables and the P3.9 instructions route on that list.

/** What the chat was opened against — drives how suggested questions are
 *  worded and scoped. "rule" is the registry-rule results surface (B2-21):
 *  aggregates for one rule across the applied tables the viewer can see. */
export type GenieContextKind = "table" | "product" | "rule";

/** Direction of the most recent score move, when the opening context knows it.
 *  Lets the "change since last run" prompt be worded as increase/decrease
 *  instead of the neutral "change". */
export type ScoreDirection = "up" | "down";

export type SuggestedCategory = {
  /** i18n key for the category heading (display-only chrome). */
  labelKey: string;
  /** Canonical English questions — displayed AND sent verbatim. */
  questions: string[];
};

/**
 * Curated, context-aware suggested questions. A small set of labelled
 * categories.
 *
 * The chip text omits the subject entirely — being in a table/product's chat
 * panel already implies it, so "...for this table" reads as redundant. The
 * concrete subject is injected programmatically into the message SENT to Genie
 * via {@link buildContextPreamble}, so Genie still knows what's being asked
 * about without the chip spelling it out.
 *
 * Questions are otherwise neutral — they don't presume quality went up or
 * down — except the "change since last run" prompt, which is worded by
 * `direction` when the opening context knows which way the score moved.
 */
export function buildSuggestedQuestions(
  kind: GenieContextKind | undefined,
  direction?: ScoreDirection,
): SuggestedCategory[] {
  // Word the "change since last run" prompt by direction when known.
  const changeVerb =
    direction === "up" ? "increase" : direction === "down" ? "decrease" : "change";
  const sinceLastRun = `Why did my DQ score ${changeVerb} since the last run?`;

  if (kind === "rule") {
    // Rule-scoped: the surface aggregates ONE rule across every applied table
    // the viewer can see, so questions are worded around "this rule" and its
    // per-table / per-column spread rather than a single table's health.
    return [
      {
        labelKey: "genie.categoryBasicStats",
        questions: [
          `What is this rule's overall pass rate?`,
          `How many tables is this rule applied to?`,
          `How many tests has this rule run in the latest run?`,
          `How many failures does this rule have right now?`,
        ],
      },
      {
        labelKey: "genie.categoryDrilldown",
        questions: [
          `Which tables is this rule failing on most?`,
          `Which columns does this rule fail on most?`,
          `Where are this rule's most severe failures right now?`,
        ],
      },
      {
        labelKey: "genie.categoryTrends",
        questions: [
          `How has this rule's pass rate changed over recent runs?`,
          `How have this rule's failures been changing over time?`,
          `What is driving the changes in this rule's score over time?`,
        ],
      },
      {
        labelKey: "genie.categoryDiagnose",
        questions: [
          `Why did this rule's pass rate ${changeVerb} since the last run?`,
          `Which table is hurting this rule's score the most?`,
          `What is the biggest factor affecting this rule's pass rate?`,
        ],
      },
    ];
  }

  if (kind === "product") {
    return [
      {
        labelKey: "genie.categoryBasicStats",
        questions: [
          `What is the overall data quality score?`,
          `Which tables have the lowest pass rate?`,
          `How many rules are running?`,
          `How many rules have been added recently?`,
        ],
      },
      {
        labelKey: "genie.categoryDrilldown",
        questions: [
          `Which rules are failing most?`,
          `Which quality dimensions are weakest?`,
          `What are my most severe issues right now?`,
        ],
      },
      {
        labelKey: "genie.categoryTrends",
        questions: [
          `How has the quality score changed over the last few runs?`,
          `How has my DQ score by severity been changing over time?`,
          `What is driving my changes in score over time?`,
        ],
      },
      {
        labelKey: "genie.categoryDiagnose",
        questions: [
          sinceLastRun,
          `Why has my score by dimension changed?`,
          `What is the biggest factor affecting my DQ score?`,
        ],
      },
    ];
  }

  // Default to table-scoped questions. dqlake's row-level prompts ("Show me
  // the rows that failed." / "What are the failing rows with the most number
  // of rules failed?") are dropped: the SP-owned space is aggregates-only
  // (P3.8 controller decision) — row samples remain app-only, OBO-gated.
  return [
    {
      labelKey: "genie.categoryBasicStats",
      questions: [
        `What is the current data quality score?`,
        `How many tests failed in the latest run?`,
        `How many rules do I have?`,
        `How many rules have been added recently?`,
      ],
    },
    {
      labelKey: "genie.categoryDrilldown",
      questions: [
        `Which rules are failing?`,
        `Which columns have the most failures?`,
        `What are my most severe issues right now?`,
      ],
    },
    {
      labelKey: "genie.categoryTrends",
      questions: [
        `How has the score changed over recent runs?`,
        `How has my DQ score by severity been changing over time?`,
        `What is driving my changes in score over time?`,
      ],
    },
    {
      labelKey: "genie.categoryDiagnose",
      questions: [
        sinceLastRun,
        `Why has my score by dimension changed?`,
        `What is the biggest factor affecting my DQ score?`,
      ],
    },
  ];
}

/**
 * Builds the subject preamble appended to a question before it is sent to
 * Genie, so the generic chip text resolves to a concrete subject. Returns an
 * empty string when there's nothing useful to inject (Genie falls back to
 * the bare question).
 *
 * `subject` should be the most specific identifier available — for a table,
 * the fully-qualified `catalog.schema.table`; for a product, its name.
 *
 * For products, `memberTables` (the member FQNs) is appended so the space can
 * scope the answer: no product-level object exists in the space, and the
 * P3.9 space instructions route product questions on this table list.
 */
export function buildContextPreamble(
  kind: GenieContextKind | undefined,
  subject: string | undefined,
  memberTables?: string[],
): string {
  const trimmed = subject?.trim();
  if (!trimmed) return "";
  if (kind === "product") {
    const tables = (memberTables ?? []).map((t) => t.trim()).filter(Boolean);
    return tables.length
      ? `(Data product: ${trimmed} — tables: ${tables.join(", ")})`
      : `(Data product: ${trimmed})`;
  }
  if (kind === "rule") {
    return `(Rule: ${trimmed})`;
  }
  return `(Table: ${trimmed})`;
}

/**
 * Injects the subject context into a question being sent to Genie. The chip /
 * typed text stays generic; this appends a one-line subject hint so Genie
 * answers about the right table or product.
 */
export function withContext(
  question: string,
  kind: GenieContextKind | undefined,
  subject: string | undefined,
  memberTables?: string[],
): string {
  const preamble = buildContextPreamble(kind, subject, memberTables);
  return preamble ? `${question}\n\n${preamble}` : question;
}
