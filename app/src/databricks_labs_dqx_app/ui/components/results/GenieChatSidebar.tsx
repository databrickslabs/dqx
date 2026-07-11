import { Suspense, useEffect, useRef, useState } from "react";
import { QueryErrorResetBoundary } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useTranslation } from "react-i18next";
import {
  ThumbsUp,
  ThumbsDown,
  Sparkles,
  Plus,
  GripVertical,
  X,
  ExternalLink,
} from "lucide-react";

import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Collapsible,
  CollapsibleTrigger,
  CollapsibleContent,
} from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import {
  AI_BANNER_BG,
  AI_BANNER_BORDER,
  AI_GRADIENT_URL,
} from "@/lib/ai-style";
import {
  useGetGenieSpace,
  useGetGenieSpaceSuspense,
  useStartGenieMessage,
  usePollGenieMessage,
  useSubmitGenieFeedback,
  type GenieSpaceOut,
} from "@/lib/api";
import selector from "@/lib/selector";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { GenieMarkdown } from "./GenieMarkdown";
import { GenieResultTable } from "./GenieResultTable";
import { GenieResultChart, planChart } from "./GenieResultChart";
import { GenieIcon } from "./GenieIcon";
import {
  buildSuggestedQuestions,
  withContext,
  type GenieContextKind,
  type ScoreDirection,
} from "./genieSuggestedQuestions";
import {
  appendMessage,
  resetConversation,
  setConversationId,
  useConversation,
  type ChatMessage,
} from "./genieConversationStore";
import { useGeniePanelWidth } from "./useGeniePanelWidth";

// Ported from dqlake's `components/results/GenieChatSidebar.tsx` (the spec —
// keep the skeleton/provisioning/error states, chips, bubbles, SQL
// collapsible, feedback, and the 1s x60 poll loop aligned with it).
// Sanctioned deviations only:
// - hooks re-pointed to the /api/v1/genie endpoints (start/poll/feedback +
//   space; the shapes were built to match dqlake's near-verbatim);
// - all user-facing strings via t() (the suggested QUESTIONS stay canonical
//   English — see genieSuggestedQuestions.ts);
// - the space-availability query carries RESULTS_QUERY_OPTIONS (no idle
//   refetch) PLUS dqlake's 4s provisioning refetchInterval, which overrides
//   staleTime while the space is still provisioning. The message poll loop
//   below is NOT idle polling — it runs only while a question is in flight
//   and stops at a terminal status, exactly dqlake's semantics;
// - `contextTables` (product member FQNs) threads into withContext so the
//   product preamble carries its tables (P3.9 instructions route on it).

// The internal "no stage yet" sentinel. Never displayed (the pending bubble
// suppresses it); backend-provided stage strings replace it as they arrive.
const PENDING_DEFAULT = "Thinking";

/** Keyword heuristic on the user's OWN question: did they deliberately ask to
 *  see a table / rows / list / records? Whole-word matches so "rules" never
 *  trips "rows"; conservative by design — when unsure, no table. */
const TABLE_INTENT_RE =
  /\b(tables?|rows?|lists?|records?|samples?|examples?)\b|show me the (records|rows|data|examples)/i;

function userAskedForTable(question: string | null | undefined): boolean {
  return question != null && TABLE_INTENT_RE.test(question);
}

/** Whether a Genie result is literal SAMPLE / raw-row data rather than an
 *  aggregate the prose already conveys. The SP-owned space is aggregates-only
 *  (P3.8), so this is rare, but two signals are unambiguous: a `failing_record`
 *  column (row-level failing records the table expands into the dataset's own
 *  columns) or a raw `SELECT *` dump of a table's rows. */
function isSampleDataResult(
  columns: string[],
  sql: string | null | undefined,
): boolean {
  if (columns.includes("failing_record")) return true;
  return sql != null && /\bselect\s+\*/i.test(sql);
}

/** The user question a given assistant message is answering — the nearest
 *  preceding user turn. Feeds the table-intent heuristic below. */
function precedingUserText(messages: ChatMessage[], index: number): string | undefined {
  for (let j = index - 1; j >= 0; j--) {
    if (messages[j].role === "user") return messages[j].text;
  }
  return undefined;
}

/**
 * Chart + table for a Genie result. The chart renders whenever planChart finds
 * a chartable shape (a clear category/time axis + numeric measure) — unchanged
 * (#66). The TABLE is suppressed BY DEFAULT and shown only when it IS the
 * answer the prose can't stand in for:
 *  - the result is literal sample data — a `failing_record` expansion or a raw
 *    `SELECT *` row dump (see isSampleDataResult); and/or
 *  - the user deliberately asked to see a table / rows / list / records (a
 *    keyword heuristic on their own question — see userAskedForTable).
 * Otherwise no table — conservative by construction. The chart is unaffected.
 */
function GenieResult({
  columns,
  rows,
  sql,
  question,
}: {
  columns: string[];
  rows: (string | null)[][];
  sql?: string | null;
  question?: string | null;
}) {
  const plan = planChart(columns, rows);
  const showTable = isSampleDataResult(columns, sql) || userAskedForTable(question);
  return (
    <>
      {plan && <GenieResultChart columns={columns} rows={rows} />}
      {showTable && <GenieResultTable columns={columns} rows={rows} />}
    </>
  );
}

/** Three bouncing dots in an assistant bubble — shown while an answer loads. */
function TypingDots() {
  const { t } = useTranslation();
  return (
    <span className="flex items-center gap-1" aria-label={t("genie.thinkingAria")}>
      {[0, 1, 2].map((i) => (
        <span
          key={i}
          className="h-1.5 w-1.5 rounded-full bg-current opacity-60 [animation:dq-typing-bounce_1.2s_ease-in-out_infinite]"
          style={{ animationDelay: `${i * 0.18}s` }}
        />
      ))}
    </span>
  );
}

/**
 * Loading state shaped like the panel it replaces: a couple of labelled
 * category groups with chip-sized shimmer rows, so the entrance doesn't jump
 * from a single grey block to the real content.
 */
function GeniePanelSkeleton() {
  const { t } = useTranslation();
  return (
    <div
      className="flex h-full flex-col gap-5 overflow-hidden"
      aria-label={t("genie.loadingAria")}
    >
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <GenieIcon className="h-4 w-4 animate-pulse" />
        <span>{t("genie.settingUp")}</span>
      </div>
      {[0, 1].map((group) => (
        <div key={group} className="flex flex-col gap-2">
          <div className="h-3 w-24 rounded bg-muted animate-pulse" />
          {[0, 1].map((row) => (
            <div
              key={row}
              className={cn(
                "h-9 w-full rounded-lg animate-pulse",
                AI_BANNER_BG,
                AI_BANNER_BORDER,
              )}
            />
          ))}
        </div>
      ))}
    </div>
  );
}

export function GenieChatBody({
  context,
  contextKind,
  contextSubject,
  contextTables,
  direction,
  autoAsk,
  scrollEnabled = true,
}: {
  /** Opaque scope key — also the per-conversation store key. */
  context?: string;
  /** Whether the chat was opened from a table's or a product's results. */
  contextKind?: GenieContextKind;
  /** Concrete subject injected into the message sent to Genie (not shown on
   *  chips): a fully-qualified `catalog.schema.table` or a product name. */
  contextSubject?: string;
  /** Product member-table FQNs, appended to the sent preamble so the space
   *  can scope product questions (no product object exists in the space). */
  contextTables?: string[];
  /** Direction of the latest score move, when known — words the
   *  "change since last run" prompt as increase/decrease. */
  direction?: ScoreDirection;
  /** When set, this question is sent automatically once on mount (used by the
   *  "Why did this drop?" entry point). */
  autoAsk?: string | null;
  /** False while the panel is sliding in — keeps scroll containers clipped so
   *  the entrance doesn't flash a scrollbar / snap height. Defaults true so the
   *  standalone body (and tests) scroll normally. */
  scrollEnabled?: boolean;
}) {
  const { t } = useTranslation();
  // Poll while the space is still provisioning so the panel flips to ready on
  // its own instead of sitting on a "getting ready…" state. Otherwise the
  // query is results-shaped: staleTime Infinity, no window-focus refetch.
  const { data: space } = useGetGenieSpaceSuspense<GenieSpaceOut>({
    query: {
      ...selector<GenieSpaceOut>().query,
      ...RESULTS_QUERY_OPTIONS,
      refetchInterval: (q) =>
        q.state.data?.data?.status === "provisioning" ? 4000 : false,
    },
  });

  // Messages + conversation_id live in a module-level store keyed by context,
  // so they survive the Sheet (and this body) unmounting on close.
  const { messages, conversationId } = useConversation(context);
  const inputRef = useRef<HTMLInputElement>(null);

  const start = useStartGenieMessage();
  const poll = usePollGenieMessage();
  const feedback = useSubmitGenieFeedback();
  const scrollRef = useRef<HTMLDivElement>(null);
  // Friendly stage label while an answer is in flight ("Writing SQL", "Running
  // query", …, backend-provided); null when idle.
  const [pending, setPending] = useState<string | null>(null);
  // Set on unmount so an in-flight poll loop stops touching state after the
  // panel closes. A reopen remounts the body with a fresh (false) ref.
  const abortedRef = useRef(false);
  useEffect(() => {
    abortedRef.current = false;
    return () => {
      abortedRef.current = true;
    };
  }, []);

  // Keep the newest message and the typing indicator in view.
  useEffect(() => {
    const el = scrollRef.current;
    // scrollTo is unimplemented in jsdom; guard so tests don't throw.
    if (el && typeof el.scrollTo === "function") {
      el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
    }
  }, [messages, pending]);

  async function send(question: string) {
    const trimmed = question.trim();
    if (!trimmed || pending !== null) return;

    // The chip / typed text stays generic ("this table"); inject the concrete
    // subject into the message Genie actually receives.
    const fullQuestion = withContext(trimmed, contextKind, contextSubject, contextTables);

    appendMessage(context, { role: "user", text: trimmed });
    if (inputRef.current) inputRef.current.value = "";
    setPending(PENDING_DEFAULT);

    try {
      // Kick off the message, then poll so the UI can show live stages instead
      // of one undifferentiated spinner.
      const { data: started } = await start.mutateAsync({
        data: { question: fullQuestion, conversation_id: conversationId },
      });
      if (started.error || !started.conversation_id || !started.message_id) {
        throw new Error(started.error ?? "genie did not start");
      }
      const cid = started.conversation_id;
      const mid = started.message_id;
      setConversationId(context, cid);
      setPending(started.stage ?? PENDING_DEFAULT);

      let last = started;
      for (let i = 0; i < 60; i++) {
        if (abortedRef.current) return;
        const { data: polled } = await poll.mutateAsync({
          data: { conversation_id: cid, message_id: mid },
        });
        last = polled;
        setPending(polled.stage ?? PENDING_DEFAULT);
        if (
          polled.status &&
          ["COMPLETED", "FAILED", "CANCELLED"].includes(polled.status)
        ) {
          break;
        }
        if (abortedRef.current) return;
        await new Promise((r) => setTimeout(r, 1000));
      }

      if (abortedRef.current) return;
      appendMessage(context, {
        role: "genie",
        text: last.answer_text ?? undefined,
        sql: last.sql,
        sqlDescription: last.sql_description,
        messageId: last.message_id,
        error: last.error,
        resultColumns: last.result_columns,
        resultRows: last.result_rows,
      });
    } catch {
      if (!abortedRef.current) {
        appendMessage(context, { role: "genie", error: "unreachable" });
      }
    } finally {
      if (!abortedRef.current) setPending(null);
    }
  }

  // Fire the auto-ask once, the first time we have a ready space and a prompt.
  const autoAskedRef = useRef(false);
  const ready =
    space.available && space.status !== "provisioning" && space.status !== "error";
  useEffect(() => {
    if (!autoAsk || autoAskedRef.current || !ready) return;
    autoAskedRef.current = true;
    void send(autoAsk);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoAsk, ready]);

  function vote(messageId: string | null | undefined, value: "up" | "down") {
    if (!messageId) return;
    // Best-effort; ignore failure.
    feedback.mutate({ data: { message_id: messageId, vote: value } });
  }

  // Provisioning / error states — calm, not an empty or raw-error panel.
  if (space.status === "error") {
    return (
      <div className="flex flex-col gap-3 text-sm">
        <p className="text-muted-foreground">{t("genie.notAvailable")}</p>
        <Button
          variant="outline"
          size="sm"
          className="self-start"
          onClick={() => window.location.reload()}
        >
          {t("genie.retry")}
        </Button>
      </div>
    );
  }

  if (!space.available || space.status === "provisioning") {
    return <GeniePanelSkeleton />;
  }

  const categories = buildSuggestedQuestions(contextKind, direction);

  return (
    // px-px pb-px: the input's focus ring is 1px and would otherwise be clipped
    // flush against the panel's overflow-hidden wrapper (uneven ring); a 1px
    // inset gives the ring room to draw evenly on all sides.
    <div className="flex h-full min-h-0 flex-col gap-4 px-px pb-px">
      {messages.length === 0 && categories.length > 0 && (
        // No own scroll region: the chip entrance (slide-in-from-bottom)
        // briefly overflows, which would flash a scrollbar and snap the
        // height when it settles. `overflow-hidden` here absorbs the slide;
        // the parent column owns any real scrolling.
        <div
          className={cn(
            // pb-16 leaves scroll room so the LAST question can clear the
            // bottom fade (the mask only dims the final ~48px of the viewport);
            // without it the bottom chips were trapped permanently in the fade.
            "flex min-h-0 flex-1 flex-col gap-4 overflow-x-hidden pb-16",
            scrollEnabled ? "overflow-y-auto" : "overflow-y-hidden",
          )}
          // When the list overflows, fade the last stretch out instead of
          // hard-cutting it at the input box — a softer "there's more below".
          style={
            scrollEnabled
              ? {
                  maskImage:
                    "linear-gradient(to bottom, black calc(100% - 48px), transparent)",
                  WebkitMaskImage:
                    "linear-gradient(to bottom, black calc(100% - 48px), transparent)",
                }
              : undefined
          }
        >
          {categories.map((cat, ci) => (
            <div key={cat.labelKey} className="flex flex-col gap-2">
              <p className="text-xs font-medium text-muted-foreground">
                {t(cat.labelKey)}
              </p>
              {cat.questions.map((q, qi) => (
                <button
                  key={q}
                  type="button"
                  onClick={() => void send(q)}
                  style={{ animationDelay: `${(ci * 2 + qi) * 120}ms` }}
                  className={cn(
                    "group flex rounded-lg px-3 py-2 text-left text-sm shadow-sm transition-shadow hover:shadow",
                    // Entrance stagger — its transform persists (fill-mode-both),
                    // so the hover "pop" lives on the inner wrapper below (a
                    // scale here would be overridden by the settled animation).
                    "animate-in fade-in slide-in-from-bottom-2 fill-mode-both duration-700 ease-out",
                    AI_BANNER_BG,
                    AI_BANNER_BORDER,
                  )}
                >
                  {/* Subtle hover "pop" (#95), matching the sample-data
                      suggested-question chips: a gentle spring-scale anchored at
                      the left edge, with a small press-in on click. Honors
                      reduced-motion. */}
                  <span className="flex items-start gap-2 origin-left transition-transform duration-200 ease-out group-hover:scale-[1.02] group-active:scale-[0.99] motion-reduce:transform-none">
                    <Sparkles
                      className="mt-0.5 h-3.5 w-3.5 shrink-0 opacity-70 transition-opacity group-hover:opacity-100"
                      stroke={AI_GRADIENT_URL}
                    />
                    <span className="font-light">{q}</span>
                  </span>
                </button>
              ))}
            </div>
          ))}
        </div>
      )}

      <div
        ref={scrollRef}
        className={cn(
          "flex flex-col gap-3",
          scrollEnabled ? "overflow-y-auto" : "overflow-y-hidden",
          // Only claim vertical space once there's a conversation; otherwise the
          // empty region would split the panel and shove the suggestions up.
          messages.length > 0 || pending !== null ? "flex-1" : "shrink-0",
        )}
      >
        {messages.map((m, i) =>
          m.role === "user" ? (
            <div
              key={i}
              className="flex justify-end animate-in fade-in slide-in-from-bottom-1 duration-300"
            >
              <div className="max-w-[85%] rounded-lg bg-primary px-3 py-2 text-sm text-primary-foreground">
                {m.text}
              </div>
            </div>
          ) : (
            <div
              key={i}
              className="flex justify-start animate-in fade-in slide-in-from-bottom-1 duration-300"
            >
              <div className="max-w-[85%] rounded-lg bg-muted px-3 py-2 text-sm">
                {m.error ? (
                  <p className="text-muted-foreground">{t("genie.unreachable")}</p>
                ) : (
                  <>
                    {/* Pull the header left by the bubble's px-3 inset (#97):
                        the icon hangs into the padding gutter at the bubble
                        edge so the "Ask Genie" label sits flush-left with the
                        response prose/table below instead of indented past the
                        icon. */}
                    <p className="mb-1 -ml-3 flex items-center gap-1 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                      <GenieIcon className="h-3 w-3 shrink-0" />
                      {t("genie.assistantName")}
                    </p>
                    {m.text && <GenieMarkdown text={m.text} />}
                    {/* Gate on there being result data at all (a non-trivial
                        row set or a literal failing record). Whether a TABLE
                        actually renders is decided inside GenieResult — it is
                        suppressed by default and shown only for sample data or
                        when the user asked for rows; the chart is independent. */}
                    {m.resultColumns &&
                      m.resultRows &&
                      m.resultRows.length > 0 &&
                      (m.resultRows.length >= 2 ||
                        m.resultColumns.length >= 4 ||
                        // A literal failing record (single JSON column that the
                        // table expands into the dataset's columns) is always
                        // worth showing, even for one failing row. Kept from
                        // dqlake even though this app's aggregates-only space
                        // never returns row-level data (harmless, faithful).
                        m.resultColumns.includes("failing_record")) && (
                        // Chart (when chartable) and/or table (only when it is
                        // the answer) — see GenieResult.
                        <GenieResult
                          columns={m.resultColumns}
                          rows={m.resultRows}
                          sql={m.sql}
                          question={precedingUserText(messages, i)}
                        />
                      )}
                  </>
                )}

                {m.sql && (
                  <Collapsible className="mt-2">
                    <CollapsibleTrigger className="text-xs font-medium text-muted-foreground hover:underline">
                      {t("genie.sqlToggle")}
                    </CollapsibleTrigger>
                    <CollapsibleContent>
                      <pre className="mt-1 overflow-x-auto rounded bg-background p-2 text-xs">
                        {m.sql}
                      </pre>
                      {m.sqlDescription && (
                        <p className="mt-1 text-xs font-light text-muted-foreground">
                          {m.sqlDescription}
                        </p>
                      )}
                    </CollapsibleContent>
                  </Collapsible>
                )}

                {!m.error && (
                  <div className="mt-2 flex gap-2">
                    <button
                      type="button"
                      aria-label={t("genie.voteHelpful")}
                      onClick={() => vote(m.messageId, "up")}
                      className="text-muted-foreground hover:text-foreground"
                    >
                      <ThumbsUp className="h-4 w-4" />
                    </button>
                    <button
                      type="button"
                      aria-label={t("genie.voteNotHelpful")}
                      onClick={() => vote(m.messageId, "down")}
                      className="text-muted-foreground hover:text-foreground"
                    >
                      <ThumbsDown className="h-4 w-4" />
                    </button>
                  </div>
                )}
              </div>
            </div>
          ),
        )}

        {pending !== null && (
          <div className="flex justify-start animate-in fade-in duration-200">
            <div className="flex items-center gap-2 rounded-lg bg-muted px-3 py-2.5 text-sm text-muted-foreground">
              <TypingDots />
              {pending && pending !== PENDING_DEFAULT && (
                <span className="text-xs">{pending}…</span>
              )}
            </div>
          </div>
        )}
      </div>

      <form
        className="flex gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          const v = inputRef.current?.value ?? "";
          void send(v);
        }}
      >
        <Input
          ref={inputRef}
          placeholder={t("genie.inputPlaceholder")}
          disabled={pending !== null}
        />
        <Button type="submit" disabled={pending !== null}>
          {t("genie.send")}
        </Button>
      </form>
    </div>
  );
}

export function GenieChatSidebar({
  open,
  onOpenChange,
  context,
  contextKind,
  contextSubject,
  contextTables,
  direction,
  autoAsk,
}: {
  open: boolean;
  onOpenChange: (o: boolean) => void;
  context?: string;
  contextKind?: GenieContextKind;
  /** Fully-qualified table or product name injected into sent messages. */
  contextSubject?: string;
  /** Product member-table FQNs for the sent preamble. */
  contextTables?: string[];
  direction?: ScoreDirection;
  autoAsk?: string | null;
}) {
  const { t } = useTranslation();
  const { width, dragging, onPointerDown } = useGeniePanelWidth();
  const { messages } = useConversation(context);
  const hasConversation = messages.length > 0;
  // Non-suspense: the deep link is best-effort chrome and must not suspend the
  // whole sheet while the space metadata loads.
  const { data: spaceInfo } = useGetGenieSpace<GenieSpaceOut>({
    query: { ...selector<GenieSpaceOut>().query, ...RESULTS_QUERY_OPTIONS },
  });

  // The Sheet slides in with a transform; while it's animating, the content's
  // scroll container measures against a moving box, which flashes a scrollbar
  // that then disappears as the panel settles. Keep the body's overflow clipped
  // until the entrance finishes, then hand scrolling back.
  const [entered, setEntered] = useState(false);
  useEffect(() => {
    if (!open) {
      setEntered(false);
      return;
    }
    const t = setTimeout(() => setEntered(true), 550);
    return () => clearTimeout(t);
  }, [open]);

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        // Width is driven by the drag handle, not the breakpoint class.
        // Hide the Sheet's default absolute close button — we render our own in
        // the header row so it lines up with the "Ask Genie" title.
        className="flex flex-col p-0 [&>button.absolute]:hidden"
        // Inline maxWidth beats the base class's `sm:max-w-sm`, which otherwise
        // caps the panel at 384px regardless of the chosen width.
        style={{
          width,
          maxWidth: "none",
          transition: dragging ? "none" : undefined,
        }}
      >
        {/* Drag handle on the inner (left) edge — widen by dragging left. */}
        <div
          role="separator"
          aria-orientation="vertical"
          aria-label={t("genie.resizeAria")}
          onPointerDown={onPointerDown}
          className={cn(
            "group absolute inset-y-0 left-0 z-50 flex w-2 cursor-col-resize items-center justify-center",
            "hover:bg-fuchsia-500/10",
            dragging && "bg-fuchsia-500/20",
          )}
        >
          <GripVertical className="h-4 w-4 text-muted-foreground/40 group-hover:text-muted-foreground" />
        </div>

        <div className="flex h-full flex-col gap-4 p-6">
          <SheetHeader>
            <div className="flex items-center justify-between gap-2">
              <SheetTitle className="flex items-center gap-2">
                <GenieIcon className="h-4 w-4" />
                {t("genie.title")}
              </SheetTitle>
              <div className="flex items-center gap-1">
                {hasConversation && (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-7 gap-1 px-2 text-xs text-muted-foreground"
                    onClick={() => resetConversation(context)}
                  >
                    <Plus className="h-3.5 w-3.5" />
                    {t("genie.newConversation")}
                  </Button>
                )}
                {spaceInfo?.space_url && (
                  <Button
                    asChild
                    variant="ghost"
                    size="icon"
                    className="h-7 w-7 text-muted-foreground"
                    title={t("genie.openSpaceTitle")}
                  >
                    <a
                      href={spaceInfo.space_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      aria-label={t("genie.openSpaceTitle")}
                    >
                      <ExternalLink className="h-4 w-4" />
                    </a>
                  </Button>
                )}
                <SheetClose asChild>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-7 w-7 text-muted-foreground"
                    aria-label={t("genie.close")}
                  >
                    <X className="h-4 w-4" />
                  </Button>
                </SheetClose>
              </div>
            </div>
          </SheetHeader>
          <div className="min-h-0 flex-1 overflow-hidden">
            <QueryErrorResetBoundary>
              {({ reset }) => (
                <ErrorBoundary
                  onReset={reset}
                  fallbackRender={({ resetErrorBoundary }) => (
                    <div className="flex flex-col gap-2 text-sm">
                      <p className="text-muted-foreground">
                        {t("genie.loadFailed")}
                      </p>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={resetErrorBoundary}
                      >
                        {t("genie.tryAgain")}
                      </Button>
                    </div>
                  )}
                >
                  <Suspense fallback={<GeniePanelSkeleton />}>
                    {/* Remount on each open so a fresh auto-ask fires. The
                        conversation itself is preserved in the store. */}
                    {open && (
                      <GenieChatBody
                        context={context}
                        contextKind={contextKind}
                        contextSubject={contextSubject}
                        contextTables={contextTables}
                        direction={direction}
                        autoAsk={autoAsk}
                        scrollEnabled={entered}
                      />
                    )}
                  </Suspense>
                </ErrorBoundary>
              )}
            </QueryErrorResetBoundary>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
}
