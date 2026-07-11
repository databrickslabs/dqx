import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { useTranslation } from "react-i18next";
import { AI_BUTTON_BG } from "@/lib/ai-style";
import { preverifyRowEntitlements } from "@/lib/entitlement-preverify";
import { GenieChatSidebar } from "./GenieChatSidebar";
import { GenieIcon } from "./GenieIcon";
import type {
  GenieContextKind,
  ScoreDirection,
} from "./genieSuggestedQuestions";

// Ported from dqlake's `components/results/AskGenieButton.tsx` (the spec —
// keep the provider/controller/standalone split aligned with it). Sanctioned
// deviations: t() strings and the `contextTables` prop (product member FQNs
// for the sent preamble — see genieSuggestedQuestions.ts).

type GenieChatController = {
  /** Open the chat with no prompt (the plain "Ask Genie" launcher). */
  open: () => void;
  /** Open the chat and auto-send a question (e.g. "Why did this drop?"). */
  ask: (question: string) => void;
};

const GenieChatContext = createContext<GenieChatController | null>(null);

/**
 * Wraps a results tab so any descendant (the floating "Ask Genie" button, a
 * "Why did this drop?" button on the score box, etc.) can open the in-app
 * Genie chat — optionally pre-filling and auto-sending a question.
 *
 * `contextKind` scopes the suggested questions to a table or product;
 * `contextSubject` is the fully-qualified subject injected into sent messages;
 * `contextTables` carries a product's member-table FQNs into that preamble;
 * `context` is the opaque per-conversation key.
 */
export function GenieChatProvider({
  context,
  contextKind,
  contextSubject,
  contextTables,
  direction,
  children,
}: {
  context?: string;
  contextKind?: GenieContextKind;
  /** Fully-qualified table or product name injected into sent messages. */
  contextSubject?: string;
  /** Product member-table FQNs injected alongside the subject. */
  contextTables?: string[];
  direction?: ScoreDirection;
  children: ReactNode;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [autoAsk, setAutoAsk] = useState<string | null>(null);

  // Pre-verify the caller's row-level (failing-rows) access for the tables
  // in scope (P4.3), so the entitlement-gated view is already open by the
  // time they ask Genie a failing-rows question: the context table on a
  // table surface, the member FQNs on a product surface. Provider mount is
  // the cheaper correct trigger (no open-state tracking) — the results tab
  // rendering IS the "user is looking at these tables" signal, and the
  // per-session dedupe inside the helper absorbs remounts. The effect keys
  // on a newline-joined serialization (FQNs can't contain newlines) because
  // contextTables is a fresh array per render.
  const preverifyKey =
    contextKind === "table"
      ? (contextSubject ?? "")
      : contextKind === "product"
        ? (contextTables ?? []).join("\n")
        : "";
  useEffect(() => {
    if (!preverifyKey) return;
    preverifyRowEntitlements(preverifyKey.split("\n"));
  }, [preverifyKey]);

  const controller = useMemo<GenieChatController>(
    () => ({
      open: () => {
        setAutoAsk(null);
        setOpen(true);
      },
      ask: (question: string) => {
        setAutoAsk(question);
        setOpen(true);
      },
    }),
    [],
  );

  const onOpenChange = useCallback((o: boolean) => {
    setOpen(o);
    if (!o) setAutoAsk(null);
  }, []);

  return (
    <GenieChatContext.Provider value={controller}>
      {children}
      <button
        type="button"
        onClick={controller.open}
        className={`fixed bottom-6 right-6 z-40 flex items-center gap-2 rounded-full px-4 py-2.5 text-sm font-semibold shadow-lg ${AI_BUTTON_BG}`}
      >
        <GenieIcon className="h-4 w-4 [&_path]:fill-white" />
        {t("genie.askGenie")}
      </button>
      <GenieChatSidebar
        open={open}
        onOpenChange={onOpenChange}
        context={context}
        contextKind={contextKind}
        contextSubject={contextSubject}
        contextTables={contextTables}
        direction={direction}
        autoAsk={autoAsk}
      />
    </GenieChatContext.Provider>
  );
}

/** Access the Genie chat controller from inside a {@link GenieChatProvider}. */
export function useGenieChat(): GenieChatController | null {
  return useContext(GenieChatContext);
}

/**
 * Standalone floating launcher + sidebar. Kept for callers that don't need the
 * provider/context (it manages its own open state).
 */
export function AskGenieButton({
  context,
  contextKind,
  contextSubject,
  contextTables,
  direction,
}: {
  context?: string;
  contextKind?: GenieContextKind;
  contextSubject?: string;
  contextTables?: string[];
  direction?: ScoreDirection;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className={`fixed bottom-6 right-6 z-40 flex items-center gap-2 rounded-full px-4 py-2.5 text-sm font-semibold shadow-lg ${AI_BUTTON_BG}`}
      >
        <GenieIcon className="h-4 w-4 [&_path]:fill-white" />
        {t("genie.askGenie")}
      </button>
      <GenieChatSidebar
        open={open}
        onOpenChange={setOpen}
        context={context}
        contextKind={contextKind}
        contextSubject={contextSubject}
        contextTables={contextTables}
        direction={direction}
      />
    </>
  );
}
