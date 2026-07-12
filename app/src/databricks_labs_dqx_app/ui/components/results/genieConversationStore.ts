import { useSyncExternalStore } from "react";

// Ported from dqlake's `components/results/genieConversationStore.ts` and then
// extended (B2-21). Sanctioned deviations from the dqlake original:
// - the localStorage key is renamed dqlake.* -> dqxStudio.*;
// - the store now keeps MULTIPLE conversations per context (a browsable
//   history), not a single thread. dqlake's public surface (appendMessage /
//   setConversationId) is preserved and always targets the ACTIVE thread; the
//   new-/select-/delete-conversation operations back the history UI in
//   GenieChatSidebar. Old single-conversation localStorage payloads are
//   migrated forward on load;
// - `context` is now REQUIRED (a non-empty string). Every Genie surface passes
//   a unique, stable context; there is deliberately no shared "__default__"
//   bucket, so a caller that forgot a context can no longer silently share a
//   thread across pages (the hard per-surface scoping — B2-21 Part A).

/** A single turn in the Genie chat. Lives in a module-level store so the
 *  conversation survives the Sheet unmounting when the panel is closed. */
export type ChatMessage = {
  role: "user" | "genie";
  text?: string;
  sql?: string | null;
  sqlDescription?: string | null;
  messageId?: string | null;
  error?: string | null;
  resultColumns?: string[] | null;
  resultRows?: (string | null)[][] | null;
};

/** One conversation thread within a context. `id` is a local identity (stable
 *  across reloads, distinct from Genie's server-side `conversationId`) used to
 *  select/delete the thread from the history UI. */
export type Conversation = {
  id: string;
  conversationId: string | null;
  messages: ChatMessage[];
  createdAt: number;
  updatedAt: number;
};

/** All threads for one chat-context, plus which one is currently open. */
export type ContextState = {
  conversations: Conversation[];
  activeId: string | null;
};

type StoreState = Record<string, ContextState>;

const STORAGE_KEY = "dqxStudio.genie.conversations";

// Stable module-level sentinels so `useSyncExternalStore` snapshots keep a
// constant reference for contexts / active threads that don't exist yet (a
// fresh object each call would loop the store subscriber).
const EMPTY_CONTEXT: ContextState = Object.freeze({
  conversations: [],
  activeId: null,
});
const EMPTY_CONVERSATION: Conversation = Object.freeze({
  id: "",
  conversationId: null,
  messages: [],
  createdAt: 0,
  updatedAt: 0,
});

function newId(): string {
  const c = typeof crypto !== "undefined" ? crypto : undefined;
  if (c && typeof c.randomUUID === "function") return c.randomUUID();
  return `c-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

/** Coerce one persisted value into the current multi-conversation shape,
 *  migrating the old single-conversation payload (`{ conversationId, messages }`)
 *  forward. Returns null for anything unrecognizable so bad entries are dropped
 *  rather than crashing the whole store. */
function migrateEntry(value: unknown): ContextState | null {
  if (!value || typeof value !== "object") return null;
  const v = value as Record<string, unknown>;

  if (Array.isArray(v.conversations)) {
    const conversations = (v.conversations as unknown[]).filter(
      (c): c is Conversation =>
        !!c && typeof c === "object" && Array.isArray((c as Conversation).messages),
    );
    const activeId = typeof v.activeId === "string" ? v.activeId : null;
    return {
      conversations,
      activeId: conversations.some((c) => c.id === activeId) ? activeId : (conversations[0]?.id ?? null),
    };
  }

  // Legacy v1 single-conversation payload.
  if (Array.isArray(v.messages)) {
    const messages = v.messages as ChatMessage[];
    const conversationId = typeof v.conversationId === "string" ? v.conversationId : null;
    if (messages.length === 0 && conversationId === null) return EMPTY_CONTEXT;
    const now = Date.now();
    const conv: Conversation = {
      id: newId(),
      conversationId,
      messages,
      createdAt: now,
      updatedAt: now,
    };
    return { conversations: [conv], activeId: conv.id };
  }

  return null;
}

function load(): StoreState {
  if (typeof localStorage === "undefined") return {};
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const out: StoreState = {};
    for (const [key, value] of Object.entries(parsed)) {
      const migrated = migrateEntry(value);
      if (migrated) out[key] = migrated;
    }
    return out;
  } catch {
    return {};
  }
}

let state: StoreState = load();
const listeners = new Set<() => void>();

function persist() {
  if (typeof localStorage === "undefined") return;
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    // Quota / private mode — keep the in-memory copy and move on.
  }
}

function emit() {
  for (const l of listeners) l();
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** All threads (and the active id) for one context. Stable reference until that
 *  context mutates. */
export function getContextState(context: string): ContextState {
  return state[context] ?? EMPTY_CONTEXT;
}

function setContextState(context: string, next: ContextState) {
  state = { ...state, [context]: next };
  persist();
  emit();
}

/** The currently-open thread for a context (the empty sentinel when none). */
export function getActiveConversation(context: string): Conversation {
  const cs = getContextState(context);
  return cs.conversations.find((c) => c.id === cs.activeId) ?? EMPTY_CONVERSATION;
}

/** Append a turn to the active thread, creating one if the context has none. */
export function appendMessage(context: string, message: ChatMessage) {
  const cs = getContextState(context);
  const active = cs.conversations.find((c) => c.id === cs.activeId);
  const now = Date.now();
  if (!active) {
    const conv: Conversation = {
      id: newId(),
      conversationId: null,
      messages: [message],
      createdAt: now,
      updatedAt: now,
    };
    setContextState(context, {
      conversations: [conv, ...cs.conversations],
      activeId: conv.id,
    });
    return;
  }
  const updated: Conversation = {
    ...active,
    messages: [...active.messages, message],
    updatedAt: now,
  };
  setContextState(context, {
    conversations: cs.conversations.map((c) => (c.id === updated.id ? updated : c)),
    activeId: cs.activeId,
  });
}

/** Record Genie's server-side conversation id on the active thread. No-op when
 *  it is unchanged (so a redundant set doesn't churn the store). */
export function setConversationId(context: string, id: string | null) {
  const cs = getContextState(context);
  const active = cs.conversations.find((c) => c.id === cs.activeId);
  if (!active || active.conversationId === id) return;
  const updated: Conversation = { ...active, conversationId: id };
  setContextState(context, {
    conversations: cs.conversations.map((c) => (c.id === updated.id ? updated : c)),
    activeId: cs.activeId,
  });
}

/** Open a fresh, empty thread and make it active. If the active thread is
 *  already empty, keep it (don't pile up blank threads). */
export function startNewConversation(context: string) {
  const cs = getContextState(context);
  const active = cs.conversations.find((c) => c.id === cs.activeId);
  if (active && active.messages.length === 0) return;
  const now = Date.now();
  const conv: Conversation = {
    id: newId(),
    conversationId: null,
    messages: [],
    createdAt: now,
    updatedAt: now,
  };
  setContextState(context, {
    conversations: [conv, ...cs.conversations],
    activeId: conv.id,
  });
}

/** Reopen a prior thread by local id. No-op for an unknown id. */
export function selectConversation(context: string, id: string) {
  const cs = getContextState(context);
  if (cs.activeId === id || !cs.conversations.some((c) => c.id === id)) return;
  setContextState(context, { ...cs, activeId: id });
}

/** Remove a thread; if it was active, fall back to the newest remaining one. */
export function deleteConversation(context: string, id: string) {
  const cs = getContextState(context);
  if (!cs.conversations.some((c) => c.id === id)) return;
  const conversations = cs.conversations.filter((c) => c.id !== id);
  const activeId = cs.activeId === id ? (conversations[0]?.id ?? null) : cs.activeId;
  setContextState(context, { conversations, activeId });
}

/** Subscribe a component to a context's full thread list + active id. Stable
 *  reference between mutations, so it is safe as a `useSyncExternalStore`
 *  snapshot. */
export function useContextConversations(context: string): ContextState {
  return useSyncExternalStore(
    subscribe,
    () => getContextState(context),
    () => EMPTY_CONTEXT,
  );
}

/** Subscribe a component to a context's ACTIVE thread. Re-renders on change and
 *  survives unmount because the data lives outside React. */
export function useActiveConversation(context: string): Conversation {
  const cs = useContextConversations(context);
  return cs.conversations.find((c) => c.id === cs.activeId) ?? EMPTY_CONVERSATION;
}
