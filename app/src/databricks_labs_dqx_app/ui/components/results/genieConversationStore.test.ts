import { describe, expect, it } from "bun:test";
import {
  appendMessage,
  deleteConversation,
  getActiveConversation,
  getContextState,
  selectConversation,
  setConversationId,
  startNewConversation,
} from "./genieConversationStore";

// The store is module-level state; each test uses its own context key so the
// tests stay isolated without a reset hook (matching real usage, where every
// table/product/rule/global surface has its own key).

describe("genieConversationStore", () => {
  it("starts empty for an unknown context", () => {
    const cs = getContextState("t:none");
    expect(cs.conversations).toEqual([]);
    expect(cs.activeId).toBeNull();
    const active = getActiveConversation("t:none");
    expect(active.conversationId).toBeNull();
    expect(active.messages).toEqual([]);
  });

  it("appending creates the active thread and isolates contexts", () => {
    appendMessage("t:a", { role: "user", text: "hi" });
    appendMessage("t:a", { role: "genie", text: "hello" });
    appendMessage("t:b", { role: "user", text: "other" });

    expect(getActiveConversation("t:a").messages.map((m) => m.text)).toEqual([
      "hi",
      "hello",
    ]);
    expect(getActiveConversation("t:b").messages).toHaveLength(1);
    // One thread per context so far.
    expect(getContextState("t:a").conversations).toHaveLength(1);
    expect(getContextState("t:b").conversations).toHaveLength(1);
  });

  it("stores and de-duplicates the conversation id on the active thread", () => {
    appendMessage("t:cid", { role: "user", text: "q" });
    setConversationId("t:cid", "c-1");
    expect(getActiveConversation("t:cid").conversationId).toBe("c-1");
    const before = getContextState("t:cid");
    setConversationId("t:cid", "c-1"); // no-op — same id
    expect(getContextState("t:cid")).toBe(before);
    setConversationId("t:cid", "c-2");
    expect(getActiveConversation("t:cid").conversationId).toBe("c-2");
  });

  it("keeps the full answer payload on a genie message", () => {
    appendMessage("t:payload", {
      role: "genie",
      text: "answer",
      sql: "SELECT 1",
      sqlDescription: "desc",
      messageId: "m-1",
      error: null,
      resultColumns: ["a"],
      resultRows: [["1"], [null]],
    });
    const m = getActiveConversation("t:payload").messages[0];
    expect(m.sql).toBe("SELECT 1");
    expect(m.messageId).toBe("m-1");
    expect(m.resultRows).toEqual([["1"], [null]]);
  });

  it("keeps multiple threads per context and reopens a prior one", () => {
    appendMessage("t:multi", { role: "user", text: "first thread" });
    const firstId = getContextState("t:multi").activeId;
    expect(firstId).not.toBeNull();

    startNewConversation("t:multi");
    const secondId = getContextState("t:multi").activeId;
    expect(secondId).not.toBe(firstId);
    // The fresh thread starts empty and is newest-first.
    expect(getActiveConversation("t:multi").messages).toEqual([]);
    expect(getContextState("t:multi").conversations).toHaveLength(2);
    expect(getContextState("t:multi").conversations[0].id).toBe(secondId);

    appendMessage("t:multi", { role: "user", text: "second thread" });

    // Reopen the first thread — its messages come back untouched.
    selectConversation("t:multi", firstId!);
    expect(getActiveConversation("t:multi").messages.map((m) => m.text)).toEqual([
      "first thread",
    ]);
  });

  it("does not pile up blank threads when starting new twice", () => {
    appendMessage("t:blank", { role: "user", text: "x" });
    startNewConversation("t:blank");
    startNewConversation("t:blank"); // active is already empty — no-op
    expect(getContextState("t:blank").conversations).toHaveLength(2);
  });

  it("deleting the active thread falls back to the newest remaining one", () => {
    appendMessage("t:del", { role: "user", text: "one" });
    const oldId = getContextState("t:del").activeId!;
    startNewConversation("t:del");
    appendMessage("t:del", { role: "user", text: "two" });
    const newId = getContextState("t:del").activeId!;

    deleteConversation("t:del", newId);
    expect(getContextState("t:del").conversations).toHaveLength(1);
    // Falls back to the remaining (older) thread.
    expect(getContextState("t:del").activeId).toBe(oldId);
    expect(getActiveConversation("t:del").messages.map((m) => m.text)).toEqual([
      "one",
    ]);
  });

  it("deleting a non-active thread leaves the active selection intact", () => {
    appendMessage("t:del2", { role: "user", text: "keep" });
    const keepId = getContextState("t:del2").activeId!;
    startNewConversation("t:del2");
    appendMessage("t:del2", { role: "user", text: "active" });
    const activeId = getContextState("t:del2").activeId!;

    deleteConversation("t:del2", keepId);
    expect(getContextState("t:del2").activeId).toBe(activeId);
    expect(getContextState("t:del2").conversations).toHaveLength(1);
  });
});
