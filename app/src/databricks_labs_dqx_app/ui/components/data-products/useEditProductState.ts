/**
 * Ported from dqlake's `routes/_sidebar/products/useEditProductState.ts`.
 * Adapted to this app's flatter data model: members are keyed by
 * `binding_id` (dqlake keys by catalog.schema.table because it has no
 * separate binding concept), the schedule buffer is a flat
 * `schedule_cron`/`schedule_tz` pair (no nested cadence object — that
 * shape is reconstructed from/to a 5-field cron string by Task 9's
 * `ProductSchedulingTab`), and steward is a plain string field rather than
 * a principal-search result.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import {
  useUpdateDataProduct,
  useAddDataProductMember,
  useRemoveDataProductMember,
  useSubmitDataProduct,
  getGetDataProductQueryKey,
  getListDataProductsQueryKey,
  getListRunSetsQueryKey,
  type DataProductOut,
  type DataProductMemberOut,
  type UpdateDataProductIn,
} from "@/lib/api";

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

function memberKey(m: DataProductMemberOut): string {
  return m.binding_id;
}

export function useEditProductState(product: DataProductOut) {
  const { t } = useTranslation();
  const qc = useQueryClient();

  // --- Server snapshots (seed + diff baseline) ---
  const serverMemberKeys = useMemo<Set<string>>(() => {
    const s = new Set<string>();
    for (const m of product.members ?? []) s.add(memberKey(m));
    return s;
  }, [product.members]);

  // Server pin per member key, used to detect a pin-only edit on save.
  const serverMemberPins = useMemo<Map<string, number | null>>(() => {
    const m = new Map<string, number | null>();
    for (const x of product.members ?? []) m.set(memberKey(x), x.pinned_version ?? null);
    return m;
  }, [product.members]);

  // --- Local buffered state, seeded once from the server snapshot ---
  const [name, setName] = useState(product.name);
  const [description, setDescription] = useState(product.description ?? "");
  const [steward, setStewardLocal] = useState(product.steward ?? "");
  const [scheduleCron, setScheduleCronLocal] = useState<string | null>(product.schedule_cron ?? null);
  const [scheduleTz, setScheduleTzLocal] = useState<string>(product.schedule_tz ?? "UTC");
  // Set by Task 9's ProductSchedulingTab/SchedulePicker while the raw-cron
  // (custom) editor holds an expression the backend scheduler would reject.
  // Gates the header's Save buttons below so a malformed cron never reaches
  // the scheduler's malformed-cron backoff path.
  const [scheduleCronInvalid, setScheduleCronInvalid] = useState(false);
  const [members, setMembers] = useState<DataProductMemberOut[]>(() => product.members ?? []);

  const setSteward = useCallback((v: string) => setStewardLocal(v), []);
  const setSchedule = useCallback((cron: string | null, tz?: string) => {
    setScheduleCronLocal(cron);
    if (tz) setScheduleTzLocal(tz);
  }, []);

  /** Add a member to the buffer. Newly added members carry no id yet and no
   *  counts — those land after save + refetch. */
  const addMember = useCallback((m: DataProductMemberOut) => {
    setMembers((prev) => {
      const key = memberKey(m);
      if (prev.some((x) => memberKey(x) === key)) return prev;
      return [...prev, m];
    });
  }, []);

  /** Drop a member from the buffer by its binding id. */
  const removeMember = useCallback((m: DataProductMemberOut) => {
    setMembers((prev) => prev.filter((x) => memberKey(x) !== memberKey(m)));
  }, []);

  /** Pin a member to a specific version, or null to follow latest approved.
   *  Writes to the buffer only; the change is flushed on save. */
  const setMemberPin = useCallback((m: DataProductMemberOut, version: number | null) => {
    setMembers((prev) => prev.map((x) => (memberKey(x) === memberKey(m) ? { ...x, pinned_version: version } : x)));
  }, []);

  // Set right after a successful save/submit so the NEXT server refetch is
  // adopted wholesale into the buffer, overriding the pin-preserving bail
  // below. Right after a save the buffer *is* the just-persisted state, so
  // there is no unsaved edit to protect — and the server may have resolved a
  // value the buffer sent optimistically (most importantly a "follow latest"
  // `pinned_version: null` that the backend pins to a concrete version when
  // `default_auto_upgrade` is off). Without adopting the server's resolved
  // value the buffer clings to its optimistic one and `isDirty` never settles
  // back to false, leaving the space permanently "dirty" (and the nav guard's
  // bypass permanently stuck). See B2-66. Consumed (cleared) by the re-seed
  // effect on the first refetch it sees.
  const resyncRef = useRef(false);

  // Re-seed the buffer from the server after a save/publish refetch, so a
  // just-added member's real rule/check counts populate without a manual
  // refresh. Only adopt when the buffered member SET (keys + pins) already
  // matches the server — i.e. there is no unsaved structural edit to clobber.
  useEffect(() => {
    const serverMembers = product.members ?? [];
    setMembers((prev) => {
      // Post-save resync: adopt the server's resolved member state verbatim
      // (real ids, server-resolved pins, refreshed counts). Safe because it
      // only fires on the refetch that immediately follows an explicit save.
      if (resyncRef.current) {
        resyncRef.current = false;
        return serverMembers;
      }
      if (prev.length !== serverMembers.length) return prev;
      const serverByKey = new Map(serverMembers.map((m) => [memberKey(m), m]));
      for (const m of prev) {
        const s = serverByKey.get(memberKey(m));
        if (!s) return prev;
        if ((m.pinned_version ?? null) !== (s.pinned_version ?? null)) return prev;
      }
      return prev.map((m) => {
        const s = serverByKey.get(memberKey(m))!;
        return { ...s, pinned_version: m.pinned_version ?? null };
      });
    });
  }, [product.members]);

  // --- Dirty detection vs server ---
  const stewardDirty = steward !== (product.steward ?? "");

  const scheduleDirty = useMemo(() => {
    const serverCron = product.schedule_cron ?? null;
    const serverTz = product.schedule_tz ?? "UTC";
    if (scheduleCron !== serverCron) return true;
    if (scheduleCron !== null && scheduleTz !== serverTz) return true;
    return false;
  }, [scheduleCron, scheduleTz, product.schedule_cron, product.schedule_tz]);

  const membersDirty = useMemo(() => {
    const cur = new Set(members.map(memberKey));
    if (cur.size !== serverMemberKeys.size) return true;
    for (const k of cur) if (!serverMemberKeys.has(k)) return true;
    for (const m of members) {
      const k = memberKey(m);
      if (serverMemberKeys.has(k) && (m.pinned_version ?? null) !== (serverMemberPins.get(k) ?? null)) {
        return true;
      }
    }
    return false;
  }, [members, serverMemberKeys, serverMemberPins]);

  const isDirty = useMemo(() => {
    if (name !== product.name) return true;
    if (description !== (product.description ?? "")) return true;
    if (stewardDirty) return true;
    if (scheduleDirty) return true;
    if (membersDirty) return true;
    return false;
  }, [name, description, product.name, product.description, stewardDirty, scheduleDirty, membersDirty]);

  // Save is blocked while the Schedule tab's raw-cron editor holds an
  // expression the backend scheduler can't parse — otherwise a PATCH would
  // persist a cron that lands the product in the scheduler's malformed-cron
  // backoff instead of ticking.
  const canSave = isDirty && !scheduleCronInvalid;

  // Bypasses the unsaved-changes nav guard right after a successful save.
  // The local buffer (name/description/steward/schedule/members) is only
  // reconciled against the server via an async invalidate+refetch, so between
  // a save resolving and that refetch settling, `isDirty` is transiently TRUE
  // (buffer != still-stale query data) even though the user JUST saved and
  // has nothing to lose. Without this, navigating in that window fires a
  // spurious "unsaved changes" prompt (B10). Set on save success; cleared once
  // the buffer settles clean again (below), so a genuine later edit still
  // engages the guard.
  const bypassGuardRef = useRef(false);
  useEffect(() => {
    if (!isDirty) bypassGuardRef.current = false;
  }, [isDirty]);

  // --- Mutations ---
  // Suppress the global mutation onError toast — errors are surfaced locally.
  const updateMut = useUpdateDataProduct({ mutation: { onError: () => {} } });
  const addMut = useAddDataProductMember({ mutation: { onError: () => {} } });
  const removeMut = useRemoveDataProductMember({ mutation: { onError: () => {} } });
  const submitMut = useSubmitDataProduct({ mutation: { onError: () => {} } });

  const invalidate = useCallback(() => {
    qc.invalidateQueries({ queryKey: getGetDataProductQueryKey(product.product_id) });
    qc.invalidateQueries({ queryKey: getListDataProductsQueryKey() });
    qc.invalidateQueries({ queryKey: getListRunSetsQueryKey() });
  }, [qc, product.product_id]);

  /** Persist all buffered edits: PATCH name/description/steward/schedule, then
   *  reconcile members (add new, remove dropped, re-add pin-only changes).
   *  Throws on failure. */
  const persist = useCallback(async () => {
    if (scheduleCronInvalid) {
      throw new Error(t("dataProducts.scheduleCronInvalid"));
    }

    const patch: UpdateDataProductIn = {};
    let patchNeeded = false;

    if (name !== product.name) {
      patch.name = name;
      patchNeeded = true;
    }
    if (description !== (product.description ?? "")) {
      patch.description = description;
      patchNeeded = true;
    }
    if (stewardDirty) {
      patch.steward = steward.trim() || null;
      patchNeeded = true;
    }
    if (scheduleDirty) {
      patch.schedule_cron = scheduleCron;
      patch.schedule_tz = scheduleCron !== null ? scheduleTz : (product.schedule_tz ?? "UTC");
      patchNeeded = true;
    }

    if (patchNeeded) {
      await updateMut.mutateAsync({ productId: product.product_id, data: patch });
    }

    // Reconcile members. Adds: in buffer, not on server. Pin changes on
    // existing members: re-POST (the endpoint upserts pinned_version by
    // binding_id). Removals: on server, not in buffer.
    const bufferKeys = new Set(members.map(memberKey));
    for (const m of members) {
      const k = memberKey(m);
      const isNew = !serverMemberKeys.has(k);
      const pinChanged = !isNew && (m.pinned_version ?? null) !== (serverMemberPins.get(k) ?? null);
      if (isNew || pinChanged) {
        await addMut.mutateAsync({
          productId: product.product_id,
          data: { binding_id: m.binding_id, pinned_version: m.pinned_version ?? null },
        });
      }
    }
    for (const m of product.members ?? []) {
      if (!bufferKeys.has(memberKey(m))) {
        await removeMut.mutateAsync({ productId: product.product_id, memberId: m.id });
      }
    }
  }, [
    scheduleCronInvalid,
    t,
    name,
    description,
    steward,
    stewardDirty,
    scheduleCron,
    scheduleTz,
    scheduleDirty,
    members,
    serverMemberKeys,
    serverMemberPins,
    product,
    updateMut,
    addMut,
    removeMut,
  ]);

  const handleSaveDraft = useCallback(async (): Promise<boolean> => {
    try {
      await persist();
      bypassGuardRef.current = true;
      resyncRef.current = true;
      invalidate();
      toast.success(t("dataProducts.toastSavedDraft"));
      return true;
    } catch (e) {
      toast.error(extractApiError(e, t("dataProducts.toastSaveFailed")), { duration: 6000 });
      return false;
    }
  }, [persist, invalidate, t]);

  /** Persist any staged edits, then submit the space for review
   *  (draft/rejected -> pending_approval). Mirrors the monitored-table
   *  "Submit for review" flow. */
  const handleSubmit = useCallback(async (): Promise<boolean> => {
    try {
      await persist();
      await submitMut.mutateAsync({ productId: product.product_id });
      bypassGuardRef.current = true;
      resyncRef.current = true;
      invalidate();
      toast.success(t("dataProducts.toastSubmitted"));
      return true;
    } catch (e) {
      toast.error(extractApiError(e, t("dataProducts.toastSubmitFailed")), { duration: 6000 });
      return false;
    }
  }, [persist, submitMut, product.product_id, invalidate, t]);

  // Called by the approve handlers (the space-level approve banner AND the
  // member-level review popover) after a successful approval — but ONLY when
  // the buffer was clean beforehand (the caller checks `isDirty`). Approval
  // refetches the product; between the mutation resolving and that refetch
  // settling, the buffer can momentarily disagree with the still-stale query
  // data and trip a spurious "unsaved changes" guard (the same window B10 fixed
  // for the save path). Engaging the bypass suppresses that, and the resync
  // adopts the refreshed server truth so `isDirty` settles cleanly afterwards.
  // Both are safe precisely because the caller only invokes this when there was
  // nothing unsaved to lose — a genuine unsaved edit takes the `isDirty` branch
  // and never calls this, so the guard still fires for it. (B2-66)
  const markApprovedWhenClean = useCallback(() => {
    bypassGuardRef.current = true;
    resyncRef.current = true;
  }, []);

  // A save touches several endpoints; treat any in flight as "save pending".
  const savePending = updateMut.isPending || addMut.isPending || removeMut.isPending;

  return {
    name,
    description,
    setName,
    setDescription,

    steward,
    setSteward,

    scheduleCron,
    scheduleTz,
    setSchedule,
    scheduleCronInvalid,
    setScheduleCronInvalid,

    members,
    addMember,
    removeMember,
    setMemberPin,

    isDirty,
    canSave,
    bypassGuardRef,
    markApprovedWhenClean,
    handleSaveDraft,
    handleSubmit,
    savePending,
    submitPending: savePending || submitMut.isPending,
  };
}

export type EditProductState = ReturnType<typeof useEditProductState>;
