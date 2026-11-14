import { useState } from "react";
import { DndContext, type DragEndEvent, type SensorDescriptor, type SensorOptions } from "@dnd-kit/core";
import { SortableContext, useSortable, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { useTranslation } from "react-i18next";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Button } from "@/components/ui/button";
import { Columns3, GripVertical } from "lucide-react";
import { cn } from "@/lib/utils";

interface SortableItemProps<K extends string> {
  id: K;
  label: string;
  checked: boolean;
  onCheckedChange: () => void;
  disabled?: boolean;
}

function ToggleItem({ label, checked, onCheckedChange, disabled }: Omit<SortableItemProps<string>, "id">) {
  return (
    <DropdownMenuCheckboxItem
      className="flex-1"
      checked={checked}
      onCheckedChange={disabled ? undefined : onCheckedChange}
      disabled={disabled}
      onSelect={(e) => e.preventDefault()}
    >
      {label}
    </DropdownMenuCheckboxItem>
  );
}

/** A pinned entry: toggleable, but with no drag handle and outside the sortable list. */
function PinnedItem({ label, checked, onCheckedChange, disabled }: Omit<SortableItemProps<string>, "id">) {
  return (
    <div className="flex items-center">
      {/* Same footprint as the drag handle so labels line up with the sortable rows. */}
      <span className="px-1" aria-hidden>
        <span className="block h-4 w-4" />
      </span>
      <ToggleItem label={label} checked={checked} onCheckedChange={onCheckedChange} disabled={disabled} />
    </div>
  );
}

function SortableItem<K extends string>({ id, label, checked, onCheckedChange, disabled }: SortableItemProps<K>) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  };

  return (
    <div ref={setNodeRef} style={style} className="flex items-center">
      <span
        {...attributes}
        {...listeners}
        className="px-1 cursor-grab active:cursor-grabbing text-muted-foreground"
      >
        <GripVertical className="h-4 w-4" />
      </span>
      <ToggleItem label={label} checked={checked} onCheckedChange={onCheckedChange} disabled={disabled} />
    </div>
  );
}

/**
 * A drag-reorderable, toggleable list of keys — one view of the Edit View
 * menu. The column view is fed by `useColumnLayout`; the filter view by
 * `useFilterLayout` (adapt it with `filterLayoutMenuConfig`).
 */
export interface SortableToggleListConfig<K extends string> {
  /** Current order, drag-reorderable — the same array driving the table / filter bar. */
  order: K[];
  labelOf: (id: K) => string;
  /** False locks an entry on (shown checked and disabled). */
  toggleableOf: (id: K) => boolean;
  /** True pins an entry to the top: listed first with no drag handle, never
   *  reorderable, and nothing can be dropped above it. Still toggleable. */
  pinnedOf?: (id: K) => boolean;
  isChecked: (id: K) => boolean;
  onToggle: (id: K) => void;
  onDragEnd: (event: DragEndEvent) => void;
  sensors: SensorDescriptor<SensorOptions>[];
}

function SortableToggleList<K extends string>({
  order,
  labelOf,
  toggleableOf,
  pinnedOf,
  isChecked,
  onToggle,
  onDragEnd,
  sensors,
}: SortableToggleListConfig<K>) {
  const pinned = pinnedOf ? order.filter(pinnedOf) : [];
  const sortable = pinnedOf ? order.filter((key) => !pinnedOf(key)) : order;
  const itemProps = (key: K) => ({
    label: labelOf(key),
    checked: toggleableOf(key) ? isChecked(key) : true,
    onCheckedChange: () => onToggle(key),
    disabled: !toggleableOf(key),
  });
  return (
    <>
      {pinned.map((key) => (
        <PinnedItem key={key} {...itemProps(key)} />
      ))}
      {pinned.length > 0 && sortable.length > 0 && <DropdownMenuSeparator />}
      {/* Only unpinned keys are sortable, so a drag can never land above a pinned entry. */}
      <DndContext sensors={sensors} onDragEnd={onDragEnd}>
        <SortableContext items={sortable} strategy={verticalListSortingStrategy}>
          {sortable.map((key) => (
            <SortableItem key={key} id={key} {...itemProps(key)} />
          ))}
        </SortableContext>
      </DndContext>
    </>
  );
}

type EditMenuView = "columns" | "filters";

/**
 * Props: the column list config (flat, as before), plus an optional
 * `filters` config. When `filters` is set the menu opens with a
 * Columns | Filters switch; the Filters view toggles and reorders the page's
 * filter pills (search, facets, group by). Pages own the filter layout with
 * `useFilterLayout` (persisted per page, `onHide` resets a hidden filter's
 * value) and render `layout.visibleKeys` in order:
 *
 * ```tsx
 * const filterLayout = useFilterLayout({ storageKey, defaultOrder, filters: FILTERS, onHide: resetFilter });
 * <EditColumnsDropdown {...columnConfig} filters={filterLayoutMenuConfig(filterLayout, (k) => t(FILTERS[k].labelKey))} />
 * ```
 */
export interface EditColumnsDropdownProps<K extends string, F extends string = string>
  extends SortableToggleListConfig<K> {
  filters?: SortableToggleListConfig<F>;
}

/**
 * The "Edit View" trigger + dropdown shared by every overview table: toggle
 * and drag-reorder columns and, when `filters` is given, the filter pills.
 * Ported from dqlake's `BindingsTable` dropdown so the lists behave identically.
 */
export function EditColumnsDropdown<K extends string, F extends string = string>({
  filters,
  ...columns
}: EditColumnsDropdownProps<K, F>) {
  const { t } = useTranslation();
  const [view, setView] = useState<EditMenuView>("columns");
  return (
    <DropdownMenu onOpenChange={(open) => !open && setView("columns")}>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-8 shrink-0 text-xs ml-auto gap-1.5">
          <Columns3 className="h-3.5 w-3.5" />
          {t("common.editColumns")}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-52" onCloseAutoFocus={(e) => e.preventDefault()}>
        {filters && (
          <div
            role="radiogroup"
            aria-label={t("common.editColumnsView")}
            className="relative mb-1 grid grid-cols-2 rounded-md bg-muted p-0.5"
          >
            {/* Sliding thumb: one segment wide, moved a full width to the right for Filters. */}
            <span
              aria-hidden
              className={cn(
                "absolute inset-y-0.5 left-0.5 w-[calc(50%-2px)] rounded-[5px] bg-background shadow-sm",
                "transition-transform duration-200 ease-out motion-reduce:transition-none",
                view === "filters" && "translate-x-full",
              )}
            />
            {(["columns", "filters"] as const).map((option) => (
              <button
                key={option}
                type="button"
                role="radio"
                aria-checked={view === option}
                onClick={() => setView(option)}
                className={cn(
                  "relative h-7 rounded-[5px] px-2 text-xs font-medium transition-colors duration-200 outline-none focus-visible:ring-2 focus-visible:ring-ring/50 motion-reduce:transition-none",
                  view === option ? "text-foreground" : "text-muted-foreground hover:text-foreground",
                )}
              >
                {option === "columns" ? t("common.editColumnsColumns") : t("common.editColumnsFilters")}
              </button>
            ))}
          </div>
        )}
        {filters && view === "filters" ? <SortableToggleList {...filters} /> : <SortableToggleList {...columns} />}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
