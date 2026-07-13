import { DndContext, type DragEndEvent, type SensorDescriptor, type SensorOptions } from "@dnd-kit/core";
import { SortableContext, useSortable, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { useTranslation } from "react-i18next";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Button } from "@/components/ui/button";
import { Columns3, GripVertical } from "lucide-react";

interface SortableColumnItemProps<K extends string> {
  id: K;
  label: string;
  checked: boolean;
  onCheckedChange: () => void;
  disabled?: boolean;
}

function SortableColumnItem<K extends string>({ id, label, checked, onCheckedChange, disabled }: SortableColumnItemProps<K>) {
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
      <DropdownMenuCheckboxItem
        className="flex-1"
        checked={checked}
        onCheckedChange={disabled ? undefined : onCheckedChange}
        disabled={disabled}
        onSelect={(e) => e.preventDefault()}
      >
        {label}
      </DropdownMenuCheckboxItem>
    </div>
  );
}

export interface EditColumnsDropdownProps<K extends string> {
  /** Column order, drag-reorderable — same array driving the table itself. */
  order: K[];
  /** Human-readable label + toggle-lock per column key. */
  labelOf: (id: K) => string;
  toggleableOf: (id: K) => boolean;
  isChecked: (id: K) => boolean;
  onToggle: (id: K) => void;
  onDragEnd: (event: DragEndEvent) => void;
  sensors: SensorDescriptor<SensorOptions>[];
}

/**
 * The "Edit Columns" trigger + dropdown: toggle column visibility and
 * drag-reorder columns. Shared by the Rules Registry list (`RulesTable`)
 * and the Monitored Tables list (`MonitoredTablesTable`) — ported from
 * dqlake's `BindingsTable` dropdown so both lists behave identically.
 */
export function EditColumnsDropdown<K extends string>({
  order,
  labelOf,
  toggleableOf,
  isChecked,
  onToggle,
  onDragEnd,
  sensors,
}: EditColumnsDropdownProps<K>) {
  const { t } = useTranslation();
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" className="h-8 text-xs ml-auto gap-1.5">
          <Columns3 className="h-3.5 w-3.5" />
          {t("common.editColumns")}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" onCloseAutoFocus={(e) => e.preventDefault()}>
        <DndContext sensors={sensors} onDragEnd={onDragEnd}>
          <SortableContext items={order} strategy={verticalListSortingStrategy}>
            {order.map((key) => (
              <SortableColumnItem
                key={key}
                id={key}
                label={labelOf(key)}
                checked={toggleableOf(key) ? isChecked(key) : true}
                onCheckedChange={() => onToggle(key)}
                disabled={!toggleableOf(key)}
              />
            ))}
          </SortableContext>
        </DndContext>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
