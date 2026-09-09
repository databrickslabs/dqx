import * as React from "react";
import { ChevronDown } from "lucide-react";
import {
  Collapsible,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { CollapseRegion } from "./CollapseRegion";

/**
 * A titled, collapsible panel. The chevron rotates as it opens. No data
 * fetching — callers pass the content in.
 */
export function CollapsibleSection({
  title,
  defaultOpen,
  children,
  headerRight,
}: {
  title: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
  /** Rendered in the header row, right-aligned next to the title — only while
   *  the section is open (hidden when collapsed). Clicks inside it don't toggle
   *  the section. */
  headerRight?: React.ReactNode;
}) {
  const [open, setOpen] = React.useState(defaultOpen ?? false);
  return (
    <Collapsible
      open={open}
      onOpenChange={setOpen}
      className="rounded-md border"
    >
      <div className="flex items-center gap-2 rounded-t-md bg-muted/40 pr-3">
        <CollapsibleTrigger className="group flex flex-1 items-center gap-2 px-3 py-2 text-sm font-semibold">
          <ChevronDown className="h-4 w-4 shrink-0 transition-transform group-data-[state=closed]:-rotate-90" />
          {title}
        </CollapsibleTrigger>
        {open && headerRight ? (
          // Stop clicks here from bubbling to the trigger (which would collapse).
          <div onClick={(e) => e.stopPropagation()}>{headerRight}</div>
        ) : null}
      </div>
      <CollapseRegion open={open}>
        <div className="border-t p-3">{children}</div>
      </CollapseRegion>
    </Collapsible>
  );
}
