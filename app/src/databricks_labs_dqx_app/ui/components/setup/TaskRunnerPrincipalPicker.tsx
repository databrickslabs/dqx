import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { ChevronDown, Loader2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Command,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import type { TaskRunnerPrincipal } from "@/lib/api";
import { principalMatches, type PickedPrincipal } from "@/lib/setup-task-runner";
import { cn } from "@/lib/utils";

type TaskRunnerPrincipalPickerProps = {
  id: string;
  value: PickedPrincipal | null;
  principals: readonly TaskRunnerPrincipal[];
  isLoading: boolean;
  disabled?: boolean;
  onChange: (principal: PickedPrincipal) => void;
};

/**
 * Searchable single-select picker of the workspace's service principals, following
 * SetupOptionPicker. Searches by name or Application ID, and offers a typed
 * Application ID as an option so a service principal the list doesn't show can
 * still be chosen.
 */
export function TaskRunnerPrincipalPicker({
  id,
  value,
  principals,
  isLoading,
  disabled = false,
  onChange,
}: TaskRunnerPrincipalPickerProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const { matches, typedId } = useMemo(
    () => principalMatches(principals, search),
    [principals, search],
  );

  const close = () => {
    setOpen(false);
    setSearch("");
  };
  const choose = (principal: PickedPrincipal) => {
    onChange(principal);
    close();
  };

  return (
    <Popover
      open={open}
      onOpenChange={(next) => (next ? setOpen(true) : close())}
    >
      <PopoverTrigger asChild>
        <Button
          id={id}
          type="button"
          variant="outline"
          role="combobox"
          aria-expanded={open}
          disabled={isLoading || disabled}
          className="w-full justify-between font-normal"
        >
          <span className={cn("truncate", !value && "text-muted-foreground")}>
            {value?.displayName || t("setup.taskRunner.pick.placeholder")}
          </span>
          {isLoading ? (
            <Loader2 className="ml-2 h-4 w-4 shrink-0 animate-spin opacity-50" />
          ) : (
            <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
          )}
        </Button>
      </PopoverTrigger>
      <PopoverContent
        className="flex max-h-(--radix-popover-content-available-height) w-[--radix-popover-trigger-width] flex-col overflow-hidden p-0"
        align="start"
      >
        {/* Filtering happens in principalMatches, so cmdk's own fuzzy scoring is off. */}
        <Command shouldFilter={false} className="flex-1 min-h-0 overflow-hidden">
          <CommandInput
            value={search}
            onValueChange={setSearch}
            placeholder={t("setup.taskRunner.pick.search")}
            className="h-9 text-sm dark:bg-input/30"
          />
          <CommandList className="dq-scroll-visible min-h-0 max-h-[300px] flex-1 p-1">
            <CommandGroup>
              {typedId && (
                <CommandItem
                  value={`typed:${typedId}`}
                  onSelect={() =>
                    choose({ applicationId: typedId, displayName: typedId })
                  }
                >
                  <span className="truncate text-sm" title={typedId}>
                    {t("setup.taskRunner.pick.useTyped", { id: typedId })}
                  </span>
                </CommandItem>
              )}
              {matches.map((principal) => (
                <CommandItem
                  key={principal.application_id}
                  value={principal.application_id}
                  onSelect={() =>
                    choose({
                      applicationId: principal.application_id,
                      displayName: principal.display_name,
                      id: principal.id,
                    })
                  }
                  className={cn(
                    "flex-col items-start gap-0",
                    principal.application_id === value?.applicationId &&
                      "bg-primary/10",
                  )}
                >
                  <span className="truncate text-sm" title={principal.display_name}>
                    {principal.display_name}
                  </span>
                  <span className="truncate font-mono text-xs text-muted-foreground">
                    {principal.application_id}
                  </span>
                </CommandItem>
              ))}
              {!typedId && matches.length === 0 && (
                <p className="px-2 py-1.5 text-sm text-muted-foreground">
                  {t("setup.taskRunner.pick.noMatches")}
                </p>
              )}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
