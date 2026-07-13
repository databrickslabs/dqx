/** Ported from dqlake's `components/common/TimezonePicker.tsx`. Adapted:
 *  i18n for the search placeholder / empty state. */
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from "@/components/ui/command";
import { Check, ChevronDown } from "lucide-react";
import { buildTimezoneOptions } from "@/lib/timezones";

export interface TimezonePickerProps {
  value: string;
  onChange: (tz: string) => void;
  disabled?: boolean;
}

/** Searchable combobox over the full IANA timezone list. Defaults to UTC. */
export function TimezonePicker({ value, onChange, disabled }: TimezonePickerProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const options = useMemo(() => buildTimezoneOptions(), []);
  const selectedLabel = useMemo(() => options.find((o) => o.value === value)?.label ?? value, [options, value]);

  return (
    <Popover open={open} onOpenChange={disabled ? undefined : setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          disabled={disabled}
          className="w-72 justify-between"
        >
          <span className="truncate" title={value}>
            {value ? selectedLabel : "UTC"}
          </span>
          <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-72 p-0" align="start">
        <Command>
          <CommandInput placeholder={t("dataProducts.scheduleTimezoneSearchPlaceholder")} />
          <CommandList onWheel={(e) => e.stopPropagation()}>
            <CommandEmpty>{t("dataProducts.scheduleTimezoneNoMatch")}</CommandEmpty>
            <CommandGroup>
              {options.map((tz) => (
                <CommandItem
                  key={tz.value}
                  value={tz.label}
                  onSelect={() => {
                    onChange(tz.value);
                    setOpen(false);
                  }}
                  className="flex items-center gap-2"
                >
                  <span className="truncate flex-1">{tz.label}</span>
                  {value === tz.value && <Check className="h-4 w-4 shrink-0" />}
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
