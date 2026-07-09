import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { OPERATORS_BY_FAMILY, type Family } from "@/lib/lowcodeOperators";
import { useTranslation } from "react-i18next";

type Props = {
  value: string;
  family: Family;
  onChange: (next: string) => void;
};

const ANY_OPS = OPERATORS_BY_FAMILY.ANY;

// Ported from dqlake's OperatorDropdown: surface the column family's own
// operators first, then the universal (ANY) operators under a divider,
// de-duped so "=" / "in" never appear twice.
export function OperatorDropdown({ value, family, onChange }: Props) {
  const { t } = useTranslation();
  const familyOps = OPERATORS_BY_FAMILY[family] ?? OPERATORS_BY_FAMILY.ANY;
  const familyLabels: Record<Family, string> = {
    NUMERIC: t("rulesRegistry.slotFamilyNumeric"),
    TEXTUAL: t("rulesRegistry.slotFamilyText"),
    TEMPORAL: t("rulesRegistry.slotFamilyTemporal"),
    BOOLEAN: t("rulesRegistry.slotFamilyBoolean"),
    ANY: t("rulesRegistry.slotFamilyAny"),
  };

  const showUniversal = family !== "ANY";
  const familySet = new Set(familyOps);
  const universalOps = showUniversal ? ANY_OPS.filter((o) => !familySet.has(o)) : [];

  return (
    <Select value={value || ""} onValueChange={onChange}>
      <SelectTrigger className="h-8 w-full font-mono text-xs">
        <SelectValue placeholder={t("rulesRegistry.lowcodeOperatorPlaceholder")} />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {showUniversal && (
            <SelectLabel className="text-[10px] uppercase tracking-wider text-muted-foreground">
              {familyLabels[family]}
            </SelectLabel>
          )}
          {familyOps.map((op) => (
            <SelectItem key={op} value={op}>
              {op}
            </SelectItem>
          ))}
        </SelectGroup>
        {showUniversal && universalOps.length > 0 && (
          <>
            <SelectSeparator />
            <SelectGroup>
              <SelectLabel className="text-[10px] uppercase tracking-wider text-muted-foreground">
                {t("rulesRegistry.lowcodeUniversalOperators")}
              </SelectLabel>
              {universalOps.map((op) => (
                <SelectItem key={op} value={op}>
                  {op}
                </SelectItem>
              ))}
            </SelectGroup>
          </>
        )}
      </SelectContent>
    </Select>
  );
}
