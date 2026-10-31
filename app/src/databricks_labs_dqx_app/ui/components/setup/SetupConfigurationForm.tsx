import { useState } from "react";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { SetupOptionPicker } from "@/components/setup/SetupOptionPicker";
import type { SetupOptionList } from "@/lib/setup-options";

export type SetupConfigurationValues = {
  catalog: string;
  prefix: string;
  audience_group: string;
};

type SetupConfigurationFormProps = {
  isSubmitting: boolean;
  errorCode?: string;
  onSubmit: (values: SetupConfigurationValues) => void;
  /** Previously saved choices to edit; empty fields fall back to the defaults. */
  initialValues?: Partial<SetupConfigurationValues>;
  /** Catalogs for the searchable picker; without a list the catalog is typed in. */
  catalogList?: SetupOptionList;
  /** Groups for the searchable picker; without a list the group is typed in. */
  groupList?: SetupOptionList;
  /** Closes the form without saving, when changing previously saved choices. */
  onCancel?: () => void;
};

const FIELDS = ["catalog", "prefix", "audience_group"] as const;

/** Copy key stem of the pickable fields: `<stem>Placeholder`, `<stem>Search`, `<stem>ListUnavailable`. */
const PICKER_COPY = { catalog: "catalog", audience_group: "group" } as const;

function optionsOf(list: SetupOptionList | null): string[] {
  return list?.status === "ready" ? list.options : [];
}

export function SetupConfigurationForm({
  isSubmitting,
  errorCode,
  onSubmit,
  initialValues,
  catalogList,
  groupList,
  onCancel,
}: SetupConfigurationFormProps) {
  const { t } = useTranslation();
  const [values, setValues] = useState<SetupConfigurationValues>(() => ({
    catalog: initialValues?.catalog || "",
    prefix: initialValues?.prefix || "dqx_studio",
    audience_group: initialValues?.audience_group || "",
  }));
  const update =
    (key: keyof SetupConfigurationValues) =>
    (event: React.ChangeEvent<HTMLInputElement>) =>
      setValues((current) => ({ ...current, [key]: event.target.value }));
  const lists = { catalog: catalogList, audience_group: groupList };
  // The picker for a field whose list loads or is ready; null means a plain text input.
  const pickerFor = (field: (typeof FIELDS)[number]) => {
    const list = field === "prefix" ? undefined : lists[field];
    return list && list.status !== "unavailable" ? list : null;
  };
  const choiceMissing = FIELDS.some(
    (field) => !!pickerFor(field) && !values[field].trim(),
  );

  return (
    <form
      className="space-y-3"
      onSubmit={(event) => {
        event.preventDefault();
        onSubmit({
          catalog: values.catalog.trim(),
          prefix: values.prefix.trim(),
          audience_group: values.audience_group.trim(),
        });
      }}
    >
      {FIELDS.map((field) => (
        <div key={field} className="space-y-1">
          <Label htmlFor={`setup-${field}`}>
            {t(`setup.configuration.${field}`)}
          </Label>
          {field !== "prefix" && pickerFor(field) ? (
            <SetupOptionPicker
              id={`setup-${field}`}
              value={values[field]}
              options={optionsOf(pickerFor(field))}
              isLoading={pickerFor(field)?.status === "loading"}
              placeholder={t(`setup.configuration.${PICKER_COPY[field]}Placeholder`)}
              searchPlaceholder={t(`setup.configuration.${PICKER_COPY[field]}Search`)}
              typedLabel={(name) => t("setup.configuration.useTyped", { name })}
              onChange={(option) =>
                setValues((current) => ({ ...current, [field]: option }))
              }
            />
          ) : (
            <Input
              id={`setup-${field}`}
              name={field}
              value={values[field]}
              onChange={update(field)}
              required
              autoComplete="off"
            />
          )}
          <p className="text-xs text-muted-foreground">
            {t(`setup.configuration.${field}Help`)}
          </p>
          {field !== "prefix" && lists[field]?.status === "unavailable" && (
            <p className="text-xs text-muted-foreground">
              {t(`setup.configuration.${PICKER_COPY[field]}ListUnavailable`)}
            </p>
          )}
          {field === "audience_group" && groupList?.status === "ready" && groupList.truncated && (
            <p className="text-xs text-muted-foreground">
              {t("setup.configuration.groupListTruncated", { count: groupList.options.length })}
            </p>
          )}
        </div>
      ))}
      {errorCode && (
        <p className="text-sm text-destructive">
          {t(`setup.configuration.errors.${errorCode}`, {
            defaultValue: t("setup.configuration.errors.default"),
          })}
        </p>
      )}
      <div className="flex flex-wrap gap-2">
        <Button
          type="submit"
          size="sm"
          disabled={isSubmitting || choiceMissing}
        >
          {t("setup.configuration.submit")}
        </Button>
        {onCancel && (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={isSubmitting}
            onClick={onCancel}
          >
            {t("common.cancel")}
          </Button>
        )}
      </div>
    </form>
  );
}
