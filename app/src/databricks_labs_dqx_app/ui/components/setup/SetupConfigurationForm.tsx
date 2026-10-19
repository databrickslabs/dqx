import { useState } from "react";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { SetupCatalogPicker } from "@/components/setup/SetupCatalogPicker";
import type { SetupCatalogList } from "@/lib/setup-catalogs";

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
  catalogList?: SetupCatalogList;
  /** Closes the form without saving, when changing previously saved choices. */
  onCancel?: () => void;
};

const FIELDS = ["catalog", "prefix", "audience_group"] as const;

export function SetupConfigurationForm({
  isSubmitting,
  errorCode,
  onSubmit,
  initialValues,
  catalogList,
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
  const pickerList =
    catalogList && catalogList.status !== "unavailable" ? catalogList : null;

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
          {field === "catalog" && pickerList ? (
            <SetupCatalogPicker
              id="setup-catalog"
              value={values.catalog}
              catalogs={pickerList.status === "ready" ? pickerList.catalogs : []}
              isLoading={pickerList.status === "loading"}
              onChange={(catalog) =>
                setValues((current) => ({ ...current, catalog }))
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
          {field === "catalog" && catalogList?.status === "unavailable" && (
            <p className="text-xs text-muted-foreground">
              {t("setup.configuration.catalogListUnavailable")}
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
          disabled={isSubmitting || (!!pickerList && !values.catalog.trim())}
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
