import { useState } from "react";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

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
};

const FIELDS = ["catalog", "prefix", "audience_group"] as const;

export function SetupConfigurationForm({
  isSubmitting,
  errorCode,
  onSubmit,
  initialValues,
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
          <Input
            id={`setup-${field}`}
            name={field}
            value={values[field]}
            onChange={update(field)}
            required
            autoComplete="off"
          />
          <p className="text-xs text-muted-foreground">
            {t(`setup.configuration.${field}Help`)}
          </p>
        </div>
      ))}
      {errorCode && (
        <p className="text-sm text-destructive">
          {t(`setup.configuration.errors.${errorCode}`, {
            defaultValue: t("setup.configuration.errors.default"),
          })}
        </p>
      )}
      <Button type="submit" size="sm" disabled={isSubmitting}>
        {t("setup.configuration.submit")}
      </Button>
    </form>
  );
}
