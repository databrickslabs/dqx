import { FileDown } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";

export const IMPORT_EXAMPLES = {
  rulesYaml: "rules/bakehouse-rules.yaml",
  dataContract: "contracts/bakehouse-sales-contract.yaml",
} as const;

export function getImportExampleUrl(examplePath: string): string {
  return `/examples/imports/${examplePath}`;
}

export function ImportExampleLinks({
  examplePath,
  className,
}: {
  examplePath: string;
  className?: string;
}) {
  const { t } = useTranslation();

  return (
    <Button asChild variant="outline" size="sm" className={`h-8 gap-1.5 ${className ?? ""}`}>
      <a href={getImportExampleUrl(examplePath)} download title={t("rulesImport.downloadExample")}>
        <FileDown className="h-3.5 w-3.5 text-blue-600" />
        {t("rulesImport.exampleFile")}
      </a>
    </Button>
  );
}
