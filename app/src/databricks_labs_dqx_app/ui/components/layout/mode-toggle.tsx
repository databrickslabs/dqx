import { Moon, Sun } from "lucide-react";
import { useTranslation } from "react-i18next";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useTheme } from "@/components/layout/theme-provider";

export function ModeToggle({ className }: { className?: string } = {}) {
  const { t } = useTranslation();
  const { theme, setTheme } = useTheme();

  const toggleTheme = () => {
    setTheme(theme === "light" ? "dark" : "light");
  };

  return (
    <Button
      variant="ghost"
      size="icon"
      onClick={toggleTheme}
      className={cn("w-8 h-8 p-2 rounded-sm transition-transform duration-200 ease-in-out hover:scale-110", className)}
    >
      {theme === "light" ? (
        <Sun className="h-4 w-4 rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0" />
      ) : (
        <Moon className="rotate-90 h-4 w-4 scale-0 transition-all dark:rotate-0 dark:scale-100" />
      )}
      <span className="sr-only">{t("navbar.toggleTheme")}</span>
    </Button>
  );
}
