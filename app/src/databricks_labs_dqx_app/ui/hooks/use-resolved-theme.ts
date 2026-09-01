import { useEffect, useState } from "react";
import { useTheme } from "@/components/layout/theme-provider";

/**
 * Resolves the `ThemeProvider`'s `theme` (`"light" | "dark" | "system"`) down
 * to the actual rendered mode, tracking `prefers-color-scheme` and the `dark`
 * class the provider places on `<html>` live — so a consumer that needs to
 * pick a matching visual (e.g. the CodeMirror predicate editor's syntax
 * theme, item 14) updates immediately when the user flips the app's theme
 * toggle, without a page reload. Mirrors the inline resolution logic in
 * `components/backgrounds/gradient.tsx`.
 */
export function useResolvedTheme(): "light" | "dark" {
  const { theme } = useTheme();
  const [resolved, setResolved] = useState<"light" | "dark">(() => {
    if (theme === "dark") return "dark";
    if (theme === "light") return "light";
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  });

  useEffect(() => {
    if (theme === "dark") {
      setResolved("dark");
      return;
    }
    if (theme === "light") {
      setResolved("light");
      return;
    }
    // "system" — track both the live media query and the `dark` class
    // ThemeProvider toggles on `<html>` (belt-and-suspenders: either signal
    // changing should re-resolve).
    const check = () =>
      setResolved(document.documentElement.classList.contains("dark") ? "dark" : "light");
    check();
    const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
    const observer = new MutationObserver(check);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    mediaQuery.addEventListener("change", check);
    return () => {
      observer.disconnect();
      mediaQuery.removeEventListener("change", check);
    };
  }, [theme]);

  return resolved;
}
