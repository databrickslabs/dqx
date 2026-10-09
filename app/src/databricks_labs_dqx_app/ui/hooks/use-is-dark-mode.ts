import { useEffect, useState } from "react";

const readIsDark = () => document.documentElement.classList.contains("dark");

/** Tracks the `dark` class on `<html>` (set by ThemeProvider). */
export function useIsDarkMode(): boolean {
  const [isDark, setIsDark] = useState(readIsDark);
  useEffect(() => {
    const observer = new MutationObserver(() => setIsDark(readIsDark()));
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => observer.disconnect();
  }, []);
  return isDark;
}
