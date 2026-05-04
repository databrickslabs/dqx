"use client";

import * as React from "react";
import { motion, type HTMLMotionProps } from "motion/react";

import { cn } from "@/lib/utils";
import { useTheme } from "@/components/apx/theme-provider";

type GradientBackgroundProps = HTMLMotionProps<"div">;

function GradientBackground({
  className,
  transition = { duration: 15, ease: "easeInOut", repeat: Infinity },
  ...props
}: GradientBackgroundProps) {
  const { theme } = useTheme();

  // Detect actual theme (handles "system" by checking document class and media query)
  const [isDark, setIsDark] = React.useState(() => {
    if (theme === "dark") return true;
    if (theme === "light") return false;
    // For "system", check media query
    return window.matchMedia("(prefers-color-scheme: dark)").matches;
  });

  React.useEffect(() => {
    if (theme === "dark") {
      setIsDark(true);
      return;
    }
    if (theme === "light") {
      setIsDark(false);
      return;
    }

    // For "system", listen to both class changes and media query
    const checkSystemTheme = () => {
      const hasDarkClass = document.documentElement.classList.contains("dark");
      setIsDark(hasDarkClass);
    };

    checkSystemTheme();
    const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
    const handleChange = () => checkSystemTheme();

    // Listen to class changes via MutationObserver
    const observer = new MutationObserver(checkSystemTheme);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class"],
    });

    mediaQuery.addEventListener("change", handleChange);

    return () => {
      observer.disconnect();
      mediaQuery.removeEventListener("change", handleChange);
    };
  }, [theme]);

  // Light theme: Soft cyan/blue/purple gradient - subtle and elegant with good contrast
  // Dark theme: Rich indigo/purple/violet gradient - deep and modern with good visibility
  const gradientClasses = isDark
    ? "bg-gradient-to-br from-indigo-900 via-purple-900 to-violet-900 bg-[length:400%_400%]"
    : "bg-gradient-to-br from-cyan-200 via-sky-200 to-purple-200 bg-[length:400%_400%]";

  return (
    <motion.div
      data-slot="gradient-background"
      className={cn("size-full", gradientClasses, className)}
      animate={{ backgroundPosition: ["0% 50%", "100% 50%", "0% 50%"] }}
      transition={transition}
      {...props}
    />
  );
}

export { GradientBackground, type GradientBackgroundProps };
