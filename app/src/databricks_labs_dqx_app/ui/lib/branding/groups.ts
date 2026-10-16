import { oklchToHex } from "./color";

export const COLOR_GROUPS = ["header", "page_background", "text", "brand", "sidebar"] as const;
export type ColorGroup = (typeof COLOR_GROUPS)[number];
export type Mode = "light" | "dark";
export type GroupColors = Partial<Record<ColorGroup, string>>;

const grey = (l: number) => oklchToHex({ l, c: 0, h: 0 });

/** Hex equivalents of today's globals.css values (DQX Default). */
export const DEFAULT_GROUPS: Record<Mode, Record<ColorGroup, string>> = {
  light: { header: grey(1), page_background: grey(1), text: grey(0.145), brand: grey(0.205), sidebar: grey(0.985) },
  dark: { header: grey(0.145), page_background: grey(0.145), text: grey(0.985), brand: grey(0.985), sidebar: grey(0.205) },
};
