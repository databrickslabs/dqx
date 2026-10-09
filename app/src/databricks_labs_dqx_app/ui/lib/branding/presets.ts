import type { ColorGroup } from "./groups";

export const PRESET_IDS = [
  "dqx-default", "databricks", "aubergine", "ocean", "dimmed", "high-contrast", "nord", "solarized", "dracula",
] as const;
export type PresetId = (typeof PRESET_IDS)[number];
type Full = Record<ColorGroup, string>;
export type Preset = { id: PresetId; light: Full | Record<string, never>; dark: Full | Record<string, never> };

export const PRESETS: readonly Preset[] = [
  { id: "dqx-default", light: {}, dark: {} },
  {
    id: "databricks",
    light: { header: "#1B3139", page_background: "#FFFFFF", text: "#1B3139", brand: "#FF3621", sidebar: "#F9F7F4" },
    dark: { header: "#0B2026", page_background: "#121A1D", text: "#EEEDE9", brand: "#FF5F46", sidebar: "#1B3139" },
  },
  {
    id: "aubergine",
    light: { header: "#3F0E40", page_background: "#FFFFFF", text: "#1D1C1D", brand: "#1164A3", sidebar: "#3F0E40" },
    dark: { header: "#2C092D", page_background: "#1A1D21", text: "#D1D2D3", brand: "#1D9BD1", sidebar: "#3F0E40" },
  },
  {
    id: "ocean",
    light: { header: "#303E4D", page_background: "#FFFFFF", text: "#1F2933", brand: "#2F6FAE", sidebar: "#303E4D" },
    dark: { header: "#1F2933", page_background: "#161D26", text: "#E4E7EB", brand: "#6698C8", sidebar: "#303E4D" },
  },
  {
    id: "dimmed",
    light: { header: "#F6F8FA", page_background: "#F6F8FA", text: "#24292F", brand: "#0969DA", sidebar: "#EAEEF2" },
    dark: { header: "#2D333B", page_background: "#22272E", text: "#ADBAC7", brand: "#539BF5", sidebar: "#2D333B" },
  },
  {
    id: "high-contrast",
    light: { header: "#FFFFFF", page_background: "#FFFFFF", text: "#000000", brand: "#0349B4", sidebar: "#FFFFFF" },
    dark: { header: "#0A0C10", page_background: "#0A0C10", text: "#FFFFFF", brand: "#71B7FF", sidebar: "#0A0C10" },
  },
  {
    id: "nord",
    light: { header: "#ECEFF4", page_background: "#ECEFF4", text: "#2E3440", brand: "#5E81AC", sidebar: "#E5E9F0" },
    dark: { header: "#2E3440", page_background: "#2E3440", text: "#ECEFF4", brand: "#88C0D0", sidebar: "#3B4252" },
  },
  {
    id: "solarized",
    light: { header: "#EEE8D5", page_background: "#FDF6E3", text: "#586E75", brand: "#268BD2", sidebar: "#EEE8D5" },
    dark: { header: "#073642", page_background: "#002B36", text: "#93A1A1", brand: "#268BD2", sidebar: "#073642" },
  },
  {
    id: "dracula",
    light: { header: "#FFFBEB", page_background: "#FFFBEB", text: "#1F1F1F", brand: "#644AC9", sidebar: "#F2EDD7" },
    dark: { header: "#21222C", page_background: "#282A36", text: "#F8F8F2", brand: "#BD93F9", sidebar: "#21222C" },
  },
];

export function presetById(id: string): Preset | undefined {
  return PRESETS.find((p) => p.id === id);
}
