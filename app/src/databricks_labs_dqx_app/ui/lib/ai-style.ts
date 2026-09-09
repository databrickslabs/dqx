// Shared purple-to-pink gradient palette for AI-driven surfaces (rule
// generation, profiler suggestions, etc). These are style *tokens* only —
// not yet wired into any feature. Later Phase 7 tasks consume them.

// Databricks Genie-inspired purple-to-pink gradient palette.
export const AI_GRADIENT_FROM_VIA_TO = "from-violet-500 via-fuchsia-500 to-pink-500";

// Subtle background tint for cards/banners. Light enough to read text on.
export const AI_BANNER_BG =
  "bg-gradient-to-br from-violet-500/5 via-fuchsia-500/5 to-pink-500/10 dark:from-violet-500/10 dark:via-fuchsia-500/10 dark:to-pink-500/15";

// Accent border for cards/banners.
export const AI_BANNER_BORDER = "border-2 border-fuchsia-500/40 dark:border-fuchsia-400/40";

// For icons / sparkle accents.
export const AI_ICON_COLOR = "text-fuchsia-600 dark:text-fuchsia-400";

// For action buttons that fire AI. Background gradient + white text.
export const AI_BUTTON_BG = `bg-gradient-to-r ${AI_GRADIENT_FROM_VIA_TO} hover:opacity-90 text-white border-0 shadow-sm`;

// Subtle gradient text (for badges/labels).
export const AI_TEXT_GRADIENT = `bg-gradient-to-r ${AI_GRADIENT_FROM_VIA_TO} bg-clip-text text-transparent`;

// Reference to the SVG <linearGradient> defined once in routes/__root.tsx.
// Use as `<Sparkles stroke={AI_GRADIENT_URL} />` to give a stroked icon a
// gradient stroke, or `fill={AI_GRADIENT_URL}` for a filled icon.
export const AI_GRADIENT_URL = "url(#dqx-ai-gradient)";
