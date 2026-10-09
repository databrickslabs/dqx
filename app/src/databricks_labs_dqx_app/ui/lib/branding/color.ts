/** Small, dependency-free colour maths for custom styling (sRGB ⇄ OKLab/OKLCH, WCAG contrast). */

export type Oklch = { l: number; c: number; h: number };
type Oklab = { L: number; a: number; b: number };

const HEX_RE = /^#[0-9A-Fa-f]{6}$/;

export function isHex(value: unknown): value is string {
  return typeof value === "string" && HEX_RE.test(value);
}

function hexToRgb(hex: string): [number, number, number] {
  if (!isHex(hex)) throw new Error("invalid hex colour");
  return [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255) as [number, number, number];
}

function rgbToHex([r, g, b]: [number, number, number]): string {
  const to = (v: number) => Math.round(Math.min(1, Math.max(0, v)) * 255).toString(16).padStart(2, "0");
  return `#${to(r)}${to(g)}${to(b)}`.toUpperCase();
}

const toLinear = (v: number) => (v <= 0.04045 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4);
const toGamma = (v: number) => (v <= 0.0031308 ? 12.92 * v : 1.055 * v ** (1 / 2.4) - 0.055);

function rgbToOklab(rgb: [number, number, number]): Oklab {
  const [r, g, b] = rgb.map(toLinear);
  const l = Math.cbrt(0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b);
  const m = Math.cbrt(0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b);
  const s = Math.cbrt(0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b);
  return {
    L: 0.2104542553 * l + 0.793617785 * m - 0.0040720468 * s,
    a: 1.9779984951 * l - 2.428592205 * m + 0.4505937099 * s,
    b: 0.0259040371 * l + 0.7827717662 * m - 0.808675766 * s,
  };
}

function oklabToRgb({ L, a, b }: Oklab): [number, number, number] {
  const l = (L + 0.3963377774 * a + 0.2158037573 * b) ** 3;
  const m = (L - 0.1055613458 * a - 0.0638541728 * b) ** 3;
  const s = (L - 0.0894841775 * a - 1.291485548 * b) ** 3;
  return [
    4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s,
    -1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s,
    -0.0041960863 * l - 0.7034186147 * m + 1.707614701 * s,
  ].map(toGamma) as [number, number, number];
}

function hexToOklab(hex: string): Oklab {
  return rgbToOklab(hexToRgb(hex));
}

function oklabToHex(lab: Oklab): string {
  return rgbToHex(oklabToRgb(lab));
}

export function hexToOklch(hex: string): Oklch {
  const { L, a, b } = hexToOklab(hex);
  const c = Math.sqrt(a * a + b * b);
  const h = c < 1e-4 ? 0 : ((Math.atan2(b, a) * 180) / Math.PI + 360) % 360;
  return { l: L, c, h };
}

export function oklchToHex({ l, c, h }: Oklch): string {
  const rad = (h * Math.PI) / 180;
  return oklabToHex({ L: l, a: c * Math.cos(rad), b: c * Math.sin(rad) });
}

export function mix(a: string, b: string, t: number): string {
  const x = hexToOklab(a);
  const y = hexToOklab(b);
  return oklabToHex({ L: x.L + (y.L - x.L) * t, a: x.a + (y.a - x.a) * t, b: x.b + (y.b - x.b) * t });
}

export function withLightness(hex: string, l: number): string {
  const c = hexToOklch(hex);
  return oklchToHex({ ...c, l: Math.min(1, Math.max(0, l)) });
}

export function tint(hex: string, toward: string, chromaShare: number): string {
  const base = hexToOklab(hex);
  const t = hexToOklch(toward);
  if (t.c < 0.01) return hex;
  const rad = (t.h * Math.PI) / 180;
  const add = t.c * chromaShare;
  return oklabToHex({ L: base.L, a: base.a + add * Math.cos(rad), b: base.b + add * Math.sin(rad) });
}

export function relativeLuminance(hex: string): number {
  const [r, g, b] = hexToRgb(hex).map(toLinear);
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

export function contrastRatio(a: string, b: string): number {
  const [hi, lo] = [relativeLuminance(a), relativeLuminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

const NEAR_BLACK = "#0A0A0A";
const NEAR_WHITE = "#FAFAFA";

export function readableOn(bg: string): string {
  return contrastRatio(bg, NEAR_BLACK) >= contrastRatio(bg, NEAR_WHITE) ? NEAR_BLACK : NEAR_WHITE;
}
