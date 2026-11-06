/** True when an element's content overflows its box (i.e. text-overflow is clipping it). */
export function isClipped(el: { scrollWidth: number; clientWidth: number } | null): boolean {
  return el !== null && el.scrollWidth > el.clientWidth;
}
