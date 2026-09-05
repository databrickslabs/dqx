/** Return the visible label for a feature's first product release. */
export function availableSinceLabel(
  productName: string,
  version: string,
): string {
  return `Available since ${productName} v${version}`;
}
