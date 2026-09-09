/** Return the visible label for a feature's first product release. */
export function availableSinceLabel(
  productName: string,
  version: string,
): string {
  return `Available since ${productName} v${version}`;
}

/** Return the visible label for a feature's product deprecation. */
export function deprecatedInLabel(
  productName: string,
  version: string,
): string {
  return `Deprecated in ${productName} v${version}`;
}
