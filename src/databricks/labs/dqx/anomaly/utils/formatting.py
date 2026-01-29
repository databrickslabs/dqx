"""Formatting utilities for anomaly detection output."""


def format_contributions_map(contributions_map: dict[str, float | None] | None, top_n: int) -> str:
    """Format contributions map as string for top N contributors.

    Args:
        contributions_map: Dictionary mapping feature names to contribution values (0-1 range)
        top_n: Number of top contributors to include

    Returns:
        Formatted string like "amount (85%), quantity (10%), discount (5%)"
        Empty string if contributions_map is None or empty

    Example:
        >>> format_contributions_map({"amount": 0.85, "quantity": 0.10}, 2)
        'amount (85%), quantity (10%)'
    """
    if not contributions_map:
        return ""

    # Sort by absolute contribution value (descending) to rank by impact magnitude
    sorted_contribs = sorted(
        contributions_map.items(), key=lambda x: abs(x[1]) if x[1] is not None else 0.0, reverse=True
    )

    # Take top N
    top_contribs = sorted_contribs[:top_n]

    # Format as string: "amount (85%), quantity (10%), discount (5%)"
    parts = [f"{col} ({val*100:.0f}%)" for col, val in top_contribs if val is not None]
    return ", ".join(parts)
