"""Human-readable casing helpers.

Shared by the check-function friendly labels (``routes/v1/check_functions.py``)
and the AI field-suggestion post-processing (``services/ai_rules_service.py``),
so both render ``snake_case`` identifiers the same way.
"""

import re

# Tokens that should be upper-cased once text has been title-cased. Applied on
# WORD boundaries, not as substrings: a substring pass would turn "Identifier"
# into "IDentifier" and "Piicheck" into "PIIcheck".
_ACRONYMS: tuple[tuple[str, str], ...] = (
    ("Sql", "SQL"),
    ("Ipv4", "IPv4"),
    ("Ipv6", "IPv6"),
    ("Ip", "IP"),
    ("Json", "JSON"),
    ("Pii", "PII"),
    ("Url", "URL"),
    ("Id", "ID"),
)


def _capitalize_word(word: str) -> str:
    """Upper-case a word's first letter, lower-casing the rest.

    Unlike :meth:`str.title` this only capitalises after whitespace, so
    ``"customer's"`` becomes ``"Customer's"`` rather than ``"Customer'S"``.
    """
    lowered = word.lower()
    for index, char in enumerate(lowered):
        if char.isalpha():
            return lowered[:index] + char.upper() + lowered[index + 1 :]
    return lowered


def to_title_case(text: str) -> str:
    """Render *text* as Title Case, treating underscores as word separators.

    Handles both ``snake_case`` identifiers and free prose, so it can normalise
    an LLM suggestion whichever style the model happened to produce:

    * ``"is_not_null"`` -> ``"Is Not Null"``
    * ``"order amount must be positive"`` -> ``"Order Amount Must Be Positive"``
    * ``"is_valid_url"`` -> ``"Is Valid URL"``

    Args:
        text: Identifier or prose to re-case. Empty/whitespace-only input
            returns an empty string.

    Returns:
        The title-cased text with known acronyms upper-cased and runs of
        whitespace collapsed to single spaces.
    """
    words = text.replace("_", " ").split()
    titled = " ".join(_capitalize_word(word) for word in words)
    for mixed, upper in _ACRONYMS:
        titled = re.sub(rf"\b{mixed}\b", upper, titled)
    return titled
