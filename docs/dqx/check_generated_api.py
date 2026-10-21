"""Fail the docs build when pydoc-markdown emits a broken API page.

Two defects reached the published site and stayed there, because nothing reads the generated Markdown and
Docusaurus is happy to render it:

1. **Parameter names replaced by numbers.** pydoc-markdown swaps inline-code spans for numbered
   placeholders and restores them afterwards. Mixing backtick styles in one docstring -- a single-backtick
   span among double-backtick ones, or a double-backtick span containing a backtick -- desynchronises the
   restore, and the Arguments list publishes ``0``, ``1``, ``2`` where the parameter names belong. Found at
   35 occurrences across 6 modules, including *is_sql_query_safe*, whose only argument was documented as
   ``1``.

2. **Sphinx cross-reference roles rendered literally.** pydoc-markdown does not understand ``:func:``,
   ``:class:``, ``:meth:``, ``:mod:``, ``:data:`` or ``:attr:``, so each one reaches the reader as raw
   markup. They are also the most common cause of defect 1.

Both are invisible to every other check in the repo: the build succeeds, links resolve, and the page looks
structurally fine unless someone reads the prose. Hence this.

Run:  python docs/dqx/check_generated_api.py
Wired into ``make docs-build`` between pydoc-markdown and the Docusaurus build.
"""

import pathlib
import re
import sys

API_ROOT = pathlib.Path(__file__).parent / "docs" / "reference" / "api"

#: An Arguments or Returns entry whose name became a placeholder number.
LOST_PARAM_NAME = re.compile(r"^\s*-\s+``\d+\s+-", re.MULTILINE)

#: A Sphinx role that pydoc-markdown passed through untouched.
SPHINX_ROLE = re.compile(r":(?:func|class|meth|mod|data|attr):`")


def main() -> int:
    if not API_ROOT.is_dir():
        print(f"error: no generated API docs at {API_ROOT}. Run pydoc-markdown first.", file=sys.stderr)
        return 1

    fatal: list[str] = []
    warnings: list[str] = []
    for path in sorted(API_ROOT.rglob("*.md")):
        text = path.read_text(encoding="utf-8")
        relative = path.relative_to(API_ROOT.parent.parent.parent)
        roles = len(SPHINX_ROLE.findall(text))
        lost = len(LOST_PARAM_NAME.findall(text))
        if roles:
            fatal.append(f"{relative}: {roles} Sphinx role(s) rendered as literal text")
        if lost:
            warnings.append(f"{relative}: {lost} parameter name(s) replaced by a placeholder number")

    if warnings:
        # A warning rather than an error, deliberately. The mangling happens inside pydoc-markdown's own
        # pipeline and the trigger is not yet isolated: an unclosed inline-code span, an odd count of
        # double-backtick delimiters before the Args section, and stray ``**`` inside a code span were each
        # tested and each ruled out on a docstring that still corrupts. Failing the build on a defect nobody
        # can currently act on would only teach people to delete this check.
        print(f"warning: {sum(int(w.split()[1]) for w in warnings)} mangled parameter name(s):", file=sys.stderr)
        for warning in warnings:
            print(f"  {warning}", file=sys.stderr)
        print("  cause is inside pydoc-markdown and not yet isolated; tracked separately.\n", file=sys.stderr)

    if fatal:
        print("Broken generated API documentation:\n", file=sys.stderr)
        for failure in fatal:
            print(f"  {failure}", file=sys.stderr)
        print(
            "\npydoc-markdown does not understand Sphinx roles, so each one reaches the reader as raw markup. "
            "House style is italics for object names (*column*, not ``column``) -- see AGENTS.md, "
            "'Backticks cause rendering issues in API docs'.",
            file=sys.stderr,
        )
        return 1

    print("generated API docs: no literal Sphinx roles")
    return 0


if __name__ == "__main__":
    sys.exit(main())
