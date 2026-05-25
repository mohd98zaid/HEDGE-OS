"""Internal helper: load a canonical JSON schema from package data.

Each model module loads its `JSON_SCHEMA` via this helper so the
filesystem path is centralised. The schemas live under
`hedge_warm_ai.schemas.json_schemas` as package data.
"""

from __future__ import annotations

from importlib import resources


def load_schema(name: str) -> str:
    """Return the canonical JSON schema for *name* (e.g. ``ai_rank``).

    The schema text is returned verbatim so callers can either:
      * pass it to ``json.loads`` for runtime validation, or
      * feed it to ``hypothesis-jsonschema`` for fuzzed payload generation.

    Raises:
        FileNotFoundError: if *name* does not match a committed schema.
    """
    filename = f"{name}.schema.json"
    package = "hedge_warm_ai.schemas.json_schemas"
    return resources.files(package).joinpath(filename).read_text(encoding="utf-8")
