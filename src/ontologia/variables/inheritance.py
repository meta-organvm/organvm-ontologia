"""Variable inheritance — cascade resolution through scope hierarchy.

When a variable is requested at a narrow scope (e.g., repo), the resolver
walks up: repo → organ → global. First explicit value wins. This provides
hierarchical defaults with override capability.
"""

from __future__ import annotations

from ontologia.variables.variable import Scope, Variable, SCOPE_ORDER


def scope_chain(scope: Scope) -> list[Scope]:
    """Return the resolution chain from the given scope up to global.

    Example: scope_chain(Scope.REPO) → [REPO, ORGAN, GLOBAL]
    """
    idx = SCOPE_ORDER.index(scope)
    return list(reversed(SCOPE_ORDER[: idx + 1]))


def find_in_chain(
    variables: dict[str, list[Variable]],
    key: str,
    scope: Scope,
    entity_chain: list[str | None] | None = None,
) -> Variable | None:
    """Walk the scope chain to find the first matching variable.

    Args:
        variables: Dict mapping variable key → list of Variable objects.
        key: Variable key to look up.
        scope: Starting scope (narrows to broadest).
        entity_chain: Ordered list of entity IDs for each scope level,
            from narrowest to broadest. None entries mean "any entity at
            that scope". Length should match the scope chain.

    Returns:
        First matching Variable, or None.
    """
    candidates = variables.get(key, [])
    if not candidates:
        return None

    chain = scope_chain(scope)
    entity_map: dict[Scope, str | None] = {}
    if entity_chain:
        for i, s in enumerate(chain):
            entity_map[s] = entity_chain[i] if i < len(entity_chain) else None

    for check_scope in chain:
        entity_id = entity_map.get(check_scope)
        for var in candidates:
            if var.scope != check_scope:
                continue
            # If entity_id specified, must match; if None, accept any
            if entity_id is not None and var.entity_id is not None:
                if var.entity_id != entity_id:
                    continue
            return var

    return None


def collect_overrides(
    variables: dict[str, list[Variable]],
    key: str,
    scope: Scope,
) -> list[Variable]:
    """Collect all variable bindings for a key across the scope chain.

    Returns them ordered from narrowest (most specific) to broadest.
    """
    candidates = variables.get(key, [])
    chain = scope_chain(scope)
    result: list[Variable] = []

    for check_scope in chain:
        for var in candidates:
            if var.scope == check_scope:
                result.append(var)

    return result
