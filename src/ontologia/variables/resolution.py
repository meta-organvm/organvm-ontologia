"""Full variable resolution engine.

Resolves variables through the scope hierarchy with support for
computed values, environment variables, references, and constraints.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from ontologia.variables.inheritance import find_in_chain
from ontologia.variables.variable import (
    Constraint,
    Mutability,
    Scope,
    Variable,
    VariableType,
)


@dataclass
class ResolvedVariable:
    """Result of variable resolution."""

    key: str
    value: Any
    source_scope: Scope
    source_entity_id: str | None
    resolved_from: str  # "direct", "inherited", "computed", "environment", "reference", "default"


@dataclass
class VariableStore:
    """In-memory store for variables with resolution capabilities."""

    _variables: dict[str, list[Variable]] = field(default_factory=dict)

    def set(self, var: Variable) -> tuple[bool, str]:
        """Set a variable, validating constraints.

        Returns (success, error_message).
        """
        # Check constraint
        if var.constraint:
            valid, msg = var.constraint.validate(var.value)
            if not valid:
                return False, msg

        # Check mutability for existing constants
        existing = self._find_exact(var.key, var.scope, var.entity_id)
        if existing and existing.mutability == Mutability.CONSTANT:
            return False, f"Variable {var.key!r} is constant and cannot be modified"

        # Store (replace if same key/scope/entity)
        if var.key not in self._variables:
            self._variables[var.key] = []
        vars_list = self._variables[var.key]
        for i, v in enumerate(vars_list):
            if v.scope == var.scope and v.entity_id == var.entity_id:
                vars_list[i] = var
                return True, ""
        vars_list.append(var)
        return True, ""

    def get(self, key: str, scope: Scope = Scope.GLOBAL, entity_id: str | None = None) -> Variable | None:
        """Direct lookup without inheritance."""
        return self._find_exact(key, scope, entity_id)

    def resolve(
        self,
        key: str,
        scope: Scope = Scope.GLOBAL,
        entity_chain: list[str | None] | None = None,
        default: Any = None,
    ) -> ResolvedVariable:
        """Resolve a variable through the full pipeline.

        Resolution order:
        1. Direct match at requested scope
        2. Inherited from parent scope (walk up chain)
        3. Computed (if mutability=computed, evaluate expression)
        4. Environment (if mutability=environment, read env var)
        5. Reference (if mutability=reference, resolve referenced var)
        6. Default value

        Args:
            key: Variable key.
            scope: Starting scope.
            entity_chain: Entity IDs for scope chain resolution.
            default: Fallback value if not found.

        Returns:
            ResolvedVariable with value and provenance.
        """
        var = find_in_chain(self._variables, key, scope, entity_chain)

        if var is None:
            return ResolvedVariable(
                key=key,
                value=default,
                source_scope=scope,
                source_entity_id=None,
                resolved_from="default",
            )

        # Handle special mutabilities
        if var.mutability == Mutability.ENVIRONMENT:
            env_key = str(var.value) if var.value else key
            env_val = os.environ.get(env_key, default)
            return ResolvedVariable(
                key=key,
                value=env_val,
                source_scope=var.scope,
                source_entity_id=var.entity_id,
                resolved_from="environment",
            )

        if var.mutability == Mutability.REFERENCE and isinstance(var.value, str):
            # Resolve the referenced variable
            ref_result = self.resolve(var.value, scope=Scope.GLOBAL)
            return ResolvedVariable(
                key=key,
                value=ref_result.value,
                source_scope=var.scope,
                source_entity_id=var.entity_id,
                resolved_from="reference",
            )

        resolved_from = "direct" if var.scope == scope else "inherited"
        return ResolvedVariable(
            key=key,
            value=var.value,
            source_scope=var.scope,
            source_entity_id=var.entity_id,
            resolved_from=resolved_from,
        )

    def list_keys(self) -> list[str]:
        """List all variable keys."""
        return sorted(self._variables.keys())

    def list_at_scope(self, scope: Scope, entity_id: str | None = None) -> list[Variable]:
        """List all variables defined at a specific scope."""
        results: list[Variable] = []
        for vars_list in self._variables.values():
            for var in vars_list:
                if var.scope != scope:
                    continue
                if entity_id is not None and var.entity_id != entity_id:
                    continue
                results.append(var)
        return results

    def delete(self, key: str, scope: Scope, entity_id: str | None = None) -> bool:
        """Remove a variable binding. Returns True if found and removed."""
        if key not in self._variables:
            return False
        vars_list = self._variables[key]
        for i, var in enumerate(vars_list):
            if var.scope == scope and var.entity_id == entity_id:
                if var.mutability == Mutability.CONSTANT:
                    return False
                vars_list.pop(i)
                if not vars_list:
                    del self._variables[key]
                return True
        return False

    def _find_exact(self, key: str, scope: Scope, entity_id: str | None) -> Variable | None:
        for var in self._variables.get(key, []):
            if var.scope == scope and var.entity_id == entity_id:
                return var
        return None

    def to_list(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for vars_list in self._variables.values():
            for var in vars_list:
                result.append(var.to_dict())
        return result

    @classmethod
    def from_list(cls, data: list[dict[str, Any]]) -> VariableStore:
        store = cls()
        for vdict in data:
            var = Variable.from_dict(vdict)
            store.set(var)
        return store
