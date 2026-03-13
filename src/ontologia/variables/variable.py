"""Variable model — typed, scoped variables with constraints.

Variables are named bindings that can be attached at any scope level
in the hierarchy (global, organ, repo, module, document, session).
Resolution walks up the scope chain; first explicit value wins.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class VariableType(str, Enum):
    """Data types for variable values."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    LIST = "list"
    DICT = "dict"


class Mutability(str, Enum):
    """How a variable's value can change."""

    CONSTANT = "constant"        # Set once at creation, never changes
    RUNTIME = "runtime"          # Can be changed freely
    COMPUTED = "computed"         # Derived from an expression
    CONDITIONAL = "conditional"  # Value depends on context
    ENVIRONMENT = "environment"  # Reads from environment variable
    REFERENCE = "reference"      # Pointer to another variable


class Scope(str, Enum):
    """Hierarchical scope levels, from broadest to narrowest."""

    GLOBAL = "global"
    ORGAN = "organ"
    REPO = "repo"
    MODULE = "module"
    DOCUMENT = "document"
    SESSION = "session"


# Scope ordering for inheritance (broadest first)
SCOPE_ORDER: list[Scope] = [
    Scope.GLOBAL,
    Scope.ORGAN,
    Scope.REPO,
    Scope.MODULE,
    Scope.DOCUMENT,
    Scope.SESSION,
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Constraint:
    """Validation constraint for a variable value."""

    allowed_values: list[Any] | None = None
    min_value: float | None = None
    max_value: float | None = None
    pattern: str | None = None  # regex for string values
    required: bool = False

    def validate(self, value: Any) -> tuple[bool, str]:
        """Validate a value against this constraint.

        Returns (is_valid, error_message).
        """
        if value is None:
            if self.required:
                return False, "Value is required"
            return True, ""

        if self.allowed_values is not None and value not in self.allowed_values:
            return False, f"Value {value!r} not in allowed values {self.allowed_values}"

        if self.min_value is not None:
            try:
                if float(value) < self.min_value:
                    return False, f"Value {value} below minimum {self.min_value}"
            except (TypeError, ValueError):
                pass

        if self.max_value is not None:
            try:
                if float(value) > self.max_value:
                    return False, f"Value {value} above maximum {self.max_value}"
            except (TypeError, ValueError):
                pass

        if self.pattern is not None and isinstance(value, str):
            import re
            if not re.match(self.pattern, value):
                return False, f"Value {value!r} does not match pattern {self.pattern!r}"

        return True, ""

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        if self.allowed_values is not None:
            d["allowed_values"] = self.allowed_values
        if self.min_value is not None:
            d["min_value"] = self.min_value
        if self.max_value is not None:
            d["max_value"] = self.max_value
        if self.pattern is not None:
            d["pattern"] = self.pattern
        if self.required:
            d["required"] = True
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Constraint:
        return cls(
            allowed_values=data.get("allowed_values"),
            min_value=data.get("min_value"),
            max_value=data.get("max_value"),
            pattern=data.get("pattern"),
            required=data.get("required", False),
        )


@dataclass
class Variable:
    """A named variable binding at a specific scope."""

    key: str
    value: Any
    var_type: VariableType = VariableType.STRING
    mutability: Mutability = Mutability.RUNTIME
    scope: Scope = Scope.GLOBAL
    entity_id: str | None = None  # Scope anchor (e.g., organ/repo UID)
    constraint: Constraint | None = None
    description: str = ""
    updated_at: str = field(default_factory=_now_iso)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "key": self.key,
            "value": self.value,
            "var_type": self.var_type.value,
            "mutability": self.mutability.value,
            "scope": self.scope.value,
            "updated_at": self.updated_at,
        }
        if self.entity_id is not None:
            d["entity_id"] = self.entity_id
        if self.constraint is not None:
            d["constraint"] = self.constraint.to_dict()
        if self.description:
            d["description"] = self.description
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Variable:
        constraint = None
        if "constraint" in data:
            constraint = Constraint.from_dict(data["constraint"])
        return cls(
            key=data["key"],
            value=data.get("value"),
            var_type=VariableType(data.get("var_type", "string")),
            mutability=Mutability(data.get("mutability", "runtime")),
            scope=Scope(data.get("scope", "global")),
            entity_id=data.get("entity_id"),
            constraint=constraint,
            description=data.get("description", ""),
            updated_at=data.get("updated_at", ""),
        )
