from functools import reduce

from pyspark.sql.functions import col, upper

def build_condition(rule):
    """
    Converts a single JSON rule into a Spark Column condition.
    """

    field = rule["field"]
    op = rule["operator"]
    value = rule.get("value")

    # --------------------------
    # Equality
    # --------------------------
    if op == "=":
        return col(field) == value

    elif op == "!=":
        return col(field) != value

    # --------------------------
    # Trim-aware Equality
    # --------------------------
    elif op == "= (TRIM)":
        return trim(col(field)) == value

    elif op == "!= (TRIM)":
        return trim(col(field)) != value

    # --------------------------
    # Case-insensitive Equality
    # --------------------------
    elif op == "= (IGNORE CASE)":
        return upper(col(field)) == value.upper()

    elif op == "!= (IGNORE CASE)":
        return upper(col(field)) != value.upper()

    # --------------------------
    # Case-insensitive + Trim
    # --------------------------
    elif op == "= (IGNORE CASE TRIM)":
        return upper(trim(col(field))) == value.upper()

    elif op == "!= (IGNORE CASE TRIM)":
        return upper(trim(col(field))) != value.upper()

    # --------------------------
    # IN / NOT IN
    # --------------------------
    elif op == "IN":
        return col(field).isin(*value)

    elif op == "NOT IN":
        return ~col(field).isin(*value)

    # --------------------------
    # Mathematical Inequalities
    # --------------------------
    elif op == ">":
        return col(field) > value

    elif op == ">=":
        return col(field) >= value

    elif op == "<":
        return col(field) < value

    elif op == "<=":
        return col(field) <= value

    # --------------------------
    # Null Checks
    # --------------------------
    elif op == "IS NULL":
        return col(field).isNull()

    elif op == "IS NOT NULL":
        return col(field).isNotNull()

    # --------------------------
    # Blank Checks
    # --------------------------
    elif op == "IS BLANK":
        return (
            col(field).isNull()
            | (trim(col(field)) == "")
        )

    elif op == "IS NOT BLANK":
        return (
            col(field).isNotNull()
            & (trim(col(field)) != "")
        )

    # --------------------------
    # Between
    # --------------------------
    elif op == "BETWEEN":
        return col(field).between(value[0], value[1])

    # --------------------------
    # Like
    # --------------------------
    elif op == "LIKE":
        return col(field).like(value)

    elif op == "NOT LIKE":
        return ~col(field).like(value)

    # --------------------------
    # Regex
    # --------------------------
    elif op == "RLIKE":
        return col(field).rlike(value)

    elif op == "NOT RLIKE":
        return ~col(field).rlike(value)
    
    elif op == "NOT RLIKE (IGNORE CASE)":
        return ~upper(col(field)).rlike(value.upper())
    
    elif op == "RLIKE (IGNORE CASE)":
        return upper(col(field)).rlike(value.upper())

    else:
        raise ValueError(
            f"Unsupported operator '{op}' "
            f"for field '{field}'"
        )

def build_group(group):
    """
    Recursively builds nested AND/OR logic groups.

    Example:

    {
      "logic": "OR",
      "rules": [
        {
          "logic": "AND",
          "rules": [...]
        },
        {
          "logic": "AND",
          "rules": [...]
        }
      ]
    }
    """

    logic = group.get("logic", "AND").upper()

    conditions = []

    for item in group.get("rules", []):

        # Skip disabled rules/groups
        if not item.get("enabled", True):
            print(
                f"[RULE_ENGINE] Skipping disabled item: "
                f"{item.get('field', 'GROUP')}"
            )
            continue

        # Nested group
        if "rules" in item:
            nested_condition = build_group(item)

            if nested_condition is not None:
                conditions.append(nested_condition)

        # Single rule
        else:
            conditions.append(build_condition(item))

    if not conditions:
        return None

    if logic == "AND":
        return reduce(lambda a, b: a & b, conditions)

    elif logic == "OR":
        return reduce(lambda a, b: a | b, conditions)

    else:
        raise ValueError(
            f"Unsupported group logic '{logic}'. "
            f"Expected AND or OR."
        )


def apply_rules(df, rules: dict):
    """
    Applies JSON-driven rules to a Spark DataFrame.

    Supports:

    - Global enable/disable
    - Rule enable/disable
    - Nested AND groups
    - Nested OR groups
    - Existing operators

    Backward compatible with:

    {
      "enabled": true,
      "rules": [...]
    }

    and supports:

    {
      "enabled": true,
      "logic": "OR",
      "rules": [...]
    }
    """

    # ------------------------------------------------------------------
    # Global Kill Switch
    # ------------------------------------------------------------------
    if not rules.get("enabled", True):
        raise RuntimeError(
            "[RULE_ENGINE FATAL] Rules are globally disabled "
            "(enabled: false). In a Configuration-Driven "
            "(Static SQL) architecture, bypassing rules would "
            "publish unfiltered raw data. Halting pipeline "
            "to prevent compliance breach."
        )

    condition = build_group(rules)

    if condition is None:
        print(
            "[RULE_ENGINE] No active rules found. "
            "Returning original DataFrame."
        )
        return df

    return df.filter(condition)