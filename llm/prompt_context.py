"""Compact Gold-schema context for future LLM prompts."""

from __future__ import annotations

import logging

from spark.gold_catalog import GOLD_TABLES

LOGGER = logging.getLogger(__name__)


def build_schema_context() -> str:
    """Render the canonical Gold catalog as compact prompt text."""
    lines = [
        "Gold tables (all are built from valid Silver rows):",
        "Day-of-week encoding: Spark convention, 1 = Sunday and 7 = Saturday.",
    ]
    for table in GOLD_TABLES:
        lines.extend([f"{table.name} (grain: {table.grain}):"])
        lines.extend(
            f"- {column.name} ({column.spark_type}): {column.description}"
            for column in table.columns
        )
    context = "\n".join(lines)
    LOGGER.info(
        "Built Gold schema context, approximately %d tokens",
        len(context.split()),
    )
    return context
