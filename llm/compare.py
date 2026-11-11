"""Run an ungraded English/French smoke comparison across configured profiles."""

from __future__ import annotations

import argparse
import csv
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from llm.llm_client import LLMError, create_client
from llm.prompt_context import build_schema_context
from llm.settings import load_settings

LOGGER = logging.getLogger(__name__)
SYSTEM_PROMPT = (
    "You are a Spark SQL expert. Write exactly one SELECT statement using only "
    "the provided tables and columns. If the question cannot be answered, output "
    "UNANSWERABLE."
)
QUESTIONS = (
    ("en", "Which hour had the highest demand on January 15, 2024?"),
    ("en", "Which five pickup zones had the highest total revenue?"),
    ("en", "What percentage of valid trips were paid by credit card?"),
    ("en", "How does average demand during weekday hours compare with weekend hours?"),
    ("en", "What is the average fare in each trip-distance bucket?"),
    ("fr", "Combien de courses ont eu lieu chaque jour en janvier 2024 ?"),
    ("fr", "Quelles sont les cinq zones de prise en charge avec le revenu le plus élevé ?"),
    ("fr", "Quelle part des courses valides a été payée par carte bancaire ?"),
    ("fr", "Comment les pics de demande en semaine se comparent-ils à ceux du week-end ?"),
    ("fr", "Quel temps fera-t-il à Paris demain ?"),
)
REPORT_COLUMNS = (
    "question",
    "language",
    "profile",
    "raw_output",
    "latency_s",
    "prompt_tokens",
    "completion_tokens",
    "error",
    "manual_grade",
)


@dataclass
class ComparisonRow:
    question: str
    language: str
    profile: str
    raw_output: str = ""
    latency_s: float | str = ""
    prompt_tokens: int | str = ""
    completion_tokens: int | str = ""
    error: str = ""
    manual_grade: str = ""


def _clean_cell(text: str) -> str:
    return (
        text.replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\n", "<br>")
        .replace("|", "\\|")
    )


def _write_reports(rows: list[ComparisonRow], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    csv_path = output_dir / f"llm_comparison_{timestamp}.csv"
    markdown_path = output_dir / f"llm_comparison_{timestamp}.md"
    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)

    lines = [
        "# LLM smoke-test comparison",
        "",
        "Outputs are recorded for manual review only; SQL is not graded in milestone 3.1.",
        "",
        "| Question | Language | Profile | Raw output | Latency (s) | Prompt tokens | Completion tokens | Error | Manual grade |",
        "|---|---|---|---|---:|---:|---:|---|---|",
    ]
    for row in rows:
        cells = [
            row.question,
            row.language,
            row.profile,
            row.raw_output,
            str(row.latency_s),
            str(row.prompt_tokens),
            str(row.completion_tokens),
            row.error,
            row.manual_grade,
        ]
        lines.append("| " + " | ".join(_clean_cell(cell) for cell in cells) + " |")
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return markdown_path, csv_path


def run_comparison(
    profiles: list[str], output_dir: Path | None = None
) -> tuple[Path, Path]:
    """Run questions for each profile, recording errors without stopping peers."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    schema_context = build_schema_context()
    settings = load_settings()
    rows: list[ComparisonRow] = []
    for profile_name in profiles:
        try:
            client = create_client(profile_name, settings)
        except (ValueError, RuntimeError) as exc:
            LOGGER.warning("Skipping profile %s: %s", profile_name, exc)
            rows.extend(
                ComparisonRow(question, language, profile_name, error=f"skipped: {exc}")
                for language, question in QUESTIONS
            )
            continue

        stopped_reason: str | None = None
        for language, question in QUESTIONS:
            if stopped_reason:
                rows.append(
                    ComparisonRow(
                        question,
                        language,
                        profile_name,
                        error=f"skipped after profile failure: {stopped_reason}",
                    )
                )
                continue
            prompt = f"{schema_context}\n\nQuestion ({language}): {question}"
            try:
                result = client.generate(
                    SYSTEM_PROMPT,
                    prompt,
                    temperature=0.0,
                    max_tokens=512,
                )
            except LLMError as exc:
                reason = str(exc)
                LOGGER.warning(
                    "Skipping profile %s after request failure: %s",
                    profile_name,
                    reason,
                )
                rows.append(
                    ComparisonRow(
                        question,
                        language,
                        profile_name,
                        error=f"skipped: {reason}",
                    )
                )
                stopped_reason = reason
                continue
            rows.append(
                ComparisonRow(
                    question=question,
                    language=language,
                    profile=profile_name,
                    raw_output=result.text,
                    latency_s=round(result.latency_s, 3),
                    prompt_tokens=result.prompt_tokens
                    if result.prompt_tokens is not None
                    else "",
                    completion_tokens=result.completion_tokens
                    if result.completion_tokens is not None
                    else "",
                )
            )

    report_paths = _write_reports(
        rows, output_dir or Path(__file__).resolve().parents[1] / "docs" / "benchmarks"
    )
    for report_path in report_paths:
        print(report_path)
    return report_paths


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profiles",
        nargs="+",
        default=["local-3b", "hosted"],
        help="Profile names from llm/model_profiles.json.",
    )
    args = parser.parse_args()
    run_comparison(args.profiles)


if __name__ == "__main__":
    main()
