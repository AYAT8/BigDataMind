"""Gold-layer catalog metadata for BigDataMind.

This module intentionally has no Spark imports so it can be used by docs, SQL
validation allowlists, and future tooling without booting a Spark session.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class GoldColumn:
    """A single column in one Gold table."""

    name: str
    spark_type: str
    description: str


@dataclass(frozen=True)
class GoldTable:
    """Catalog metadata describing one Gold aggregation table."""

    name: str
    grain: str
    purpose: str
    columns: tuple[GoldColumn, ...]
    example_questions_en: tuple[str, ...]
    example_questions_fr: tuple[str, ...]
    caveats: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        """Serialize the definition for docs and JSON."""
        return {
            "name": self.name,
            "grain": self.grain,
            "purpose": self.purpose,
            "columns": [asdict(column) for column in self.columns],
            "example_questions_en": list(self.example_questions_en),
            "example_questions_fr": list(self.example_questions_fr),
            "caveats": list(self.caveats),
        }


GOLD_TABLES: tuple[GoldTable, ...] = (
    GoldTable(
        name="gold_hourly_demand",
        grain="one row per pickup_date and pickup_hour",
        purpose="Hourly trip volume and revenue demand by time of day.",
        columns=(
            GoldColumn("pickup_date", "date", "Calendar date for the pickup."),
            GoldColumn("pickup_hour", "int", "Hour of the day in 24-hour format (0-23)."),
            GoldColumn("trips", "bigint", "Number of valid Silver trips in the slot."),
            GoldColumn("total_revenue", "double", "Sum of total_amount for the slot."),
            GoldColumn("avg_fare", "double", "Average fare_amount for trips in the slot."),
            GoldColumn(
                "avg_distance_miles",
                "double",
                "Average trip_distance in miles for trips in the slot.",
            ),
            GoldColumn(
                "avg_duration_min",
                "double",
                "Average trip_duration_min for trips in the slot.",
            ),
        ),
        example_questions_en=(
            "Which hour had the highest demand on 2024-01-15?",
            "What was the total revenue for pickup hour 18 in January 2024?",
            "How did average fare and trip length vary across morning and evening demand peaks?",
        ),
        example_questions_fr=(
            "Quelle heure a connu la demande la plus élevée le 2024-01-15 ?",
            "Quel a été le revenu total pour l'heure de prise en charge 18 en janvier 2024 ?",
            "Comment le tarif moyen et la durée du trajet ont-ils varié entre les pics du matin et du soir ?",
        ),
        caveats=(
            "Rejected rows are excluded because Gold is rebuilt from the valid Silver layer.",
            "Revenue and averages ignore nulls, and only valid Silver rows are counted.",
            "Cash tips are not recorded in total_amount per the source dictionary.",
        ),
    ),
    GoldTable(
        name="gold_daily_demand",
        grain="one row per pickup_date",
        purpose="Daily demand and revenue, with a weekday label aligned to the Silver pickup_dow encoding.",
        columns=(
            GoldColumn("pickup_date", "date", "Calendar date for the pickup."),
            GoldColumn(
                "day_of_week",
                "int",
                "Spark day-of-week encoding from Silver pickup_dow: 1 = Sunday, 7 = Saturday.",
            ),
            GoldColumn(
                "day_name",
                "string",
                "Human-readable weekday name mapped from the Silver pickup_dow value.",
            ),
            GoldColumn("trips", "bigint", "Total valid trips on the date."),
            GoldColumn("total_revenue", "double", "Sum of total_amount for the date."),
            GoldColumn("avg_fare", "double", "Average fare_amount for the date."),
            GoldColumn(
                "avg_distance_miles",
                "double",
                "Average trip_distance in miles for the date.",
            ),
        ),
        example_questions_en=(
            "Which day in January had the most trips?",
            "What was the weekday pattern for total revenue across the month?",
            "How many trips occurred on Sundays compared with Thursdays?",
        ),
        example_questions_fr=(
            "Quel jour de janvier a enregistré le plus de courses ?",
            "Quel a été le profil hebdomadaire du revenu total au cours du mois ?",
            "Combien de courses ont eu lieu les dimanches par rapport aux jeudis ?",
        ),
        caveats=(
            "Rejected rows are excluded before aggregation.",
            "day_of_week uses the same encoding as Silver pickup_dow and is not reinterpreted as Monday-first.",
            "Flex Fare rows can be kept with null passenger counts and other missing fields when they otherwise pass Silver validation.",
        ),
    ),
    GoldTable(
        name="gold_revenue_by_zone",
        grain="one row per pickup zone",
        purpose="Trip counts and revenue grouped by pickup taxi zone and borough.",
        columns=(
            GoldColumn("pu_location_id", "int", "Pickup taxi-zone ID from Silver."),
            GoldColumn(
                "zone",
                "string",
                "Pickup zone name from the TLC lookup, including Unknown.",
            ),
            GoldColumn(
                "borough",
                "string",
                "Pickup borough name from the TLC lookup, including Unknown.",
            ),
            GoldColumn("trips", "bigint", "Total valid trips for the pickup zone."),
            GoldColumn("total_revenue", "double", "Sum of total_amount for the pickup zone."),
            GoldColumn("avg_fare", "double", "Average fare_amount for the pickup zone."),
        ),
        example_questions_en=(
            "Which zone generated the most revenue in January?",
            "What is the average fare by borough?",
            "Which taxi zones still appear as Unknown in the pickup lookup?",
        ),
        example_questions_fr=(
            "Quelle zone a généré le plus de revenus en janvier ?",
            "Quel est le tarif moyen par arrondissement ?",
            "Quelles zones de taxi apparaissent encore comme Inconnu dans la recherche de prise en charge ?",
        ),
        caveats=(
            "Unmatched pickup zones stay in Gold as Unknown to preserve the full valid trip volume.",
            "This table aggregates by pickup zone only; drop-off zone is not used here.",
            "Cash tips are not included because total_amount is the reported prepaid revenue total.",
        ),
    ),
    GoldTable(
        name="gold_peak_hours",
        grain="one row per day_of_week and pickup_hour",
        purpose="Busiest weekday-hour slots ranked by average trips per operating day.",
        columns=(
            GoldColumn("day_of_week", "int", "Spark day-of-week value in the same encoding as Silver pickup_dow."),
            GoldColumn("day_name", "string", "Human-readable day label associated with the numeric weekday."),
            GoldColumn("pickup_hour", "int", "Hour of day for the slot."),
            GoldColumn("trips", "bigint", "Trips in that weekday-hour slot across the month."),
            GoldColumn(
                "avg_trips_per_day",
                "double",
                "Trips in the slot divided by the distinct pickup dates that fall on that weekday.",
            ),
            GoldColumn("avg_fare", "double", "Average fare_amount for the weekday-hour slot."),
            GoldColumn(
                "peak_rank",
                "int",
                "Dense rank of weekday-hour slots by avg_trips_per_day descending, with 1 being busiest.",
            ),
        ),
        example_questions_en=(
            "What were the busiest weekday-hour slots by average daily demand?",
            "Which hour on Friday ranked highest after adjusting for the number of Fridays in the month?",
            "How did weekday evening demand compare with weekday midday demand?",
        ),
        example_questions_fr=(
            "Quels étaient les créneaux les plus chargés par moyenne quotidienne de demande ?",
            "Quel créneau du vendredi a obtenu le meilleur classement après ajustement au nombre de vendredis du mois ?",
            "Comment la demande du soir en semaine se compare-t-elle à celle du milieu de journée ?",
        ),
        caveats=(
            "peak_rank is a dense rank based on average trips per day, not raw trip counts.",
            "Slots are compared across all weekday-hour combinations; the denominator is the number of distinct pickup dates on that weekday in Silver.",
            "Unlike raw counts, this measure normalizes for uneven weekday frequency within the month.",
        ),
    ),
    GoldTable(
        name="gold_payment_mix",
        grain="one row per payment type",
        purpose="Payment usage mix across valid Silver trips, including revenue share and total revenue.",
        columns=(
            GoldColumn("payment_type_id", "int", "Payment type code from Silver payment_type."),
            GoldColumn("payment_type_name", "string", "Human-readable label from the TLC dictionary."),
            GoldColumn("trips", "bigint", "Total valid trips for a payment type."),
            GoldColumn("total_revenue", "double", "Sum of total_amount for the payment type."),
            GoldColumn(
                "share_of_trips_pct",
                "double",
                "Trips share of the total valid trip volume for the month, expressed as a percentage.",
            ),
        ),
        example_questions_en=(
            "What share of trips were paid by credit card in January?",
            "Which payment type had the highest total revenue?",
            "How does the payment mix compare between cash and Flex Fare?",
        ),
        example_questions_fr=(
            "Quelle part des courses a été payée par carte bancaire en janvier ?",
            "Quel mode de paiement a généré le revenu total le plus élevé ?",
            "Comment le mix de paiement se compare-t-il entre cash et Flex Fare ?",
        ),
        caveats=(
            "Rejected rows are excluded from the mix, so the percentages represent valid Silver trips only.",
            "Cash tips are not reported separately in the source; only reported total_amount is aggregated.",
            "Flex Fare rows remain eligible even when passenger_count or other fields are null, as long as all Silver validation rules pass.",
        ),
    ),
    GoldTable(
        name="gold_distance_duration_stats",
        grain="one row per distance bucket",
        purpose="Distance-band summary for trip duration, fare and speed.",
        columns=(
            GoldColumn("distance_bucket", "string", "Bucket label for the trip-distance range."),
            GoldColumn("bucket_order", "int", "Sorting order for the bucket labels, from 1 to 6."),
            GoldColumn("trips", "bigint", "Trips in the distance bucket."),
            GoldColumn(
                "avg_duration_min",
                "double",
                "Average trip_duration_min for trips in the bucket.",
            ),
            GoldColumn("avg_fare", "double", "Average fare_amount for trips in the bucket."),
            GoldColumn(
                "avg_speed_mph",
                "double",
                "Average avg_speed_mph for trips in the bucket; null durations are excluded.",
            ),
        ),
        example_questions_en=(
            "Which distance band has the longest average trip duration?",
            "How do average fare and speed change across the 0-1, 1-3, and 3-5 mile buckets?",
            "What is the trip volume in the 10-20 mile band?",
        ),
        example_questions_fr=(
            "Quelle bande de distance a la durée moyenne de trajet la plus longue ?",
            "Comment le tarif moyen et la vitesse évoluent-ils entre les intervalles 0-1, 1-3 et 3-5 miles ?",
            "Quel est le volume de trajets dans la bande de distance 10-20 miles ?",
        ),
        caveats=(
            "Buckets are defined on trip_distance with lower-bound inclusion and upper-bound exclusion: 0-1, 1-3, 3-5, 5-10, 10-20, and 20+.",
            "Exact boundaries such as 1.0, 3.0, 5.0, 10.0 and 20.0 are assigned to the intended bucket per the defined rules.",
            "Distance, duration, speed and shares are rounded only in the final projected Gold select.",
        ),
    ),
)

TABLES_BY_NAME = {table.name: table for table in GOLD_TABLES}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def write_catalog_json(output_path: Path | None = None) -> Path:
    """Write the machine-readable gold catalog JSON and return its path."""
    root = _project_root()
    target = output_path or root / "docs" / "gold_catalog.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {"tables": [table.as_dict() for table in GOLD_TABLES]}
    target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return target


def write_data_dictionary(output_path: Path | None = None) -> Path:
    """Write a human-readable Gold data dictionary for RAG and documentation."""
    root = _project_root()
    target = output_path or root / "docs" / "data_dictionary.md"
    target.parent.mkdir(parents=True, exist_ok=True)

    sections: list[str] = [
        "# Gold data dictionary",
        "",
        "This document is generated from the catalog defined in `spark/gold_catalog.py`.",
        "",
        "## Shared rules",
        "",
        "- Gold is always rebuilt from the valid Silver Delta table `data/silver/yellow_trips`.",
        "- Day-of-week encoding matches Silver `pickup_dow`, where 1 = Sunday and 7 = Saturday.",
        "- Revenue uses `SUM(total_amount)`; cash tips are not recorded in source totals.",
        "- Rejected rows are excluded from all Gold aggregates.",
        "",
    ]

    for table in GOLD_TABLES:
        sections.extend(
            [
                f"## {table.name}",
                "",
                f"- Grain: {table.grain}",
                f"- Purpose: {table.purpose}",
                "",
                "### Columns",
                "",
                "| Column | Type | Description |",
                "|---|---|---|",
            ]
        )
        for column in table.columns:
            sections.append(
                f"| `{column.name}` | `{column.spark_type}` | {column.description} |"
            )
        sections.extend(["", "### Example questions (English)", ""])
        for question in table.example_questions_en:
            sections.append(f"- {question}")
        sections.extend(["", "### Example questions (Français)", ""])
        for question in table.example_questions_fr:
            sections.append(f"- {question}")
        sections.extend(["", "### Caveats", ""])
        for caveat in table.caveats:
            sections.append(f"- {caveat}")
        sections.append("")

    target.write_text("\n".join(sections) + "\n", encoding="utf-8")
    return target


def main() -> None:
    """CLI entry point for documenting the Gold catalog."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write-docs",
        action="store_true",
        help="Write docs/data_dictionary.md and docs/gold_catalog.json.",
    )
    args = parser.parse_args()

    if args.write_docs:
        write_data_dictionary()
        write_catalog_json()
        print("Wrote docs/data_dictionary.md and docs/gold_catalog.json")
        return

    print(json.dumps({"tables": [table.name for table in GOLD_TABLES]}, indent=2))


if __name__ == "__main__":
    main()
