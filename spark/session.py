"""Shared Spark session configuration for local development."""

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """Create or retrieve the project Spark session."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
