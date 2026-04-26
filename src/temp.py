import sys
import argparse
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim


def get_spark_session(app_name: str = "PySparkETLApp", checkpoint_dir: str = "./spark_checkpoints") -> SparkSession:
    """Initialize and return a SparkSession with checkpointing enabled."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    return spark


def extract(spark: SparkSession, source_path: str, source_format: str = "csv", **options) -> DataFrame:
    logging.info(f"Extracting data from {source_path} as {source_format}")
    df = spark.read.format(source_format).options(**options).load(source_path)
    logging.info(f"Extracted {df.count()} records.")
    return df


def transform(df: DataFrame) -> DataFrame:
    logging.info("Starting data transformation...")
    # Example: trim all string columns and filter out null IDs
    string_cols = [field.name for field in df.schema.fields if str(field.dataType) == 'StringType']
    for col_name in string_cols:
        df = df.withColumn(col_name, trim(col(col_name)))
    if 'id' in df.columns:
        df = df.filter(col('id').isNotNull())
    logging.info("Transformation complete. Checkpointing DataFrame...")
    df = df.checkpoint(eager=True)
    logging.info("Checkpoint complete.")
    return df


def load(df: DataFrame, target_path: str, target_format: str = "parquet", mode: str = "overwrite", **options):
    logging.info(f"Loading data to {target_path} as {target_format} (mode={mode})")
    df.write.format(target_format).mode(mode).options(**options).save(target_path)
    logging.info("Data load complete.")


def parse_args():
    parser = argparse.ArgumentParser(description="PySpark ETL Pipeline with Checkpointing")
    parser.add_argument('--source', required=True, help='Source file or directory path')
    parser.add_argument('--source-format', default='csv', help='Source data format (csv, parquet, json, etc)')
    parser.add_argument('--target', required=True, help='Target file or directory path')
    parser.add_argument('--target-format', default='parquet', help='Target data format (parquet, csv, json, etc)')
    parser.add_argument('--checkpoint-dir', default='./spark_checkpoints', help='Spark checkpoint directory')
    parser.add_argument('--log-level', default='INFO', help='Logging level')
    parser.add_argument('--delimiter', default=',', help='CSV delimiter (if applicable)')
    parser.add_argument('--header', default='true', help='CSV header (if applicable)')
    return parser.parse_args()


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )


def main():
    args = parse_args()
    setup_logging(args.log_level)
    spark = get_spark_session(checkpoint_dir=args.checkpoint_dir)
    try:
        extract_options = {}
        if args.source_format == 'csv':
            extract_options = {'header': args.header, 'inferSchema': 'true', 'delimiter': args.delimiter}
        df = extract(spark, args.source, args.source_format, **extract_options)
        df_transformed = transform(df)
        load_options = {}
        if args.target_format == 'csv':
            load_options = {'header': 'true', 'delimiter': args.delimiter}
        load(df_transformed, args.target, args.target_format, **load_options)
        logging.info("ETL pipeline with checkpointing completed successfully.")
    except Exception as e:
        logging.exception(f"ETL pipeline failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()