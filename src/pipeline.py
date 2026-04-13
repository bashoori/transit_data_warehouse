from bronze.ingest_gtfs import run_bronze_ingestion
from silver.transform_gtfs import run_silver_transformation
from gold.build_warehouse import run_gold_transformation
from common.logger import get_logger

logger = get_logger(__name__)


def main() -> None:
    logger.info('Starting TransLink GTFS medallion pipeline')
    run_bronze_ingestion()
    run_silver_transformation()
    run_gold_transformation()
    logger.info('GTFS warehouse pipeline completed successfully')


if __name__ == '__main__':
    main()
