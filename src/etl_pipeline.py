from dotenv import load_dotenv
load_dotenv()
from onetl.connection import Postgres
from onetl.db import DBReader, DBWriter
from onetl.hwm.store import YAMLHWMStore
from onetl.strategy import IncrementalStrategy
from pyspark.sql import SparkSession, DataFrame
from src.transformations import filter_invalid_values, add_calculated_fields, aggregate_transactions
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("ETL-Pipeline")
logging.getLogger("onetl").setLevel(logging.WARNING)
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("pyspark").setLevel(logging.WARNING)


class PaySimPipeline:
    __PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def __init__(self, config: dict):
        self.config = config
        self.spark = self.__create_spark_session()

    def __create_spark_session(self) -> SparkSession:
        jars_path = os.path.join(self.__PROJECT_PATH, "jars")
        postgres_path = os.path.join(jars_path, self.config["spark"]["jar_name"])

        postgres_packages = Postgres.get_packages()

        spark = (SparkSession.builder.appName("PaySim_ETL")
                .config("spark.jars", postgres_path)
                .config("spark.jars.packages", ",".join(postgres_packages))
                .getOrCreate())

        return spark

    def __setup_hwm_storage(self):
        data_path = os.path.join(self.__PROJECT_PATH, "data")
        hwm_path = os.path.join(data_path, "hwm")
        return YAMLHWMStore(path=hwm_path)

    def _extract(self, is_incremental: bool = False) -> DataFrame:
        start_time = time.time()
        logger.info("Data extraction has begun...")

        postrgres = Postgres(
            host=self.config["db"]["host"],
            port=self.config["db"]["port"],
            database=self.config["db"]["name"],
            user=self.config["db"]["user"],
            password=os.getenv("DB_PASSWORD"),
            spark=self.spark)

        hwm = None
        if is_incremental:
            hwm = DBReader.AutoDetectHWM(name=self.config["hwm"]["name"],
                                         expression=self.config["hwm"]["column"],)

        reader = DBReader(connection=postrgres,
                          source=self.config["db"]["table"],
                          hwm=hwm)


        df = reader.run()
        duration = time.time() - start_time
        logger.info(f"{df.count()} lines have been extracted (lazy) in {duration:.2f} seconds")
        return df

    @staticmethod
    def _transform(target_df: DataFrame) -> DataFrame:
        start_time = time.time()
        logger.info("Data transformation has begun...")
        transformed_df = filter_invalid_values(target_df)
        transformed_df = add_calculated_fields(transformed_df)
        transformed_df = aggregate_transactions(transformed_df)

        duration = time.time() - start_time
        logger.info(f"{transformed_df.count()} groups have been created in {duration:.2f} seconds")
        return transformed_df

    def _load(self, target_df: DataFrame, is_incremental: bool = False) -> None:
        start_time = time.time()
        if target_df.count() == 0:
            logger.info("No new data to load. Skipping loading to prevent data corruption")
            return

        logger.info("Data uploading has begun...")
        postgres = Postgres(
            host=self.config["db"]["host"],
            port=self.config["db"]["port"],
            user=self.config["db"]["user"],
            password=os.getenv("DB_PASSWORD"),
            database=self.config["db"]["name"],
            spark=self.spark
        )

        writing_mode = "append" if is_incremental else "replace_entire_table"
        writer = DBWriter(
            connection=postgres,
            table=self.config["db"]["target_table"],
            options={"if_exists": writing_mode}
        )
        writer.run(target_df)
        duration = time.time() - start_time
        logger.info(f"{target_df.count()} lines have been uploaded in {duration:.2f} seconds")

    def run(self, is_incremental: bool = False) -> None:
        if is_incremental:
            hwm_store = self.__setup_hwm_storage()
            with hwm_store, IncrementalStrategy():
                df = self._extract(is_incremental)
                transformed_df = self._transform(df)
                self._load(transformed_df, is_incremental)
        else:
            df = self._extract()
            transformed_df = self._transform(df)
            self._load(transformed_df)
