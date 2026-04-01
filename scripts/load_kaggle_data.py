import os
import yaml
from dotenv import load_dotenv
import argparse
from pyspark.sql import SparkSession

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(project_path, "config.yaml")
with open(config_path, mode="r", encoding="UTF-8") as rfile:
    config = yaml.safe_load(rfile)

load_dotenv()
import kaggle


def argument_parser() -> bool:
    parser = argparse.ArgumentParser()
    parser.add_argument("--append", help="Append mode", action="store_true")
    args = parser.parse_args()
    return args.append


def load_kaggle_dataset() -> None:
    """Download PaySim Kaggle dataset to a separate data directory
     and change the name of the downloaded dataset to a more aesthetically pleasing name"""
    api = kaggle.api.__class__()
    api.authenticate()

    project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(project_path, "data")
    if not os.path.exists(data_path):
        os.makedirs(data_path)

    api.dataset_download_files(config["kaggle"]["dataset"], path=data_path, unzip=True)
    downloaded_files = os.listdir(path=data_path)
    for file in downloaded_files:
        if file.endswith(".csv"):
            old_path = os.path.join(data_path, file)
            new_path = os.path.join(data_path, f"{config["db"]["table"]}.csv")
            os.replace(old_path, new_path)


def dump_to_postgresql(actual_mode: bool) -> None:
    """Use PySpark to write the downloaded PaySim dataset to a PostgreSQL database
            Arguments:
            1. actual_mode: True or False. If set to True then PySpark simply appends new data
            otherwise if set to False, overwrites existing data"""
    project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    jars_path = os.path.join(project_path, "jars")
    data_path = os.path.join(project_path, "data")
    spark = (SparkSession.builder
             .appName(config["spark"]["app_name"])
             .config("spark.jars", os.path.join(jars_path, config["spark"]["jar_name"]))
             .getOrCreate())
    host = config["db"]["host"]
    port = config["db"]["port"]
    db_name = config["db"]["name"]
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db_name}"

    try:
        df = (spark.read.
              csv(os.path.join(data_path, f"{config["db"]["table"]}.csv"), header=True, inferSchema=True).
              limit(config["spark"]["limit_rows"]))
        write_mode = "append" if actual_mode else "overwrite"
        (df.write.
         format("jdbc").
         option("url", jdbc_url).
         option("dbtable", config["db"]["table"]).
         option("user", config["db"]["user"]).
         option("password", os.getenv("DB_PASSWORD")).
         option("driver", "org.postgresql.Driver").
         mode(write_mode).
         save())
    except Exception:
        print("Connection to PostgreSQL failed")
    finally:
        spark.stop()


if __name__ == "__main__":
    is_append_mode = argument_parser()
    load_kaggle_dataset()
    dump_to_postgresql(is_append_mode)
