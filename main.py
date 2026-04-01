from src.etl_pipeline import PaySimPipeline
import yaml
import os

project_path = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(project_path, "config.yaml")
with open(config_path, mode="r", encoding="UTF-8") as rfile:
    config = yaml.safe_load(rfile)

os.environ['SPARK_LOCAL_IP'] = os.getenv('SPARK_LOCAL_IP', config["spark"]["local_ip"])

if __name__ == "__main__":
    paySimPipeline = PaySimPipeline(config)
    paySimPipeline.run(False)
