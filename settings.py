import os

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

BASE_DIR = os.path.dirname(os.path.dirname(__file__))