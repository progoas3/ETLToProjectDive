from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\USER\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\USER\AppData\Local\Programs\Python\Python311\python.exe"
spark = (SparkSession.builder
                      .appName("TransformData")
                      .config("spark.python.worker.faulthandler.enabled", "true")
                      .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
                      .getOrCreate())

df = spark.read.parquet("./data/tmp/marineregions")

df.show(10)