import json

import requests
from pyspark.sql import SparkSession
import os


os.environ["PYSPARK_PYTHON"] = r"C:\Users\USER\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\USER\AppData\Local\Programs\Python\Python311\python.exe"
spark = (SparkSession.builder
          .appName("TransformData")
          .config("spark.python.worker.faulthandler.enabled", "true")
          .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
          .getOrCreate())


url = f"https://api.obis.org/v3/occurrence?geometry=-63.898056,17.947574,-62.09112,18.954432&size=500"

resp = requests.get(url)
data = resp.json()

rdd = spark.sparkContext.parallelize([json.dumps(x) for x in data['results']])
df = spark.read.json(rdd)
df.show(10)