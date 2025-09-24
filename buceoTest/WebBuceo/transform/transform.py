from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os
import json

class TransformData:

    def __init__(self):
        os.environ["PYSPARK_PYTHON"] = r"C:\Users\USER\AppData\Local\Programs\Python\Python311\python.exe"
        os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\USER\AppData\Local\Programs\Python\Python311\python.exe"
        self.spark = (SparkSession.builder
                      .appName("TransformData")
                      .config("spark.python.worker.faulthandler.enabled", "true")
                      .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
                      .getOrCreate())
        self.schema = StructType([
            StructField("MRGID", LongType(), True),
            StructField("gazetteerSource", StringType(), True),
            StructField("placeType", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("minLatitude", DoubleType(), True),
            StructField("minLongitude", DoubleType(), True),
            StructField("maxLatitude", DoubleType(), True),
            StructField("maxLongitude", DoubleType(), True),
            StructField("precision", StringType(), True),  # o DoubleType() si puede ser numérico
            StructField("preferredGazetteerName", StringType(), True),
            StructField("preferredGazetteerNameLang", StringType(), True),
            StructField("status", StringType(), True),
            StructField("accepted", LongType(), True),
        ])

    def convert_numbers_to_float(self, data):
        keys_to_float = ["latitude", "longitude", "minLatitude", "minLongitude", "maxLatitude", "maxLongitude"]
        for sublist in data:
            for item in sublist:
                for key in keys_to_float:
                    if key in item and item[key] is not None:
                        item[key] = float(item[key])
        return data

    def transformData(self, data):
        data2 = self.convert_numbers_to_float(data)
        flat_data = [item for sublist in data2 for item in sublist]
        dfAll = self.spark.createDataFrame(
            flat_data,
            schema=self.schema
        )
        df = self.cleanData(dfAll)
        return df

    def cleanData(self, df):
        dfTransform = df.filter(df.preferredGazetteerName.contains("Colombian") & df.gazetteerSource.isNotNull())
        return dfTransform