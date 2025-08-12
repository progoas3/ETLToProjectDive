from pyspark.sql import *
from pyspark.sql.functions import *

class loadDataParquet:
    def __init__(self):
        pass


    def loadData(self, df):
        df.write.mode("overwrite").parquet("buceo/WebBuceo/data/tmp/marineregions.parquet")