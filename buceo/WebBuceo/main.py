from WebBuceo.load.loadData import loadDataParquet
from extract.ExtractGeo import ExtractGeo
from transform.transform import *
from load.loadData import *

if __name__ == "__main__":
    #Extract Data API
    extractor = ExtractGeo()
    resultados = extractor.extract()

    #Transform Data
    transformer = TransformData()
    df = transformer.transformData(resultados)

    #Load Data
    load = loadDataParquet()
    load.loadData(df)
