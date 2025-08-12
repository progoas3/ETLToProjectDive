from extract.ExtractGeo import ExtractGeo
from transform.transform import *

if __name__ == "__main__":
    #Extract Data API
    extractor = ExtractGeo()
    resultados = extractor.extract()

    #Transform Data
    transformer = TransformData()
    df = transformer.transformData(resultados)
    df.show()
