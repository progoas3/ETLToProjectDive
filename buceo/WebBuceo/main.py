from extract.ExtractGeo import ExtractGeo
from transform.transform import *

if __name__ == "__main__":
    extractor = ExtractGeo()
    resultados = extractor.extract()

    transformer = TransformData()
    df = transformer.transformData(resultados)
    dfFilter = df.filter(df.preferredGazetteerName.contains("Colombian"))
    dfFilter.show()
