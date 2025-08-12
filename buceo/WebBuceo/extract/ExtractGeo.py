import json
import requests

class ExtractGeo:
    def __init__(self):
        self.endpoint = "https://marineregions.org/rest/getGazetteerRecordsByName.json/Caribbean/like"
        self.resultados = []
        self.response = requests.get(self.endpoint)

    def extract(self):
        if self.response.status_code == 200:
            data = self.response.json()
            nombres = [item["preferredGazetteerName"] for item in data]
            diccionario = {"nombres": nombres}
            for x in range(len(diccionario["nombres"])):
                name = diccionario["nombres"][x]
                url = f"https://marineregions.org/rest/getGazetteerRecordsByName.json/{name}/"""
                response2 = requests.get(url)
                self.resultados.append(response2.json())
            return self.resultados
            print("Finalizado con exito.")
        else:
            print("Error: ",self.response.status_code)