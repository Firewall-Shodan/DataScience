import csv
from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017/")

mydb = client["Covid19"]
mycol = mydb["covid19"]

with open('dados/registo_covid19.csv', 'r', encoding='ISO-8859-1') as csv_file:
    data = csv.reader(csv_file, delimiter=';')
    for r in data:
        d = {
            "NUMSEQ": int(r[0]),
            "DATA_REGISTO": r[1],
            "TEMPERATURA": r[2],
            "FALTA_AR": r[3],
            "DOR_CABEÇA": r[4],
            "DOR_MUSCULAR": r[5],
            "TOSSE": r[6],
            "DIARREIA": r[7],
            "OLFATO_PALADAR": r[8],
            "AVALIACAO_GLOBAL": r[9]
        }
        mycol.insert_one(d)