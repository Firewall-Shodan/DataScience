import csv
from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017/")

mydb = client["Covid19"]
mycol = mydb["covid19"]

with open('dados/cod_postal.csv', 'r', encoding='ISO-8859-1') as csv_file:
    data = csv.reader(csv_file, delimiter=';')
    for r in data:
        d = {
            "CODE_POSTAL": int(r[0]),
            "LOCALIDADE": r[1]
        }
        mycol.insert_one(d)

