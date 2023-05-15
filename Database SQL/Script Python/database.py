import csv
import cx_Oracle


connection = cx_Oracle.connect('marcos/marcos2020@localhost:1521/xe')


with open('dados/cod_postal.csv',  encoding='ISO-8859-1') as csv_file:
    data = csv.reader(csv_file, delimiter=';')
    next(data)
    for r in data:
        n, t = r
        cursor = connection.cursor()
        cursor.execute("INSERT INTO Cod_Postal(CODE_POSTAL, LOCALIDADE) VALUES(:1, :2)", (n, t))
        cursor.close()


connection.commit()
connection.close()


