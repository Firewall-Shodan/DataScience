import csv
import cx_Oracle


connection = cx_Oracle.connect('marcos/marcos2020@localhost:1521/xe')

with open('dados/doentes2.csv',  encoding='ISO-8859-1') as csv_file:
    data = csv.reader(csv_file, delimiter=';')
    next(data)
    for r in data:
        n1, n2, n3, n4, n5, n6 = r
        cursor = connection.cursor()
        cursor.execute("INSERT INTO DOENTES(CODE_POSTAL, DATA_NASCIMENTO, GENERO, ID_PACIENTE, NOME, ID_POSTAL) VALUES(:1, :2, :3, :4, :5)", (n1, n2, n3, n4, n5, n6))
        cursor.close()

connection.commit()
connection.close()

