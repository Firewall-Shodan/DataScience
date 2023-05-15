import csv
import cx_Oracle


connection = cx_Oracle.connect('marcos/marcos2020@localhost:1521/xe')

with open('dados/registos_covid19.csv',  encoding='ISO-8859-1', newline='') as csv_file:
    data = csv.reader(csv_file,  delimiter=';')
    for r in data:
        n1, n2, n3, n4, n5, n6, n7, n8, n9, n10 = r

        cursor = connection.cursor()
        cursor.execute("INSERT INTO REGISTO_COVID19(NUMSEQ, DATA_REGISTO, TEMPERATURA, FALTA_AR, DOR_CABECA, DOR_MUSCULAR, TOSSE, DIARREIA,OLFATO_PALADAR, AVALIACAO_GLOBAL) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)", (n1, n2, n3, n4, n5, n6, n7, n8, n9, n10))
        cursor.close()

connection.commit()
connection.close()

