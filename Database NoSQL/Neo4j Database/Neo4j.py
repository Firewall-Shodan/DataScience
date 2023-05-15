from neo4j import GraphDatabase

driver = GraphDatabase.driver('bolt://localhost:7687', auth=('marcos', 'marcos1234'))

with open('dados/doentes.csv', 'r', encoding='ISO-8859-1') as f:
    next(f)
    for line in f:
        fields = line.strip().split(';')
        cod_postal = fields[0]
        data_nascimento = fields[1]
        genero = fields[2]
        id_paciente = fields[3]
        nome = fields[4]

        query = (
            'CREATE (:REGISTO_COVID19 '
            '{ cod_postal: $cod_postal,'
            '  data_nascimento: $data_nascimento,'
            '  genero: $genero,'
            '  id_paciente: $id_paciente,'
            '  nome: $nome'
            '})'
        )

        with driver.session() as session:
            session.run(
                query,
                cod_postal=cod_postal,
                data_nascimento=data_nascimento,
                genero=genero,
                id_paciente=id_paciente,
                nome=nome
            )
    driver.close()




