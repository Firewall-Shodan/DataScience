import cx_Oracle
from neo4j import GraphDatabase


# Dados de Conexão á Oracle database
oracle_username = "store"
oracle_password = "marcos2020"
oracle_host = "localhost"
oracle_port = 1521
oracle_service_name = "xe"


# Dados para Conexão á base de Dados neo4j

neo4j_uri = "bolt://localhost:7687"
neo4j_username = "tpratico"
neo4j_password = "marcos1234"

# Categorias Correspondente

table_labels = {
    "store_users": "User",
    "product_categories": "ProductCategory",
    "discount": "Discount",
    "cart_item": "CartItem",
    "shopping_session": "ShoppingSession",
    "order_details": "OrderDetails",
    "order_items": "OrderItem",
    "payment_details": "PaymentDetails",
    "employees": "Employee",
    "departments": "Department",
    "addresses": "Address",
    "stock": "Stock",
    "product": "Product"
}

# Relaçãoes Neo4j

relationships = [
    ("store_users", "shopping_session", "login"),
    ("shopping_session", "cart_item", "carrinho"),
    ("store_users", "order_details", "dados"),
    ("cart_item", "product", "Encomenda"),
    ("product", "discount", "desconto"),
    ("product", "order_items", "escolher"),
    ("order_items", "order_details", "mais_dados"),
    ("order_details", "payment_details", "confirmar_dados"),
    ("order_details", "addresses", "endereco"),
    ("product", "product_categories", "Categorias"),
    ("product", "stock", "tem_stock"),
    ("store_users", "payment_details", "Pagamento"),
    ("employees", "departments", "trabalham_em"),

]


def fetch_data_from_oracle():
    oracle_dsn = cx_Oracle.makedsn(oracle_host, oracle_port, service_name=oracle_service_name)
    oracle_conn = cx_Oracle.connect(oracle_username, oracle_password, dsn=oracle_dsn)

    # Fetch dos dados
    data = {}
    for i, label in table_labels.items():
        cursor = oracle_conn.cursor()
        query = f"SELECT * FROM {i}"
        cursor.execute(query)
        data[label] = cursor.fetchall()
        cursor.close()

    # Close Oracle database connection
    oracle_conn.close()

    return data


def import_data_to_neo4j(data):

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_username, neo4j_password))
    with driver.session() as session:
        for label, records in data.items():
            # Criar os Nodes
            for record in records:
                properties = {}
                for i, value in enumerate(record):
                    properties[f"prop_{i}"] = value
                session.run(f"CREATE (:{label} $properties)", properties=properties)

            # Criar as Relações
            for relationship in relationships:
                if relationship[0] == label:
                    source_label = relationship[0]
                    target_label = relationship[1]
                    relation_type = relationship[2]
                    session.run(f"MATCH (a:{source_label}), (b:{target_label}) WHERE a.prop_0 = b.prop_0 CREATE (a)-[:{relation_type}]->(b)")

    # Fechar a Conexaão Neo4j database
    driver.close()

# Fetch data from Oracle database
data = fetch_data_from_oracle()

# Import data into Neo4j database
import_data_to_neo4j(data)
