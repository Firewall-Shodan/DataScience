import cx_Oracle
from pymongo import MongoClient

# Conectar a  Oracle database
connect = cx_Oracle.connect('store/marcos2020@localhost:1521/xe')

# Query e fetch dos dados
oracle_cursor = connect.cursor()
oracle_cursor.execute('SELECT p.product_id, p.product_name, c.category_id, c.category_name, p.sku, p.price, p.discount_id, p.created_at, p.last_modified, s.quantity, s.max_stock_quantity, s.unit FROM product p JOIN stock s ON p.product_id = s.product_id JOIN product_categories c ON p.category_id = c.category_id')
oracle_data = oracle_cursor.fetchall()

# Transformar em dicionário para o MongoDB
products = []
for row in oracle_data:
    product = {
        'product_id': row[0],
        'product_name': row[1],
        'category': {
            'category_id': row[2],
            'category_name': row[3]
        },
        'sku': row[4],
        'price': row[5],
        'discount_id': row[6],
        'created_at': row[7],
        'last_modified': row[8],
        'stock': {
            'quantity': row[9],
            'max_stock_quantity': row[10],
            'unit': row[11]
        }

    }
    products.append(product)

# fechar a conexão
oracle_cursor.close()
connect.close()


# Conexão ao  MongoDB Atlas
client = MongoClient('')

# Acessar a nossa database
db = client['Tpratico']


# Inserir dados no MongoDB
product_collection = db['produtos']
product_collection.insert_many(products)

# fechar a conexão
client.close()
