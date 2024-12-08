from extract_and_save_data import connect_mongo, create_db, create_collection
import pandas as pd
import os
from dotenv import load_dotenv

def visualize_collection(collection):
    for doc in collection.find():
        print(doc)

def rename_column(collection, col_name, new_name):
    collection.update_many({}, {'$rename': {f'{col_name}': f'{new_name}'}})

def select_category(collection, category):
    query = { 'Categoria do Produto': f'{category}'}

    list_category = []
    for doc in collection.find(query):
        list_category.append(doc)
    return list_category

def make_regex(collection, regex):
    query = {'Data da Compra': {'$regex': f'{regex}'}}

    list_regex = []
    for doc in collection.find(query):
        list_regex.append(doc)
    return list_regex

def create_dataframe(list):
    data =  pd.DataFrame(list)
    return data

def format_date(data):
    data['Data da Compra'] = pd.to_datetime(data['Data da Compra'], format='%d/%m/%Y')
    data['Data da Compra'] = data['Data da Compra'].dt.strftime('%Y-%m-%d')

def save_csv(data, path):
    data.to_csv(path, index=False)
    print('Arquivo Gerado com Sucesso!')

if __name__ == '__main__':
    # Load File .env
    load_dotenv('../.env')

    uri = os.getenv('MONGODB_URI')

    # Realizando conexao
    client = connect_mongo(uri)

    # Criando Base de Dados
    database = create_db(client, 'db_desafio')
    collection = create_collection(client, 'col_desafio')

    # Renomeando Colunas Latitude e longitude
    rename_column(collection, 'lat', 'Latitude')
    rename_column(collection, 'lon', 'Longitude')

    # Salvando Dados Categoria Livros
    list_books = select_category(collection, 'livros')
    df_books = create_dataframe(list_books)

    # Formatando Data
    format_date(df_books)

    # Salvando .csv
    save_csv(df_books, 'data/books_desafio.csv')

    # Salvando Dados Produtos Vendidos > 2021
    list_products = make_regex(collection, '/202[1-9]')
    df_products = create_dataframe(list_products)

    # Formatando Data
    format_date(df_products)

    # Salvando .csv
    save_csv(df_products, 'data/products_desafio.csv')
