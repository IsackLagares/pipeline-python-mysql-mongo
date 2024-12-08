import os
from dotenv import load_dotenv
import mysql.connector
import pandas as pd

def connect_mysql(host,user,password):
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password
    )
    print('Conexão realizada com sucesso!')
    return connection


def create_cursor(connection):
    return connection.cursor()

def create_database(cursor,db_name):
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name};')
    print('Base de Dados criada com sucesso!')
    return db_name

def create_table(cursor,db_name,tb_name):
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {db_name}.{tb_name}(
               id VARCHAR(200),
               Product VARCHAR(100),
               Prdocut_Category VARCHAR(100),
               Price FLOAT(10,2),
               Freight FLOAT(10,2),
               Date_Purchase DATE,
               Seller VARCHAR(100),
               Local_Purchase VARCHAR(100),
               Purchase_Evaluation INT,
               Payment_Type VARCHAR(100),
               Number_Installments INT,
               Latitude FLOAT(10,2),
               Longitude FLOAT(10,2),

               PRIMARY KEY (id)
               );
    ''')

    print('Tabela criada com sucesso!')
    return tb_name

def read_csv(path):
    data = pd.read_csv(path)
    return data

def insert_data(connection,cursor,data,db_name,tb_name):
    data_list = [tuple(row) for i, row in data.iterrows()]
    sql = f'INSERT INTO {db_name}.{tb_name} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    cursor.executemany(sql,data_list)
    connection.commit()
    print(f'Foram incluídos {cursor.rowcount} dados!')


if __name__ == '__main__':
    # Load File .env
    load_dotenv('../.env')

    host = os.getenv("DB_HOST") 
    user = os.getenv("DB_USERNAME") 
    password = os.getenv("DB_PASSWORD") 

    # Realizando conexao
    connection = connect_mysql(host,user,password)
    cursor = create_cursor(connection)

    # Criando Base de Dados
    database = create_database(cursor,'db_desafio')
    table = create_table(cursor,database,'tb_desafio')

    # Leitura e Inclusao dos Dados
    data = read_csv('data/books_table.csv')
    insert_data(connection,cursor,data,database,table)

    cursor.close()
    connection.close()