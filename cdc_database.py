import pandas as pd
import sqlite3
import os

# 22 tables in database

def insert_tables(path):
    db_path = "/Users/jackbecker/xforce/CDC_Webscraping/tox_database.db"
    conn = sqlite3.connect(db_path)
    if os.path.isdir(path): 
        for filename in os.listdir(path):
            if filename.endswith(".csv"):
                df = pd.read_csv(os.path.join(path, filename))
                df.to_sql(name = os.path.splitext(filename)[0], con = conn, if_exists = 'replace')
    else: 
        if path.endswith('.csv'):
            df = pd.read_csv(path)
            df.to_sql(name = os.path.splitext(os.path.basename(path))[0], con = conn, if_exists = 'replace')
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type = 'table';")
    tables = cur.fetchall()
    for table_name in tables:
        table_name = table_name[0]
        print(f"Table name: {table_name}")
        cur.execute(f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT 5;")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print('\n')
    conn.close()

def update_database():
    db_path = "/Users/jackbecker/xforce/CDC_Webscraping/tox_database.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    insert_tables('/Users/jackbecker/xforce/TRV_Lists_initial/Auxiliary_dataframes/TRV/')
    insert_tables('/Users/jackbecker/xforce/TRV_Lists_initial/Auxiliary_dataframes/')
    insert_tables("/Users/jackbecker/xforce/CDC_Webscraping/clean_output.csv")
    conn.close()

def count_tables():
    db_path = "/Users/jackbecker/xforce/CDC_Webscraping/tox_database.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table';")
    count = cur.fetchone()[0]
    conn.close()
    return count

def print_tables():
    db_path = "/Users/jackbecker/xforce/CDC_Webscraping/tox_database.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    table_names = set()
    cur.execute("SELECT name FROM sqlite_master WHERE type = 'table';")
    tables = cur.fetchall()
    for table_name in tables:
        table_names.add(table_name[0])
    for name in table_names:
        print(f"Table name: {name}")
        cur.execute(f"SELECT * FROM {name} ORDER BY RANDOM() LIMIT 3;")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print('\n')
    conn.close()
