import os
import duckdb
import gdown
import pandas as pd
import psycopg2
import sqlite3
from sqlalchemy import create_engine
from src.data import download_dataset, load_data
from src.queries import queries, sqlite_queries, pandas_queries
from src.execution import execute_queries, round_results

def main():
    download_dataset()

    df = load_data()
    if 'Airport_fee' in df.columns:
        df = df.drop(columns=['Airport_fee'])

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

    sqlite_conn = sqlite3.connect('data/sqlite_database.db')
    df.to_sql('trips', sqlite_conn, if_exists='replace', index=False)

    duck_conn = duckdb.connect(database='data/duckdb_database.db', read_only=False)
    duck_conn.from_df(df).create_view('trips')

    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )

    cur = conn.cursor()
    sqlite_cur = sqlite_conn.cursor()
    duck_cur = duck_conn.cursor()

    results = execute_queries(conn, cur, queries)
    sqlite_results = execute_queries(sqlite_conn, sqlite_cur, sqlite_queries)
    duck_results = execute_queries(duck_conn, duck_cur, queries)
    pandas_results = execute_queries(None, df, pandas_queries)
    alchemy_results = execute_queries(engine, None, queries)

    results = round_results(results)
    sqlite_results = round_results(sqlite_results)
    duck_results = round_results(duck_results)
    pandas_results = round_results(pandas_results)
    alchemy_results = round_results(alchemy_results)

    df = pd.DataFrame({
        'Psycopg2': results,
        'SQLite': sqlite_results,
        'DuckDB': duck_results,
        'Pandas': pandas_results,
        'SQLAlchemy': alchemy_results
    })

    df.index = [f'Query {i+1}' for i in range(len(results))]

    df.to_csv('results.csv', index=False)

if __name__ == "__main__":
    main()