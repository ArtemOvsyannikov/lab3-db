import os
import statistics
import time

import duckdb
import gdown
import pandas as pd
import psycopg2
import sqlite3
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine


def download_dataset():
    if not os.path.exists('data/tiny.csv'):
        if not os.path.exists('data'):
            os.makedirs('data')

        url = 'https://drive.google.com/uc?export=download&id=1XWCk4XmgdNUZ8E42ktjGpeeKZeTO9YnJ'
        output = 'data/tiny.csv'
        gdown.download(url, output, quiet=False)

def execute_query(conn, cur, query):
    times = []
    for _ in range(10):
        start_time = time.perf_counter()
        if isinstance(cur, pd.DataFrame) and callable(query):
            query(cur)
        elif isinstance(conn, Engine):
            with conn.connect() as connection:
                result = connection.execute(text(query))
                result.fetchall()
        else:
            cur.execute(query)
            cur.fetchall()
        end_time = time.perf_counter()
        times.append(end_time - start_time)
    median_time = statistics.median(times)
    return median_time


def execute_queries(conn, cur, queries):
    results = []
    for query in queries:
        result = execute_query(conn, cur, query)
        results.append(result)
    if not isinstance(cur, pd.DataFrame) and not isinstance(conn, Engine):
        cur.close()
        conn.close()
    return results


def round_results(results):
    return [round(result, 3) for result in results]


def main():
    download_dataset()

    df = pd.read_csv('./data/tiny.csv')
    if 'Airport_fee' in df.columns:
        df = df.drop(columns=['Airport_fee'])

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

    sqlite_conn = sqlite3.connect('sqlite_database.db')
    df.to_sql('trips', sqlite_conn, if_exists='replace', index=False)

    duck_conn = duckdb.connect(database='duckdb_database.db', read_only=False)
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

    queries = [
        """SELECT "VendorID", COUNT(*)
           FROM trips GROUP BY 1;""",
        """SELECT "passenger_count", AVG("total_amount")
           FROM trips GROUP BY 1;""",
        """SELECT "passenger_count", EXTRACT(year FROM CAST("tpep_pickup_datetime" AS TIMESTAMP)), COUNT(*)
           FROM trips GROUP BY 1, 2;""",
                """SELECT "passenger_count", EXTRACT(year FROM CAST("tpep_pickup_datetime" AS TIMESTAMP)), ROUND("trip_distance"), COUNT(*)
           FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;""",
    ]

    sqlite_queries = [
        """SELECT "VendorID", COUNT(*)
           FROM trips GROUP BY 1;""",
        """SELECT "passenger_count", AVG("total_amount")
           FROM trips GROUP BY 1;""",
        """SELECT "passenger_count", strftime('%Y', "tpep_pickup_datetime"), COUNT(*)
           FROM trips GROUP BY 1, 2;""",
        """SELECT "passenger_count", strftime('%Y', "tpep_pickup_datetime"), ROUND("trip_distance"), COUNT(*)
           FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;""",
    ]

    pandas_queries = [
        lambda df: df.groupby("VendorID").size(),
        lambda df: df.groupby("passenger_count")["total_amount"].mean(),
        lambda df: df.assign(year=pd.to_datetime(df["tpep_pickup_datetime"]).dt.year).groupby(["passenger_count", "year"]).size(),
        lambda df: df.assign(year=pd.to_datetime(df["tpep_pickup_datetime"]).dt.year, distance=df["trip_distance"].round()).groupby(["passenger_count", "year", "distance"]).size().sort_values(ascending=False),
    ]

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