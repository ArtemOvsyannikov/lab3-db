import pandas as pd
import statistics
import time
from sqlalchemy import text
from sqlalchemy.engine.base import Engine

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
    return statistics.mean(times)

def execute_queries(conn, cur, queries):
    results = []
    for query in queries:
        results.append(execute_query(conn, cur, query))
    return results

def round_results(results):
    return [round(num, 3) for num in results]