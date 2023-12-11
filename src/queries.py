import pandas as pd

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