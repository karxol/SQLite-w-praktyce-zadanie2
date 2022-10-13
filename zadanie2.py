from sqlite3 import Date
import sqlalchemy
import csv

from sqlalchemy import create_engine, MetaData, Integer, String, Table, Column, Float

from datetime import date #moduł z datą 

engine = create_engine('sqlite:///database.db', echo=True)

meta = MetaData()


def load_items_csv(csvfile): # odczytywanie plików csv
    datas = []
    with open(csvfile, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            datas.append(row)
    return datas

stations_datas = load_items_csv("clean_stations.csv")
measure_datas = load_items_csv("clean_measure.csv")

for m in measure_datas:
    m["date"] = date.fromisoformat(m["date"])

print(stations_datas,"\n")
print(measure_datas,"\n")
print(engine)
#tabele
stations = Table(
    "stations",
    meta,
    Column("station", String, primary_key=True),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Float),
    Column("name", String),
    Column("country", String),
    Column("state", String),
)

measure = Table(
    "measure", meta,
    Column("station", String),
    Column("date", date),
    Column("precip", Float),
    Column("tobs", Integer),
)
measure_datas_insert = measure.insert().values(measure_datas)
stadion_datas_insert = stations.insert().values(stations_datas)


conn = engine.connect()
conn.execute(measure_datas_insert)
conn.execute(stadion_datas_insert)

conn.execute("SELECT * FROM stations LIMIT 10").fetchall()
conn.execute("SELECT * FROM measure LIMIT 10").fetchall()
