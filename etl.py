from datetime import date, datetime
import math
import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="brownliving"
)
mycursor = mydb.cursor()

today = date.today().isoformat().split("-")
df = pd.read_csv('data/incidents_{}_{}_{}.csv'.format(today[0], today[1], today[2]), header=0)

for data in df.iterrows():
    created_at_date = None
    resolved_date = None
    if data[1]["date_of_creation"] and type(data[1]["date_of_creation"]) != float:
        splitted_created_date = data[1]["date_of_creation"].split("-")
        created_at_date = date(int(splitted_created_date[2]), int(splitted_created_date[1]), int(splitted_created_date[0]))
        created_at_date = datetime.combine(created_at_date, datetime.min.time())
    
    if data[1]["date_of_resolution"] and type(data[1]["date_of_resolution"]) != float:
        splitted_resolution_date = data[1]["date_of_resolution"].split("-")
        resolved_date = date(int(splitted_resolution_date[2]), int(splitted_resolution_date[1]), int(splitted_resolution_date[0]))
        resolved_date = datetime.combine(resolved_date, datetime.min.time())

    sql = "INSERT INTO incidents (title, description, priority, status, created_at, resolved_date, user, source, country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (
        "{}".format(data[1]["title"]),
        "{}".format(data[1]["description"]),
        "{}".format(data[1]["priority"]),
        "{}".format(data[1]["status"]),
        created_at_date,
        resolved_date,
        "{}".format(data[1]["user"]),
        "csv",
        "{}".format(data[1]["country"])
    )
    print(val)
    mycursor.execute(sql, val)

mydb.commit()

print("Done inserting records:", len(df.index))

