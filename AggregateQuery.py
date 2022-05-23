import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from logger import get_logger
import logging
import schedule
import time
import os
from dotenv import load_dotenv
logger = get_logger()


def getEnvironmentVariables():
    DATABASE_HOST = os.getenv('DATABASE_HOST')
    DATABASE_USER = os.getenv('DATABASE_USER')
    DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
    DATABASE = os.getenv('DATABASE')
    return DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE


def create_connection(h, u, p, d):
    mydb = mysql.connector.connect(host=h, user=u, password=p, database=d)
    return mydb


def average_query(c):
    cursor = c.cursor()
    sql = 'SELECT AVG(main_temp) AS temperature, AVG(main_pressure) AS pressure, AVG(main_humidity) AS humidity, minute(dt), city_id, name AS city_name FROM city_weather_data GROUP BY minute(dt)'
    cursor.execute(sql)
    data = cursor.fetchall()
    appended_data = []
    for x in data:
        appended_data.append(x)
    return appended_data


def sum_query(c):
    cursor = c.cursor()
    sql = 'SELECT SUM(main_temp) AS temperature, SUM(main_pressure) AS pressure, SUM(main_humidity) AS humidity, minute(dt), city_id, name AS city_name FROM city_weather_data GROUP BY minute(dt)'
    cursor.execute(sql)
    data = cursor.fetchall()
    appended_data = []
    for x in data:
        appended_data.append(x)
    return appended_data


def create_dataframe(dff, dff1):
    df1 = pd.DataFrame(dff)
    df1.fillna(0, inplace=True)
    df1.columns = ['temperature', 'pressure', 'humidity', 'minutes', 'city_id', 'city_name']
    df2 = pd.DataFrame(dff1)
    df2.fillna(0, inplace=True)
    df2.columns = ['temperature', 'pressure', 'humidity', 'minutes', 'city_id', 'city_name']
    return df1, df2


def schema_creation(conn):
    cursor = conn.cursor()
    schema1 = "CREATE TABLE IF NOT EXISTS weather_average_data (temperature float(20), pressure float(20), humidity float(20), minutes int(20), city_id int(10), city_name VARCHAR(255)) "
    schema2 = "CREATE TABLE IF NOT EXISTS weather_sum_data (temperature float(20), pressure float(20), humidity float(20), minutes int(20), city_id int(10), city_name VARCHAR(255)) "
    cursor.execute(schema1)
    cursor.execute(schema2)


def load_to_db(df, df1, h, u, d):
    my_conn = create_engine("mysql+mysqldb://"+u+":@"+h+"/"+d)
    try:
        df.to_sql('weather_average_data', con=my_conn, index=False, if_exists='append')
        df1.to_sql('weather_sum_data', con=my_conn, index=False, if_exists='append')
    except Exception as err:
        print(f"Error: '{err}'")
        logger.error(err)


def main():
    load_dotenv('D:\PycharmProjects\GettingStartedWithSQL\connection.env')
    con = create_connection()
    datas = average_query(con)
    print(datas)
    datas1 = sum_query(con)
    data_frame, data_frame1 = create_dataframe(datas, datas1)
    print(data_frame)
    schema_creation(con)
    load_to_db(data_frame, data_frame1)


if __name__ == "__main__":
    main()
    # schedule.every(60).minutes.do(main)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
