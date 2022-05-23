import requests
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from datetime import datetime
from logger import get_logger
import os
from dotenv import load_dotenv

logger = get_logger()


def getEnvironmentVariables():
    try:
        logger.info("Receiving Data From Environment Variables....")
        DATABASE_HOST = os.getenv('DATABASE_HOST')
        DATABASE_USER = os.getenv('DATABASE_USER')
        DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
        DATABASE = os.getenv('DATABASE')
        logger.info('Received Data From Environment Variables!!!')
        return DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE
    except Exception as err:
        logger.exception(err)


def create_connection(h, u, p, d):
    try:
        logger.info('Creating Connection....')
        mydb = mysql.connector.connect(host=h, user=u, password=p, database=d)
        logger.info('Successfully Created Connection!!!')
        return mydb
    except Exception as err:
        logger.exception(err)


def fetch_city_data(c):
    try:
        logger.info('Fetching City Weather Data....')
        cursor = c.cursor()
        sql = 'SELECT Lon,Lat FROM city_weather'
        cursor.execute(sql)
        data1 = cursor.fetchall()
        print(data1)
        appended_data = []
        for x in data1:
            appended_data.append(x)
        logger.info('Fetched City Weather Data!!!')
        return appended_data
    except Exception as err:
        logger.exception(err)


def loop_city_data(data2):
    result = []
    try:
        logger.info('Looping Multiple City Data....')
        for lon, lat in data2:
            query = {'lat': lat, 'lon': lon, 'appid': 'c166a23bf20918eb9eb473980b75710e'}
            response = requests.get("https://api.openweathermap.org/data/2.5/weather", params=query)
            result.append(response.json())
        logger.info('Successfully Looped City Weather Data')
    except Exception as err:
        logger.exception(err)
        print(result)
    return result


def transformation(transformed_data):
    tdata = {}
    try:
        logger.info('Performing Transformations....')
        transformed_data["city_id"] = transformed_data.pop("id")
        for key, value in transformed_data.items():
            if isinstance(value, dict):
                for k, v in value.items():
                    tdata[f"{key}_{k}"] = v
                continue
            tdata[key] = value
        logger.info('Successfully Transformed!!!')
    except Exception as err:
        logger.exception(err)
    return tdata


def transformation_without_weather(weather_data):
    try:
        logger.info('Transformation Excluding Weather....')
        new_transformed = {k: weather_data[k] for k in weather_data.keys() - {'weather'}}
        logger.info('Successfully Transformed Excluding Weather!!!!')
        return new_transformed
    except Exception as err:
        logger.exception(err)


def create_dataframe(transformed_new1):
    try:
        logger.info('Creating Dataframes....')
        df = pd.DataFrame(transformed_new1)
        df.fillna(0, inplace=True)
        df['dt'] = pd.to_datetime(df['dt'], unit='s')
        df['created_by'] = 'system'
        df['created_in'] = datetime.now()
        # print(df['dt'])
        logger.info('Successfully Created Dataframes!!!')
        return df
    except Exception as err:
        logger.exception(err)


def dataframe_to_csv(dframe, a):
    try:
        logger.info('Creating CSV File....')
        dframe.to_csv(a, index=False)
    except Exception as err:
        logger.exception(err)


def schema_creation(cons):
    try:
        logger.info('Creating Schema.....')
        cursor = cons.cursor()
        schema = "CREATE TABLE IF NOT EXISTS city_weather_data (id INTEGER(110) AUTO_INCREMENT PRIMARY KEY, visibility " \
                 "bigint(20), " \
                 "base text, cod bigint(20), coord_lon bigint(20), coord_lat bigint(20), main_temp float(20), " \
                 "main_feels_like float(20), main_temp_min float(20), main_temp_max float(20), main_pressure bigint(" \
                 "20), main_humidity bigint(20), main_sea_level bigint(20), main_grnd_level bigint(20), sys_type bigint(" \
                 "20), sys_id bigint(20), sys_country text(500), sys_sunrise bigint(20), sys_sunset bigint(20), " \
                 "name text, timezone bigint(20), clouds_all bigint(20), dt datetime, wind_speed float(20), " \
                 "wind_deg bigint(20), wind_gust float(20), rain_1h float(50) DEFAULT 0, snow_1h float(50) DEFAULT 0, created_by VARCHAR(" \
                 "255), created_in datetime, city_id bigint(" \
                 "20)) "
        cursor.execute(schema)
        logger.info('Successfully Created Schema!!!')
    except Exception as err:
        logger.exception(err)


def load_to_db(df, h, u, d):
    try:
        logger.info('Loading To Database....')
        my_conn = create_engine("mysql+mysqldb://" + u + ":@" + h + "/" + d)
        df.to_csv('citydata.csv', index=False)
        df.to_sql('city_weather_data', con=my_conn, index=False, if_exists='append')
        logger.info('Successfully Loaded To Database!!!')
    except Exception as err:
        logger.exception(err)


def main():
    load_dotenv('D:\PycharmProjects\GettingStartedWithSQL\connection.env')
    host, user, pwd, database = getEnvironmentVariables()
    conn = create_connection(host, user, pwd, database)
    fetch = fetch_city_data(conn)
    data = loop_city_data(fetch)
    print(data)
    a = []
    for res in data:
        transformed = transformation_without_weather(res)
        transformed_new = transformation(transformed)
        a.append(transformed_new)
    dataframe = create_dataframe(a)
    city_weather = pd.read_sql('SELECT * FROM city_weather_data', conn)
    dataframe_to_csv(city_weather, 'weatherdata.csv')
    dataframe_to_csv(dataframe, 'cityweatherdata.csv')
    schema_creation(conn)
    load_to_db(dataframe, host, user, database)


if __name__ == "__main__":
    main()
