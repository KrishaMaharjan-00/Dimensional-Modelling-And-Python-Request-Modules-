import gzip
import json
import mysql.connector
from sqlalchemy import create_engine
from logger import get_loggers
import os
from dotenv import load_dotenv
import pandas as pd
import warnings
from sqlalchemy.dialects.mysql import insert


def create_connection(h, u, p, d):
    try:
        logger.info('Creating Connection....')
        mydb = mysql.connector.connect(host=h, user=u, password=p, database=d)
        logger.info('Successfully Created Connection!!!')
        return mydb
    except Exception as err:
        logger.exception(err)


def schema_creation(conn, schema):
    try:
        cursor = conn.cursor()
        cursor.execute(schema)
    except Exception as err:
        logger.exception(err)


def fetching_json_city_data():
    try:
        logger.info('Fetching JSON Data.....')
        with gzip.open("city.list.json.gz", "r") as f:
            data = f.read()
        association_data = json.loads(data.decode('utf-8'))
        return association_data
        logger.info('Fetched Data......')
    except Exception as err:
        logger.exception(err)


def transformation(t_data):
    tdata = {}
    try:
        for key, value in t_data.items():
            if isinstance(value, dict):
                for k, v in value.items():
                    tdata[f"{key}_{k}"] = v
                continue
            tdata[key] = value
    except Exception as err:
        logger.exception(err)
    return tdata


def transformation_without_state(s_data):
    try:
        new_transformed = {k: s_data[k] for k in s_data.keys() - {'state'}}
        return new_transformed
    except Exception as err:
        logger.exception(err)


def create_dataframe(data):
    try:
        logger.info('Creating Dataframes....')
        df = pd.DataFrame(data)
        df.fillna(0, inplace=True)
        logger.info('Successfully Created Dataframes!!!')
        return df
    except Exception as err:
        logger.exception(err)


def create_date_dataframe(start, end):
    try:
        logger.info('Creating Date Dataframes....')
        df = pd.DataFrame({"Date": pd.date_range(start, end)})
        df["year"] = df.Date.dt.year
        df["month"] = df.Date.dt.month
        df["day"] = df.Date.dt.day
        return df
        logger.info('Successfully Created Date Dataframes!!!')
    except Exception as err:
        logger.exception(err)


def create_time_dataframe(start, end):
    try:
        logger.info('Creating Time Dataframes....')
        df = pd.DataFrame({"Date": pd.date_range(start, end, freq="1H")})
        df["hours"] = df.Date.dt.hour
        df["minutes"] = df.Date.dt.minute
        df["seconds"] = df.Date.dt.second
        df.drop(columns=['Date'], inplace=True)
        return df
        logger.info('Successfully Created Time Dataframes!!!')
    except Exception as err:
        logger.exception(err)


def load_to_db(df, h, u, d, tables):
    try:
        my_conn = create_engine("mysql+mysqldb://" + u + ":@" + h + "/" + d)
        logger.info('Loading To Database....')

        def insert_on_duplicate(table, conn, keys, data_iter):
            insert_stmt = insert(table.table).values(list(data_iter))
            on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
            conn.execute(on_duplicate_key_stmt)

        df.to_sql(tables,  con=my_conn, index=False, if_exists='append', chunksize=4096, method=insert_on_duplicate)
        logger.info('Successfully Loaded To Database!!!')

    except Exception as err:
        logger.exception(err)


def abstract_citydata(c):
    cursor = c.cursor()
    sql = 'SELECT DISTINCT city_id FROM city_weather_data'
    cursor.execute(sql)
    data1 = cursor.fetchall()
    city_list = []
    for x in data1:
        city_list.append(x)
    return city_list


def manage_location_dimension(c, h, u, d):
    as_data = fetching_json_city_data()
    data = []
    logger.info('Performing Transformations....')
    for i in as_data:
        transformed = transformation_without_state(i)
        transformed_data = transformation(transformed)
        data.append(transformed_data)
    city_list = abstract_citydata(c)
    transformed_dat = [d for d in data if d['id'] in city_list]
    logger.info('Successfully Transformed!!!!')
    data_frame = create_dataframe(transformed_dat)
    logger.info('Defining Location Schema......')
    location_schema = """CREATE TABLE IF NOT EXISTS location_dimension 
                                (id int(20) PRIMARY KEY, name VARCHAR(255), country VARCHAR(255),  
                                coord_lon bigint(20), coord_lat bigint(20)) """
    schema_creation(c, location_schema)
    logger.info('Successfully Created Location Schema!!!!')
    load_to_db(data_frame, h, u, d, 'location_dimension')


def manage_date_dimension(c, h, u, d):
    date_frame = create_date_dataframe('2000-01-01', '2050-12-31')
    logger.info('Creating Date Schema.....')
    date_schema = """CREATE TABLE IF NOT EXISTS date_dimension 
                        (Date date PRIMARY KEY, year int(20), month int(30), 
                        day int(30))"""
    schema_creation(c, date_schema)
    logger.info('Successfully Created Date Schema!!!!')
    load_to_db(date_frame, h, u, d, 'date_dimension')


def manage_time_dimension(c, h, u, d):
    time_schema = """CREATE TABLE IF NOT EXISTS time_dimension
    (hours int(20) PRIMARY KEY,
    minutes int(20), seconds int(20)) """
    schema_creation(c, time_schema)
    time_frame = create_time_dataframe("01:00", "23:00")
    load_to_db(time_frame, h, u, d, 'time_dimension')


def merge_citydata_with_date(conn):
    logger.info('Reading City Weather Data')
    weather_df = pd.read_sql('SELECT * FROM city_weather_data', conn)
    logger.info('Extracting date only from datetime')
    weather_df['date'] = weather_df['dt'].dt.date
    weather_df['time'] = weather_df['dt'].dt.hour
    logger.info('Reading Date Dimension Data')
    date_dim_df = pd.read_sql('SELECT * FROM date_dimension', conn)

    logger.info('Renaming Certain Columns For Ease Of Use')
    weather_df.rename(columns={'id': 'weather_id', 'main_temp': 'temperature', 'main_pressure': 'pressure', 'main_humidity': 'humidity',
                               'main_sea_level': 'sea_level', 'main_grnd_level': 'ground_level'}, inplace=True)

    logger.info('Merging city weather and date table')
    weather_date = pd.merge(date_dim_df[['Date']], weather_df[
        ['date', 'time', 'weather_id', 'city_id', 'temperature', 'pressure', 'humidity', 'sea_level', 'ground_level']],
                            left_on='Date',
                            right_on='date', how='inner')

    logger.info('Dropping date column from merge table')
    weather_date.drop(columns=['date'], inplace=True)
    return weather_date


def merge_citydate_with_time(conn, weather_date):
    logger.info('Reading Time Dimension Data')
    time_df = pd.read_sql('SELECT * FROM time_dimension', conn)
    logger.info('Merging city weather and date with time table')
    weather_dates = pd.merge(time_df[['hours']], weather_date, left_on='hours', right_on='time',
                             how='inner')
    logger.info('Dropping time column from merge table')
    weather_dates.drop(columns=['time'], inplace=True)
    return weather_dates


def merge_citydatetime_with_location(conn, weather_dates):
    logger.info('Reading Location Dimension Data')
    loc_df = pd.read_sql('SELECT * FROM location_dimension', conn)

    logger.info('Merging Location Data With Weather Date Data')
    weather_date_loc = pd.merge(loc_df[['id']], weather_dates, left_on='id', right_on='city_id',
                                how='inner')
    logger.info('Dropping id from location dimension table')
    weather_date_loc.drop(columns=['city_id'], inplace=True)
    return weather_date_loc


def create_weather_fact(conn, host, user, database):
    logger.info('Creating Weather Schema.....')
    weather_schema = """CREATE TABLE IF NOT EXISTS weather_facts 
                            ( 
                            id int(30) REFERENCES location_dimension(id), 
                            hours int(30) REFERENCES time_dimension(hours),
                            Date date REFERENCES date_dimension(Date), 
                            weather_id int(110) PRIMARY KEY,
                            temperature float(50), pressure bigint(20), 
                            humidity bigint(20), sea_level bigint(20), ground_level bigint(20))"""
    schema_creation(conn, weather_schema)
    logger.info('Successfully Created Weather Schema!!!!!')
    city_date = merge_citydata_with_date(conn)
    citydate_time = merge_citydate_with_time(conn, city_date)
    citydatetime_loc = merge_citydatetime_with_location(conn, citydate_time)
    load_to_db(citydatetime_loc, host, user, database, 'weather_facts')
    conn.commit()


def main():
    load_dotenv('D:\PycharmProjects\GettingStartedWithSQL\connection.env')

    logger.info('Retrieving database credentials from environmental file')
    DATABASE_HOST = os.getenv('DATABASE_HOST')
    DATABASE_USER = os.getenv('DATABASE_USER')
    DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
    DATABASE = os.getenv('DATABASE')

    con = create_connection(DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE)

    manage_location_dimension(con, DATABASE_HOST, DATABASE_USER, DATABASE)
    manage_date_dimension(con, DATABASE_HOST, DATABASE_USER, DATABASE)
    manage_time_dimension(con, DATABASE_HOST, DATABASE_USER, DATABASE)
    create_weather_fact(con, DATABASE_HOST, DATABASE_USER, DATABASE)


if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    logger = get_loggers()
    main()
