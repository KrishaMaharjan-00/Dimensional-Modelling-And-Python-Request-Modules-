import mysql.connector
from sqlalchemy import create_engine
from logger import get_loggers
import os
from dotenv import load_dotenv
import pandas as pd
import warnings


def create_connection(h, u, p, d):
    try:
        logger.info('Creating Connection....')
        mydb = mysql.connector.connect(host=h, user=u, password=p, database=d)
        logger.info('Successfully Created Connection!!!')
        return mydb
    except Exception as err:
        logger.exception(err)


def select_query():
    try:
        logger.info('Selecting Data From Weather Facts.....')
        fact_query = "SELECT * FROM weather_facts;"
        return fact_query
    except Exception as err:
        logger.exception(err)


def query_to_dataframe(conn, schema):
    try:
        logger.info('Reading SQL Query.....')
        sql_query = pd.read_sql_query(schema, conn)
        df = pd.DataFrame(sql_query)
        return df
        logger.info('Successfully Turned Query Into Dataframes!!!')
    except Exception as err:
        logger.exception(err)


def aggregate_by_wc_date(data, host, user, database):
    logger.info('Performing Aggregations......')
    agg = data.groupby(['id', 'Date'], as_index=False).agg(max_temperature=('temperature', 'max'),
                                           average_temperature=('temperature', 'mean'),
                                           min_temperature=('temperature', 'min'),
                                           max_pressure=('pressure', 'max'),
                                           average_pressure=('pressure', 'mean'),
                                           min_pressure=('pressure', 'min'),
                                           max_humidity=('humidity', 'max'),
                                           average_humidity=('humidity', 'mean'),
                                           min_humidity=('humidity', 'min')
                                           )
    logger.info('Successfully Performed Aggregations!!!')
    load_to_db(agg, host, user, database, 'aggregate_wc_date')


def aggregate_by_wc(data, host, user, database):
    logger.info('Performing Aggregations......')
    agg = data.groupby(['id'], as_index=False).agg(max_temperature=('temperature', 'max'),
                                   average_temperature=('temperature', 'mean'),
                                   min_temperature=('temperature', 'min'),
                                   max_pressure=('pressure', 'max'),
                                   average_pressure=('pressure', 'mean'),
                                   min_pressure=('pressure', 'min'),
                                   max_humidity=('humidity', 'max'),
                                   average_humidity=('humidity', 'mean'),
                                   min_humidity=('humidity', 'min')
                                   )
    load_to_db(agg, host, user, database, 'aggregate_wc')


def aggregate_by_date(data, host, user, database):
    logger.info('Performing Aggregations......')
    agg = data.groupby(['Date'], as_index=False).agg(max_temperature=('temperature', 'max'),
                                     average_temperature=('temperature', 'mean'),
                                     min_temperature=('temperature', 'min'),
                                     max_pressure=('pressure', 'max'),
                                     average_pressure=('pressure', 'mean'),
                                     min_pressure=('pressure', 'min'),
                                     max_humidity=('humidity', 'max'),
                                     average_humidity=('humidity', 'mean'),
                                     min_humidity=('humidity', 'min')
                                     )
    load_to_db(agg, host, user, database, 'aggregate_date')


def aggregate_by_hours(data, host, user, database):
    logger.info('Performing Aggregations......')
    agg = data.groupby(['hours'], as_index=False).agg(max_temperature=('temperature', 'max'),
                                      average_temperature=('temperature', 'mean'),
                                      min_temperature=('temperature', 'min'),
                                      max_pressure=('pressure', 'max'),
                                      average_pressure=('pressure', 'mean'),
                                      min_pressure=('pressure', 'min'),
                                      max_humidity=('humidity', 'max'),
                                      average_humidity=('humidity', 'mean'),
                                      min_humidity=('humidity', 'min')
                                      )
    load_to_db(agg, host, user, database, 'aggregate_hours')


def load_to_db(df, h, u, d, table_name):
    try:
        my_conn = create_engine("mysql+mysqldb://" + u + ":@" + h + "/" + d)
        logger.info('Loading To Database....')
        df.to_sql(table_name, con=my_conn, index=False, if_exists='replace')
        logger.info('Successfully Loaded To Database!!!')
    except Exception as err:
        logger.exception(err)


def main():
    load_dotenv('D:\PycharmProjects\GettingStartedWithSQL\connection.env')
    logger.info('Retrieving database credentials from environmental file')
    DATABASE_HOST = os.getenv('DATABASE_HOST')
    DATABASE_USER = os.getenv('DATABASE_USER')
    DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
    DATABASE = os.getenv('DATABASE')
    con = create_connection(DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE)
    datas = select_query()
    dataframe = query_to_dataframe(con, datas)
    aggregate_by_wc_date(dataframe, DATABASE_HOST, DATABASE_USER, DATABASE)
    aggregate_by_wc(dataframe, DATABASE_HOST, DATABASE_USER, DATABASE)
    aggregate_by_date(dataframe, DATABASE_HOST, DATABASE_USER, DATABASE)
    aggregate_by_hours(dataframe, DATABASE_HOST, DATABASE_USER, DATABASE)


if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    logger = get_loggers()
    main()
