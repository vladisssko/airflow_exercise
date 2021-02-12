"""Short workflow as an exercise for working with Apache Airflow.
It imports a covid xml file from S3, transforms data and loads to Postgres tabel .
+ Fetch covid data from API and imports to Postgres database.
output : two tables in DB 
countries2020_2021 =  vaccinations given in 2020/2021 per country
totals2020_2021 = total vaccinations given per world in 2020/2021, total deaths per 2020/2021

"""

import csv
import requests
import json
import pandas as pd 
import pandasql as ps
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
import boto3
from datetime import date

class ImportCovidS3dPercountry:
    """Basic python callable class to initialize and run the `VaccinatedPercountry` class.

    Functions:
        run(loperation, table_name, **kwargs):
            Initializes and runs the `CovidRefresh` class, calling the `main` function.
    """

    @staticmethod
    def run(operation, table_name, **kwargs):
        """Initializes and runs the `VaccinatedPercountry` class, calling the `main` function."""
        vaccinated_percountry = VaccinatedPercountry(task_instance=kwargs['task_instance'])
        result = vaccinated_percountry.main(
            table_name=table_name
            , task=operation)
        return result

class VaccinatedPercountry:
    """Class to import covid data

    Attributes:
        task_instance: the airflow task instance object used for Xcoms
    Functions:
        __init__(dag_info, task_instance, logger): Initializes the CovidRefresh class
        main(table_name, task): Wrapper to call the right function for a given operation
        start_etl(): Exctracts data from S3, transform and load to postgreSQL
        create_table(table_name): Create the destination table in the database
        start_etl_totals(): 
            exctracts data from table countries2020_2021, transform and load to postgreSQL
        start_etl_deaths: 
            Fetch covid data from API , transform data and loads to Postgres totals2020_2021  
        
    """

    def __init__(self, task_instance):
        """Initializes the CovidRefresh class"""
        self.task_instance = task_instance
        

    def main(self, table_name, task):
        """Wrapper to call the right function for a given operation

        Parameters:
            table_name(string): The name of the table data is being imported into
            task(string): The specific subtask that needs to be executed
        Returns:
            bool: True if everything successful
        """

        if task == 'start_etl':
            self.start_etl(table_name)
        elif task == 'create_table_countries2020_2021':
            self.table_name = table_name
            self.create_table_countries2020_2021(table_name)
        elif task == 'create_table_totals2020_2021':
            self.create_table_totals2020_2021(table_name)
        elif task == 'start_etl_totals':
            self.start_etl_totals(table_name)
        elif task == 'start_etl_deaths':
            self.start_etl_deaths(table_name)
        else:
            return False
        return True

    def create_table_countries2020_2021(self, table_name):    
        """Create the destination table in the database

        Parameters:
            table_name(string): The name of the table that should be created
        Returns:
            bool: True if everything successful
        """

        # get the xml file's path that holds the SQL
        path = __file__.split('.')[-2] + '.xml'
        create_table_query = ET.parse(path).getroot().find('create_table_countries2020_2021').text
        pg_hook = PostgresHook(postgres_conn_id='covid_id')
        pg_hook.run(create_table_query.format(table_name=table_name))
        return True


    def start_etl(self, table_name):
        """Download the data from S3 bucket,  transform data, load to table(countries2020_2021)

        Parameters:
            table_name(string): The name of the table that will be used for loading data
        Returns:
            bool: True if everything successful
        """

        # 1. extract  data from s3 to dataframe 
        client = boto3.client('s3')
        path = 's3://covid-exercise/covid-data.csv'
        df = pd.read_csv(path)

        # 2. transform data(dataframe) with pandas for loading to postgreSQL
        q2021 = """SELECT location, SUM (new_vaccinations) FROM df
                WHERE new_vaccinations IS NOT NULL AND date LIKE '%2021%' GROUP BY location""" 
        q2020 = """SELECT location, SUM (new_vaccinations) FROM df WHERE
                new_vaccinations IS NOT NULL AND date LIKE '%2020%' GROUP BY location""" 
        df2021 = ps.sqldf(q2021, locals())
        df2020 = ps.sqldf(q2020, locals())
        df2021 = df2021.rename(columns = {'location': 'Country',
                                        'SUM (new_vaccinations)': 'Total_2021'}, inplace = False)
        df2020 = df2020.rename(columns = {'location': 'Country',
                                        'SUM (new_vaccinations)': 'Total_2020'}, inplace = False)
        dict1 = df2020.set_index('Country')['Total_2020'].to_dict()
        df2021['Total_2020']= df2021['Country'].map(dict1)
        df2021['Total_2021'] = df2021['Total_2021'].fillna(0).astype(int)
        df2021['Total_2020'] = df2021['Total_2020'].fillna(0).astype(int)
        df2021=df2021.reindex(columns=['Country','Total_2020','Total_2021'])
        
        # 3. load to posgreSQL
        connection = PostgresHook(postgres_conn_id='covid_id').get_conn()
        cursor = connection.cursor()

        for index, row in df2021.iterrows():    
            cursor.execute("""INSERT INTO countries2020_2021 
                    (Country, Total_2020, Total_2021) 
                    VALUES (%s, %s, %s)""",
                    (row["Country"], row["Total_2020"], row["Total_2021"]) )
        connection.commit()
        return True

    def create_table_totals2020_2021(self, table_name):
        """Create the destination table in the database

        Parameters:
            table_name(string): The name of the table that should be created
        Returns:
            bool: True if everything successful
        """

        # get the xml file's path that holds the SQL
        path = __file__.split('.')[-2] + '.xml'
        create_table_query = ET.parse(path).getroot().find('create_table_totals2020_2021').text
        pg_hook = PostgresHook(postgres_conn_id='covid_id')
        pg_hook.run(create_table_query.format(table_name=table_name))
        return True

    def start_etl_totals(self, table_name):
        """Gets the data from postgreSQL(countries2020_2021),
            transform data(world sum vaccined people), load to table(totals2020_2021)

        Parameters:
            table_name(string): The name of the table that will be used for loading data
        Returns:
            bool: True if everything successful
        """
        # 1. extract/SELECT  data from postgreSQL
        path = __file__.split('.')[-2] + '.xml'
        create_select_query =  ET.parse(path).getroot().find('select_query').text.format(table_name=table_name)
        df = pd.read_sql_query(create_select_query,con = PostgresHook(postgres_conn_id='covid_id').get_conn())
        
        # 2. transform data with pandas for loading to postgreSQL
        df = df.drop('country', 1)
        dfs = df.sum(axis=0)

        # 3. load to posgreSQL
        connection = PostgresHook(postgres_conn_id='covid_id').get_conn()
        cursor = connection.cursor()
        cursor.execute("""INSERT INTO totals2020_2021 
                    (fields, total_2020, total_2021) 
                    VALUES ('vaccinated', %s, %s)""",
                    (int(dfs[0]), int(dfs[1])) )
        connection.commit()
        return True

    def start_etl_deaths(self, table_name):
        """Fetch covid data from API , transform data and loads to Postgres totals2020_2021

        Parameters:
            table_name(string): The name of the table that should be used for loading
        Returns:
            bool: True if everything successful
        """
        #fetch API data 
        response = requests.get('https://corona-api.com/countries?include=timeline')
        data = json.loads(response.text)

        #transform data to totals per 2020 and 2021/today
        def countries_deaths_yearly(date):

            total_deaths = 0
            for i in range(len(data['data'])):
                for x in range(len( data['data'][i]['timeline'])):
                    if data['data'][i]['timeline'][x]['date'] == date:
                        total_deaths += data['data'][i]['timeline'][x]['deaths']

            return total_deaths

        deaths_2020 = countries_deaths_yearly('2020-12-31')
        deaths_2021 = countries_deaths_yearly(str(date.today())) - deaths_2020

        # load to posgreSQL
        connection = PostgresHook(postgres_conn_id='covid_id').get_conn()
        cursor = connection.cursor()
        cursor.execute("""INSERT INTO totals2020_2021 
                    (fields, total_2020, total_2021) 
                    VALUES ('deaths', %s, %s)""",
                    (deaths_2020, deaths_2021) )
        connection.commit()
        return True


          

