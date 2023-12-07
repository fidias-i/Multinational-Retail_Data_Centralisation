from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import boto3
from io import StringIO
import json

class DataExtractor(DatabaseConnector):
    def __init__(self):
        super().__init__()
        self.api_dict = {}
        self.api_dict['x-api-key'] = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    def read_rds_table(self,table_name):
        self.db_conn_data = super().read_db_creds()
        self.engine = super().init_db_engine()
        self.table_list = super().list_db_tables()
        # self.sql_query = 'SELECT * FROM legacy_users LIMIT 5'
        self.connection = self.engine.connect()
        #result = self.connection.execute(self.sql_query)
        #pd.read_sql_query(self.sql_query, self.engine)
        self.table_name = table_name
        self.df = pd.read_sql_table(self.table_name, con=self.connection)
        return self.df
    #task 4
    def retrieve_pdf_data(self,pdf_path):
        pdf_df = tabula.read_pdf(pdf_path, pages="all")
        return pdf_df
    #task 5
    def list_number_of_stores(self):   
        url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(url, headers=self.api_dict)
        json_data = json.loads(response.text)
        self.number_stores = json_data.get("number_stores")
        print(self.number_stores)
        return self.number_stores
        #print the text
    def retrieve_stores_data(self):
        columns = ['store_number', 'response_content']
        store_list = []
        self.api_dict['x-api-key'] = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        for store_number in range(0,self.number_stores):   
            url = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
            r = requests.get(url, headers=self.api_dict)
            # r = requests.get(url, headers=api_dict)
            j = r.json()
            store_list.append(j)
            # print(j.keys())
            # if store_number == 2:
                # break
        df = pd.DataFrame(store_list)
        return df
            
# for store_number in range(0,number_stores):   
#     url = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
#     r = requests.get(url, headers=api_dict)
#     # r = requests.get(url, headers=api_dict)
#     j = r.json()
#     store_list.append(j)
#     # print(j.keys())
#     if store_number == 2:
#         break
#     df = pd.DataFrame(store_list)
#         return df

    #task 6  
    def extract_from_s3(self,address):
        keys_df = pd.read_csv('Fidias_accessKeys.csv')
        aws_access_key_id = keys_df['Access key ID'][0]
        aws_secret_access_key = keys_df['Secret access key'][0]
        region_name = 'us-east-1'

        address_without_s3 = address.replace('s3://','')
        bucket_name,csv_key = address_without_s3.split('/')
        # Create an S3 client
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
        # Read the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=csv_key)
        csv_data = response['Body'].read().decode('utf-8')

        # Use pandas to create a DataFrame
        df = pd.read_csv(StringIO(csv_data))

        # Display the DataFrame
        return df
    #Task 8
    def extract_json_from_s3(self):
        address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        region_name = 'eu-west-1'
        s3 = boto3.client('s3', region_name=region_name)
        bucket_name,file_key = address.split('.s3.')
        bucket_name = bucket_name.replace('https://','')
        file_key = file_key.replace(f'{region_name}.amazonaws.com/','')
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        json_data = response['Body'].read().decode('utf-8')
# Load JSON data into a Pandas DataFrame
        df = pd.read_json(json_data)
        return df

# if __name__ == "__main__":
    # extr = DataExtractor()
    # print(extr.read_rds_table())
    # datacleaner = DataCleaning()
    # cleaned_df = datacleaner.clean_user_data()
    #print(cleaned_df.head())




# class DataExtractor():

#     def read_rds_table(self):
#         self.mydb = DatabaseConnector()
#         self.db_conn_data = self.mydb.read_db_creds()
#         self.engine = self.mydb.init_db_engine()
#         self.table_list = self.mydb.list_db_tables()
#         self.sql_query = 'SELECT * FROM legacy_users LIMIT 5'
#         self.connection = self.engine.connect()
#         #result = self.connection.execute(self.sql_query)
#         #pd.read_sql_query(self.sql_query, self.engine)
#         self.table_name = 'legacy_users'
#         self.df = pd.read_sql_table(self.table_name, con=self.connection)
#         return self.df