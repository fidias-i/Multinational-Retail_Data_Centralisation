from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd

def remove_letters(weight):
    if isinstance(weight, str):
        return ''.join(char for char in weight if char.lower() not in ('g','m','l'))
        
    else:
        return weight
def convert_kg_to_g(weight):
    if isinstance(weight, str):
        if weight[-1] == 'k':
            weight = 1000 * float(weight[:-1])
            # return weight
        elif weight[-2:] == 'oz':
            weight = 28.3495 * float(weight[:-2])
    return weight
def perform_products(string):
    try:
        if isinstance(string, str):
            parts = [int(part) for part in string.split(' x ')]         
            result = parts[0] * parts[1]
            string = result
        return string
    except (ValueError, IndexError):
        return string
def remove_non_numbers(row):
    try:
        return int(row)
    except ValueError:
        return None

class DataCleaning():
    extractor = None

    def __init__(self):
        self.extractor = DataExtractor()
    def clean_user_data(self,df):
        # df = self.extractor.read_rds_table('legacy_users')
        print('Columns: \n')
        print(df.columns)
        print('Country codes: \n')
        print(df['country_code'].unique())
        df = df[[len(d)==2 for d in df['country_code']]]
        df = df[~df['phone_number'].str.contains('\.')]
        return df
        
    def clean_orders_data(self,df):
        df = df[['index', 'date_uuid', 'first_name', 'last_name', 'user_uuid',
        'card_number', 'store_code', 'product_code', 'product_quantity']]
        # df = self.extractor.read_rds_table('orders_table')
        # print('Columns: \n')
        # print(df.columns)
        return df

    def clean_pdf_data(self,pdf_df_list):
        return pdf_df
    def clean_card_data(self,pdf_df_list):
        pdf_df = pd.concat(pdf_df_list, axis=0)
        # pdf_df['expiry_date'] = pdf_df['expiry_date'].apply(lambda d: d if len(d) == 5 else None)
        pdf_df = pdf_df[[len(d) == 5 for d in pdf_df['expiry_date']]]
        return pdf_df
    def convert_product_weights(self,products):    
        products['weight'] = products['weight'].apply(remove_letters)
        products['weight'] = products['weight'].apply(convert_kg_to_g)
        return products
    def clean_products_data(self,products):
        products['weight'] = products['weight'].apply(perform_products)
        products['weight'] = products['weight'].apply(remove_non_numbers)
        return products
    def clean_store_data(self):
        number_stores = self.extractor.list_number_of_stores()
        store_data = self.extractor.retrieve_stores_data()
        return store_data
    def clean_json_data(self,df):
        df = df[[(len(d) in (6,7) or (d =='Late_Hours')) for d in df['time_period']]]
        return df
# datacleaner = DataCleaning()
# cleaned_df = datacleaner.clean_user_data()
# print(cleaned_df.head())
