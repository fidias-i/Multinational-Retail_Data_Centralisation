# from data_extraction import DasstaExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

# Task 3: Extract and clean user data
extractor_cls = DataExtractor()
df = extractor_cls.read_rds_table('legacy_users')
db_connn_cls = DatabaseConnector()
db_connn_cls.read_db_creds()
db_connn_cls.init_db_engine()
db_tables = db_connn_cls.list_db_tables()

datacleaning_cls = DataCleaning()
cleaned_df,dirty_df = datacleaning_cls.clean_user_data(df)
print(df.head())
db_connn_cls.upload_to_db(cleaned_df,'dim_users')

# Task 4: Extract and clean pdf data
pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
pdf_df = extractor_cls.retrieve_pdf_data(pdf_path)
cleaned_pdf_df = datacleaning_cls.clean_card_data(pdf_df)
print(cleaned_pdf_df)
cleaned_pdf_df.to_csv('cleaned_card_data.csv',index=False)
db_connn_cls.upload_to_db(cleaned_pdf_df,'dim_card_details')

import pandas as pd
# store_data = pd.read_csv('store_data.csv')
# Task 5: Extract API data
number_stores = extractor_cls.list_number_of_stores()
store_data = extractor_cls.retrieve_stores_data()
cleaned_store_data = datacleaning_cls.clean_store_data(store_data)
db_connn_cls.upload_to_db(cleaned_store_data,'dim_store_details')
store_data_copy = store_data.copy()
# store_data_copy['staff_numbers'] = reduce(lambda s, char: s.str.replace(char, ''), characters_to_remove, df['phone_number'])


# Task 6: Extract s3 data (product details)
products = extractor_cls.extract_from_s3('s3://data-handling-public/products.csv')
products = datacleaning_cls.convert_product_weights(products)
products = datacleaning_cls.clean_products_data(products)

db_connn_cls.upload_to_db(products,'sales_data')


#Task 7: orders_table
df_orders = extractor_cls.read_rds_table('orders_table')
cleaned_orders_df = datacleaning_cls.clean_orders_data(df_orders)
db_connn_cls.upload_to_db(cleaned_orders_df,'orders_table')

#Task 8 : Date events - JSON data
json_df = extractor_cls.extract_json_from_s3()
json_df.columns
# json_df_clean['time_period'].unique()
json_df_clean = datacleaning_cls.clean_json_data(json_df)
db_connn_cls.upload_to_db(json_df_clean,'dim_date_times')



# pattern = r'(?P<numerical>[\d.]+)?\s*(?P<non_numerical>\D+)?'
# df_extracted = products['weight'].str.extract(pattern)

# Task 5: orders table
# import requests
# import pandas as pd
# number_stores = 451
# api_dict = {}
# api_dict['x-api-key'] = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
# columns = ['store_number', 'response_content']
# df = pd.DataFrame(columns = columns)
# for store_number in range(0,number_stores): 
#     store_number = str(store_number)
#     url = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
#     # print(url)
#     response = requests.get(url, headers=api_dict)
#     # print(response.content)
#     df.loc[store_number] = response.content
# print(df)

#orders table has entry for integer 4971858637664481
cleaned_orders_df[cleaned_orders_df['card_number'] == '4971858637664481'] 
filtered_df = cleaned_orders_df[cleaned_orders_df['card_number'] == 4971858637664481]

#dim card details
filtered_df2 = cleaned_pdf_df[cleaned_pdf_df['card_number'] == 4971858637664481] 

#dim_store_details issue
store_data
filtered_df3 = store_data[store_data['store_code'] == 'WEB-1388012W'] 
filtered_df4 = store_data[store_data['store_code'] == 'SO-B5B9CB3B'] 
filtered_df5 = dirty_store_data[dirty_store_data['store_code'] == 'SO-B5B9CB3B'] 
# 429+15  MI-20EE1BFA
filtered_df4 = cleaned_store_data[cleaned_store_data['store_code'] == 'MI-20EE1BFA'] 
filtered_df5 = dirty_store_data[dirty_store_data['store_code'] == 'MI-20EE1BFA'] 


#dim users
cleaned_df
filtered_df6 = cleaned_df[cleaned_df['user_uuid'] == '7c8dc654-3994-40b1-a4a2-e55dc7081fe0'] 
filtered_df7 = dirty_df[dirty_df['user_uuid'] == 'bd3e3bc5-3a25-40dd-ac0f-3f2aaf5814df'] 
filtered_df8 = cleaned_df[cleaned_df['user_uuid'] == 'bd3e3bc5-3a25-40dd-ac0f-3f2aaf5814df'] 

df[df['user_uuid'] == 'bd3e3bc5-3a25-40dd-ac0f-3f2aaf5814df'] 
dirty_df[dirty_df['country_code'].unique()]

store_data.to_csv('store_data.csv',index=False)
row_store_data = store_data[store_data['staff_numbers'] == 'A97']

store_data_cleaned_step = store_data[store_data['latitude'].str.isnumeric()]
