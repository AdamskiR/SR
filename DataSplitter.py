# Script by R. Jendraszak

import pandas as pd

path = "C:/Users/Rafal/Desktop/SR/Criteo_Conversion_Search/CriteoSearchData"
DS_file_name = 'CriteoSearchData'

columns = ['Sale', 'SalesAmountInEuro', 'time_delay_for_conversion', 'click_timestamp', 'nb_clicks_1week', 'product_price', 'product_age_group', 'device_type', 'audience_id', 'product_gender', 'product_brand','product_category_1', 'product_category_2', 'product_category_3', 'product_category_4', 'product_category_5', 'product_category_6', 'product_category_7', 'product_country', 'product_id', 'product_title', 'partner_id', 'user_id']

DS_DF = pd.read_csv(path, nrows=10000, delimiter='\t', names=columns, low_memory=False)

groups_DF = DS_DF.groupby('partner_id')

for partner in DS_DF['partner_id'].unique():
    temp_df = groups_DF.get_group(partner)
    temp_df.to_csv('Chunks/CSD_{0}.csv'.format(partner), index=False)
