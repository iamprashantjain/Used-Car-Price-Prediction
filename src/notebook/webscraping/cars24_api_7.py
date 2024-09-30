#run cars24_api.py to extract appointment_id & then run below

import pdb
import os
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings;warnings.filterwarnings('ignore')


# Load the appointment IDs
appointmentIds = pd.read_excel('cars24_data.xlsx')['appointmentId'].tolist()

# Initialize final DataFrame
final_df = pd.DataFrame()

# Function to handle nested structures
def normalize_nested_data(df):
    for column in df.columns:
        if isinstance(df[column].iloc[0], dict):
            nested_df = pd.json_normalize(df[column])
            df = df.drop(columns=[column])
            df = pd.concat([df, nested_df], axis=1)
        elif isinstance(df[column].iloc[0], list):
            df_exploded = df[column].explode().dropna().reset_index(drop=True)
            if len(df_exploded) > 0 and isinstance(df_exploded.iloc[0], dict):
                nested_df = pd.json_normalize(df_exploded)
                df = df.drop(columns=[column])
                df = pd.concat([df, nested_df], axis=1)
    return df

def check_duplicate_columns(df):
    duplicates = df.columns[df.columns.duplicated()].unique()
    if len(duplicates) > 0:
        pass
    return duplicates

def rename_duplicate_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique(): 
        cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
    df.columns = cols
    return df


count = 1


def process_appointment(appointment):
	global count

	print(count)
	count += 1

	cookies = {
    'statsigStableId': '477af838-cc81-4f93-9d88-dbce466d0236',
    '__cf_bm': 'PNHy10im3k0KCzHrN8t8s6sCwItrokXwMSr65ssmbsY-1727597780-1.0.1.1-wJizfeeqs.ZTyoYFtcGce.qv29SMhnLXOy0eHPNKa92GlQ3x3bDFdEnWwI8oz86c.nmdzRcMc05RAlqrXsQYZA',
    'c24-city': 'noida',
    'user_selected_city': '134',
    'pincode': '201301'}

	headers = {
	    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
	    'accept-language': 'en-US,en;q=0.7',
	    'cache-control': 'max-age=0',
	    # 'cookie': 'statsigStableId=477af838-cc81-4f93-9d88-dbce466d0236; __cf_bm=PNHy10im3k0KCzHrN8t8s6sCwItrokXwMSr65ssmbsY-1727597780-1.0.1.1-wJizfeeqs.ZTyoYFtcGce.qv29SMhnLXOy0eHPNKa92GlQ3x3bDFdEnWwI8oz86c.nmdzRcMc05RAlqrXsQYZA; c24-city=noida; user_selected_city=134; pincode=201301',
	    'priority': 'u=0, i',
	    'sec-ch-ua': '"Brave";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
	    'sec-ch-ua-mobile': '?0',
	    'sec-ch-ua-platform': '"Windows"',
	    'sec-fetch-dest': 'document',
	    'sec-fetch-mode': 'navigate',
	    'sec-fetch-site': 'none',
	    'sec-fetch-user': '?1',
	    'sec-gpc': '1',
	    'upgrade-insecure-requests': '1',
	    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
	}

	try:
		response = requests.get(
		    f'https://www.cars24.com/buy-used-honda-city-2023-cars-new-delhi-{appointment}/',
		    cookies=cookies,
		    headers=headers,
		)


		soup = BeautifulSoup(response.content, 'html.parser')
		script = soup.find('script', text=lambda t: t and '__PRELOADED_STATE__' in t)

		js_content = script.string.strip()
		start_idx = js_content.find('{')
		end_idx = js_content.rfind('}') + 1
		json_content = js_content[start_idx:end_idx]

		data = json.loads(json_content)
		car_details_data = data.get('carDetails', [])
		car_details_df = pd.json_normalize(car_details_data)

		normalized_df = normalize_nested_data(car_details_df)
		duplicates = check_duplicate_columns(normalized_df)
		if len(duplicates) > 0:
		    normalized_df = rename_duplicate_columns(normalized_df)
		            

		normalized_df = normalized_df[['content.appointmentId','content.make','content.model','content.variant','content.year','content.transmission','content.bodyType','content.fuelType','content.ownerNumber','content.odometerReading','content.cityRto','content.registrationNumber','content.listingPrice','content.onRoadPrice','content.fitnessUpto','content.insuranceType','content.insuranceExpiry','content.lastServicedAt','content.duplicateKey','content.city','label_11','value_6']]

		filtered_df = normalized_df.dropna(subset=['label_11'])
		pivot_df = filtered_df.pivot(index='content.appointmentId', columns='label_11', values='value_6')
		pivot_df.reset_index(inplace=True)
		pivot_df = pivot_df.fillna(method='ffill').fillna(method='bfill')
		pivot_df = pivot_df.groupby('content.appointmentId').first().reset_index()

		#other columns which got deleted in pivot_df
		info_data_df = normalized_df.loc[:, 'content.appointmentId':'content.city'].dropna()

		#merging info_data_df & pivot_df
		final_df = pd.merge(info_data_df, pivot_df, on='content.appointmentId', how='outer')
		final_df['content.insuranceExpiry'] = pd.to_datetime(final_df['content.insuranceExpiry'], unit='s')

		return final_df

	except Exception as e:
		return pd.DataFrame()



#Use ThreadPoolExecutor to process appointments concurrently
# max_workers = os.cpu_count()
max_workers = 10

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_appointment = {executor.submit(process_appointment, appointment): appointment for appointment in appointmentIds}
    
    for future in as_completed(future_to_appointment):
        appointment = future_to_appointment[future]
        try:
            df = future.result()
                        
            if not df.empty:
                final_df = pd.concat([final_df, df], ignore_index=True)
        except Exception as e:
            print(f"An error occurred while processing appointmentId {appointment}: {e}")

final_df.to_excel('cars24_final_data.xlsx', index=False)

import pdb;pdb.set_trace()