import pandas as pd
import numpy as np
import datetime
import os


#functions

def state_data_processing(df_ndonor, df_donation):

    #merge
    df_state_master = pd.merge(df_donation, df_ndonor, how ="left", left_on= ["date","state","donations_new"], right_on = ["date","state", "total"])

    #checking null in merge + getting the target column to fix
    if df_state_master.isna().sum().sum() > 0:
        target_column = []
        for column, total_na in df_state_master.isna().sum().items():
            
            if total_na > 0:
            
                target_column.append(column)
            
            else:
                print(f"This column: {column} don't have missing value!")
    
    
        #get the rows that null
        null_mask = df_state_master.isnull().any(axis=1)
        null_rows = df_state_master[null_mask]
        print(f"Total {len(null_rows)} rows need to fix!")

        #get the original data to fix the null
        for i in range(len(null_rows)):
            target = df_ndonor.loc[null_rows.index[i],target_column].to_dict()
            df_state_master.loc[null_rows.index[i], target_column] =  target

        print("Finish Processed State Master!")
        return df_state_master

    else:
        print(f"No missing value today!")
        return df_state_master
    
def facility_data_processing(df_ndonor, df_donation):

    #merge
    df_facility_master = pd.merge(df_donation, df_ndonor, how ="left", left_on= ["date","hospital","donations_new"], right_on = ["date","hospital", "total"])

    #checking null in merge + getting the target column to fix
    if df_facility_master.isna().sum().sum() > 0:
        target_column = []
        for column, total_na in df_facility_master.isna().sum().items():
            
            if total_na > 0:
            
                target_column.append(column)
            
            else:
                print(f"This column: {column} don't have missing value!")
    
    
        #get the rows that null
        null_mask = df_facility_master.isnull().any(axis=1)
        null_rows = df_facility_master[null_mask]
        print(f"Total {len(null_rows)} rows need to fix!")

        #get the original data to fix the null
        for i in range(len(null_rows)):
            target = df_ndonor.loc[null_rows.index[i],target_column].to_dict()
            df_facility_master.loc[null_rows.index[i], target_column] =  target
        
        print("Finish Processed Facility Master!")
        return df_facility_master

    else:
        print(f"No missing value today!")
        return df_facility_master

def age(visit_date, birth_date):
    # visit_date = int(visit_date)
    # birth_date = int(birth_date)
    return visit_date.year - birth_date.year

datetime_now = datetime.datetime.now()

#Code running as schedulled
print(f"Data Processing Code Run at {datetime_now}")

#source URL  
url_donation_facility = "https://raw.githubusercontent.com/MoH-Malaysia/data-darah-public/main/donations_facility.csv"
url_donation_state = "https://raw.githubusercontent.com/MoH-Malaysia/data-darah-public/main/donations_state.csv"
url_newdonor_facility = "https://raw.githubusercontent.com/MoH-Malaysia/data-darah-public/main/newdonors_facility.csv"
url_newdonor_state = "https://raw.githubusercontent.com/MoH-Malaysia/data-darah-public/main/newdonors_state.csv"

url_donorid = "https://dub.sh/ds-data-granular"

#Get the latest data form GitHub

print("Read latest data from GitHub!")
df_ndonor_fac = pd.read_csv(url_newdonor_facility)

df_ndonor_state = pd.read_csv(url_newdonor_state)

df_donation_fac = pd.read_csv(url_donation_facility)

df_donation_state = pd.read_csv(url_donation_state)

df_id = pd.read_parquet(url_donorid, engine="fastparquet")

print("Successfully read all data!")


data_date = str(datetime.date.today()-datetime.timedelta(days = 1))
date_now = pd.Timestamp.now()

# print(f"Filter df_donor_fac date to {data_date}!")

# #Filter only those current date
# df_ndonor_fac = df_ndonor_fac[df_ndonor_fac["date"] == data_date]

# df_ndonor_state = df_ndonor_state[df_ndonor_state["date"] == data_date]

# df_donation_state = df_donation_state[df_donation_state["date"] == data_date]

# df_donation_fac = df_donation_fac[df_donation_fac["date"] == data_date]

# df_id = df_id[df_id["vist_date"] == data_date]


state_master_path = os.getcwd()+"/state_master.parquet.gzip"
facility_master_path = os.getcwd()+"/facility_master.parquet.gzip"

if os.path.exists(state_master_path) & os.path.exists(facility_master_path):
    
    print("File exists!")

    #Filter only those current date
    df_ndonor_fac = df_ndonor_fac[df_ndonor_fac["date"] == data_date]

    df_ndonor_state = df_ndonor_state[df_ndonor_state["date"] == data_date]

    df_donation_state = df_donation_state[df_donation_state["date"] == data_date]

    df_donation_fac = df_donation_fac[df_donation_fac["date"] == data_date]

    print("Creating New State Master Dataset")
    df_state_master_new = state_data_processing(df_donation= df_donation_state, df_ndonor= df_ndonor_state)
    
    print("Creating New Facility Master Dataset")
    df_facility_master_new = facility_data_processing(df_donation= df_donation_fac, df_ndonor= df_ndonor_fac)
    
    #create day/momnth/year column state master
    df_state_master_new['day'] = df_state_master_new['date'].dt.day
    df_state_master_new['month'] = df_state_master_new['date'].dt.month
    df_state_master_new['year'] = df_state_master_new['date'].dt.year

    #create day/momnth/year column facility master
    df_facility_master_new['day'] = df_facility_master_new['date'].dt.day
    df_facility_master_new['month'] = df_facility_master_new['date'].dt.month
    df_facility_master_new['year'] = df_facility_master_new['date'].dt.year

    #read the previous master data
    df_state_master_old = pd.read_parquet("df_state_master.parquet.gzip")

    df_facility_master_old = pd.read_parquet("df_facility_master.parquet.gzip")

    #append new dataset to old
    df_state_master_update = df_state_master_old.append(df_state_master_new, ignore_index=True)

    df_facility_master_update = df_facility_master_old.append(df_facility_master_new, ignore_index=True)

    #Creating Master for Retaining ID

    #standardize format
    df_id["visit_date"] = pd.to_datetime(df_id['visit_date'], format="%Y-%m-%d")
    df_id["birth_date"] = pd.to_datetime(df_id['birth_date'], format="%Y")

    #get the age of each id
    df_id["age"] = df_id.apply(lambda x: age(x['visit_date'], x['birth_date']), axis=1)
    
    #get the vusut difference
    grouped = df_id.groupby(by="donor_id")
    df_id['previous_diff'] = grouped['visit_date'].diff()
    df_id['previous_diff'] = df_id['previous_diff'].fillna(pd.Timedelta('0 days'))

    #create retaining master df
    df_retaining_master = pd.DataFrame(
        {
            'last_donate_date': grouped['visit_date'].max(),
            'last_donate_age': grouped['age'].max(),
            'current_age':  date_now - (grouped['birth_date'].max().dt.year),  # Assuming current year is 2024
            'total_donation': grouped.size(),
            'median_frequency_donor_months': grouped["previous_diff"].median().astype(str)
        }).reset_index()
    
    df_retaining_master['median_frequency_donor_months'] = df_retaining_master['median_frequency_donor_months'].apply(lambda x: x.split(' ')[0]).astype(int)
    
    df_retaining_master['last_donate_age'] = df_retaining_master[df_retaining_master["last_donate_age"]> 0]

    df_state_master_new.to_parquet("df_state_master.parquet.gzip", compression = "gzip")
    df_facility_master_new.to_parquet("df_facility_master.parquet.gzip", compression = "gzip")
    df_retaining_master.to_parquet("df_retain_master.parquet.gzip", compression = "gzip")
    # df_id = df_id[df_id["vist_date"] == data_date]
    #Append the data to the existing one if not exist!

else:
    print("File not exist! Creating Master Dataset!")

    print("Creating State Master Dataset")
    df_state_master = state_data_processing(df_donation= df_donation_state, df_ndonor= df_ndonor_state)
    
    print("Creating Facility Master Dataset")
    df_facility_master = facility_data_processing(df_donation= df_donation_fac, df_ndonor= df_ndonor_fac)

    print("Standardized Date State Master Dataset")
    #change datetime format state master format
    df_state_master["date"] = pd.to_datetime(df_state_master['date'], format="%Y-%m-%d") 
    
    print("Standardized Date Facility Master Dataset")
    #change datetime format facility master format
    df_facility_master["date"] = pd.to_datetime(df_facility_master['date'], format='%Y-%m-%d')
    
    print("Creating Day, Month, Year Columns State & Facility Master Dataset")
    #create day/momnth/year column state master
    df_state_master['day'] = df_state_master['date'].dt.day
    df_state_master['month'] = df_state_master['date'].dt.month
    df_state_master['year'] = df_state_master['date'].dt.year

    #create day/momnth/year column facility master
    df_facility_master['day'] = df_facility_master['date'].dt.day
    df_facility_master['month'] = df_facility_master['date'].dt.month
    df_facility_master['year'] = df_facility_master['date'].dt.year

    #Creating Master for Retaining ID
    print("Creating Retaining ID Master Dataset")
    #standardize format
    df_id["visit_date"] = pd.to_datetime(df_id['visit_date'], format="%Y-%m-%d")
    df_id["birth_date"] = pd.to_datetime(df_id['birth_date'], format="%Y")

    #get the age of each id
    df_id["age"] = df_id.apply(lambda x: age(x['visit_date'], x['birth_date']), axis=1)
    
    #get the visut difference
    grouped = df_id.groupby(by="donor_id")
    df_id['previous_diff'] = grouped['visit_date'].diff()
    df_id['previous_diff'] = df_id['previous_diff'].fillna(pd.Timedelta('0 days'))

    #create retaining master df
    df_retaining_master = pd.DataFrame(
        {
            'last_donate_date': grouped['visit_date'].max(),
            'last_donate_age': grouped['age'].max(),
            'total_donation': grouped.size(),
            'median_frequency_donor_months': grouped["previous_diff"].median().astype(str)
        }).reset_index()
    
    df_retaining_master['median_frequency_donor_months'] = df_retaining_master['median_frequency_donor_months'].apply(lambda x: x.split(' ')[0]).astype(int)

    df_state_master.to_parquet("df_state_master.parquet.gzip", compression = "gzip")
    df_facility_master.to_parquet("df_facility_master.parquet.gzip", compression = "gzip")
    df_retaining_master.to_parquet("df_retain_master.parquet.gzip", compression = "gzip")