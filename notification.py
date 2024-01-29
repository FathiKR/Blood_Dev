import pandas as pd
import warnings
import requests
import datetime
warnings.filterwarnings("ignore", category=RuntimeWarning)
### FUNCTIONS

def send_message(token, chat_id, text):
    message = 'https://api.telegram.org/bot'+token+ "/sendMessage?chat_id="+ chat_id+"&text="+text
    requests.get(message)

check_master = pd.read_parquet("df_facility_master.parquet.gzip")
data_date = str(datetime.date.today()-datetime.timedelta(days = 1))


if __name__ == "__main__":

    if (check_master == data_date).any().any():
        
        print(f"Data date {data_date} exists in the DataFrame! Master already updated!")
        bot_token = "6436719893:AAG_L3y89L8m7zCzRr9z8PH-7UxHP2Mw4TI"
        chat_id = "-4149818320"
        message = 'New data has been updated! Check the dashboard for the latest overview. https://my-blood-donation-dashboard-fkr.streamlit.app/'
        
        print("######## Sending the text ##############")
        send_message(bot_token, chat_id, text = message)

    else:       
        print(f"Data date {data_date} not exists in the DataFrame! No data updated!")
        bot_token = "6436719893:AAG_L3y89L8m7zCzRr9z8PH-7UxHP2Mw4TI"
        chat_id = "-4149818320"
        message = 'Data has not update! Will debug on this matter! Sorry for your inconvenience!'
        
        print("######## Sending the text ##############")
        send_message(bot_token, chat_id, text = message)

