from datetime import datetime
import datetime as dt
import glob
import io
import json
import os
import re
import time
from urllib.parse import urljoin
import pandas as pd
import pybaseball
import requests
import urllib3
from bs4 import BeautifulSoup as bs
from selenium import webdriver
# from selenium.common import StaleElementReferenceException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import sqlite3
import logging

os.environ["GH_TOKEN"] = "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"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
pybaseball.cache.enable()
database_path = "data.db"

conn = sqlite3.connect(database_path)
cursor = conn.cursor()


# Create a logger
logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)

# Create a console (stdout) handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and set it for both handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(console_handler)




def get_mlb_json(pk):
    """
    Retrieves game data from either a saved JSON file or the MLB Baseball Savant API
    and returns a dictionary of the relevant game data.

    Args:
        pk (int): A unique identifier for the game.

    Returns:
        dict: A dictionary containing the game data for the specified game.
    """
    # Construct filename and filepath from game identifier
    filename = str(pk) + '.json'
    filepath = os.path.join('gamejson', filename)

    # Check if data for the game is already saved as a JSON file
    if os.path.exists(filepath):
        # If the data is saved, load it from the JSON file
        with open(filepath, 'r') as f:
            js = json.load(f)
    else:
        # If the data is not saved, fetch it from the MLB Baseball Savant API
        pay_url = 'https://baseballsavant.mlb.com/gf?game_pk=%s' % pk
        resp = requests.get(pay_url, verify=False)
        js = json.loads(resp.content)

        with open(filepath, 'w') as json_file:
            json.dump(js, json_file)

def download_games_year(year):
    base_path = os.getcwd()
    base_path = os.path.join(base_path, 'scraping/data/')

    if not os.path.exists(base_path):
        # Create the directory if it doesn't exist
        os.makedirs(base_path)

    # first we check if the filse already exist
    current_year = int(datetime.now().year)
    files = os.listdir(base_path)

    print(f"downloading games from {current_year}")
    for file in files:
        if str(year) in file:
            if current_year != year:
                return os.path.join(base_path, str(year) + '.csv')

    print("re-downloadingg current year")
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": base_path,  # Set download path to current working directory
    })
    try:
        driver = webdriver.Chrome(options=chrome_options)
        # Navigate to the website
        url = "https://baseballsavant.mlb.com/statcast_search?hfPTM=&hfPT=&hfAB=&hfGT=R%7CPO%7C&hfPR=&hfZ=&hfStadium=&hfBBL=&hfNewZones=&hfPull=&hfC=&hfSea={year}%7C&hfSit=&player_type=pitcher&hfOuts=&hfOpponent=&pitcher_throws=&batter_stands=&hfSA=&hfMo=&hfTeam=&home_road=&hfRO=&position=&hfInfield=&hfOutfield=&hfInn=&hfBBT=&hfFlag=&metric_1=&group_by=team-date&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&player_event_sort=api_p_release_speed&sort_order=desc#results"
        driver.get(url.format(year=year))

        # Find the element with the link to download the CSV file
        wait = WebDriverWait(driver, 40)  # 10 seconds is the maximum wait time

        max_retrties = 5
        for _ in range(max_retrties):
            try:
                download_icon = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'table-icon.csv_table')))
                download_icon.click()
                break
            except Exceptions as e :
                print(e)
                continue
        time.sleep(5)
        # Define the new file name
        new_file_name = "{year}.csv"
        # Find the most recent file
        downloads_folder = os.path.expanduser(base_path)
        list_of_files = glob.glob(os.path.join(downloads_folder, "*"))
        latest_file = max(list_of_files, key=os.path.getctime)
        print(latest_file)
        # Rename the downloaded file to the new name
        os.rename(latest_file, os.path.join(base_path, new_file_name.format(year=year)))
        driver.quit()
        print('downloaded year {year}'.format(year=year))
        return os.path.join(base_path, str(year) + '.csv')
    except Exception as e:
        print(e)
        # Close the browser
    finally:
        driver.quit()
    return

def process_game_pk(csv_file):
    # we create a dataframe per year by reading the csv file returned from download_games_year
    # we load those into a temp table z_tmp_game_ids. We upsert from z_tmp_gme_ids to game_ids
    df_yearly = pd.read_csv(open(csv_file, 'rb'))
    df_yearly.to_sql('z_tmp_game_ids', conn, if_exists='replace', index=False)
    #upsert to gameids
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO game_ids SELECT * FROM z_tmp_game_ids')
    conn.commit()
# %%
def get_game_ids(start_dt, end_dt):
    for year in range(2018,2024):
       process_game_pk(download_games_year(year))

# %%
def update_json_inventory(path):
    #we need to get a lit of jsons we've alaready downloaded to gamejson directory
    #we separate extention and filaname, filename to a table in an sqlite database "data.db"
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS json_files (
                id INTEGER PRIMARY KEY,
                filename TEXT,
                extension TEXT
            )
        ''')

    cursor.execute("SELECT filename, extension from json_files")
    existing_files = set((filename, extension) for filename, extension in cursor.fetchall())
    print(len(existing_files))
    for root, dirs, files in os.walk(path):
        for filename in files:
            if filename.endswith('.json'):
                name, ext = os.path.splitext(filename)
                if (name, ext) not in existing_files:
                    try:
                        cursor.execute("INSERT INTO json_files(filename, extension) VALUES (?, ?)", (name, ext))
                        existing_files.add((name, ext))
                    except Exception as e:
                        print("Error inserting file {filename}: {e}")
    conn.commit()
# %%
def download_and_update_json():
    # we query the json_pending_download table and get the total game_pks that need to be downloaded
    # we use the function get_mlb_json(pk_id) to download all json_files
    # we then run the update_json_inventory function to update the downloaded json files

    get_game_ids(2018, 2023)
    cursor.execute("SELECT game_pk FROM json_pending_download")
    game_pks_to_download = cursor.fetchall()
    total_records = len(game_pks_to_download)
    print(f"Games to process: {total_records}")
    if total_records > 2000:
        return
    chunk_size = 50
    for i in range(0, total_records, chunk_size):
        chunk = game_pks_to_download[i:i + chunk_size]
        print(f"Processing records {i+1} to {i+len(chunk)}")
        logging.info(f"Processing records {i+1} to {i+len(chunk)}")
        for game_pk in chunk:
            get_mlb_json(game_pk[0])


    update_json_inventory('gamejson')

    return True

#download_and_update_json()
#conn.close()
