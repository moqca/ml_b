#from scraping import scrape as sc
import os
import json
import pandas as pd
import sqlite3

database_path = "..data.db"

conn = sqlite3.connect(database_path)
cursor = conn.cursor()


def get_pitcher_data(js, where):
    """Return a list of dictionaries containing the pitching statistics for a specified team.

    Args:
        js (dict): A JSON object containing the data to be processed.
        where (str): A string representing the team whose data is to be retrieved.

    Returns:
        list: A list of dictionaries containing the pitching statistics for each pitcher on the specified team.
    """

    pitcher_ids = js['boxscore']['teams'][where]['pitchers']
    pitchers = []

    players = js['boxscore']['teams'][where]['players']
    for player_id, values in players.items():
        player_id = values['person']['id']
        if player_id in pitcher_ids:
            p_stats = js['boxscore']['teams'][where]['players']['ID' + str(player_id)]['stats']['pitching']
            p_id = js['boxscore']['teams'][where]['players']['ID' + str(player_id)]['person']
            p_stats['is_home_team'] = True if where == 'home' else False
            p_stats.update(p_id)
            pitchers.append(p_stats)

    return pitchers

def game_summary(js, pk):

    try:
        # Extract relevant game data from JSON object
        home_score = js['scoreboard']['linescore']['teams']['home']['runs']
        away_score = js['scoreboard']['linescore']['teams']['away']['runs']
        home_team = js['home_team_data']['abbreviation']
        away_team = js['away_team_data']['abbreviation']
        game_id = pk
        date = js['game_date']
        away_batting = js['boxscore']['teams']['away']['teamStats']['batting']
        away_pitching = js['boxscore']['teams']['away']['teamStats']['pitching']
        away_fielding = js['boxscore']['teams']['away']['teamStats']['fielding']
        home_batting = js['boxscore']['teams']['home']['teamStats']['batting']
        home_pitching = js['boxscore']['teams']['home']['teamStats']['pitching']
        home_fielding = js['boxscore']['teams']['home']['teamStats']['fielding']

        # Retrieve individual pitcher data for both teams using the get_pitcher_data function
        away_pitchers = get_pitcher_data(js, 'away')
        home_pitchers = get_pitcher_data(js, 'home')
    except Exception as e:
        print('Game not processed: ','' , 'error:', e)
        return pd.DataFrame()

        # Create dictionary containing relevant game data
    game_summary = {
        'game': game_id,
        'date': date,
        'home_team': home_team,
        'away_team': away_team,
        'home_score': home_score,
        'away_score': away_score
    }
    data = {
        'game_summary': game_summary,
        'home_batting': home_batting,
        'away_batting': away_batting,
        'home_pitching': home_pitching,
        'away_pitching': away_pitching,
        'home_fielding': home_fielding,
        'away_fielding': away_fielding,
        'home_pitchers': home_pitchers,
        'away_pitchers': away_pitchers
    }

    return data

def read_json(pk):

    if 'json' in str(pk):
        filename = pk
    else:
        filename = str(pk) + '.json'
    filename = os.path.join('gamejson', filename)

    with open(filename, 'r') as file:
       json_data =  json.loads(file.read())
    return json_data



def get_df_game_summary(game_info):
    game_df = pd.DataFrame([game_info])
    return game_df

def process_game(pk):

   js = game_summary(read_json(pk), pk) #using game_summary as a placeholder

   return js

def process_game_batch(n_batches=2):
    file_list = os.listdir('gamejson/')

    for i in range(0, len(file_list[:2]), n_batches):
        batch_to_process = file_list[i:i+n_batches]
        df_games = pd.DataFrame()
        df_batting = pd.DataFrame()
        df_pitching = pd.DataFrame()
        df_pitchers = pd.DataFrame()
        df_fielding = pd.DataFrame()

        frames = { 'batting': df_batting,
                  'pitching': df_pitching,
                  'pitchers': df_pitchers,
                  'fielding': df_fielding}

        for item in batch_to_process:
            fnam, ext = os.path.splitext(item)
            processed_json = process_game(item)
            df_games = pd.concat([df_games, get_df_game_summary(processed_json['game_summary'])])
            for typ, df in frames.items():
                for where in ['home', 'away']:
                    print(where, typ)
                    frames[typ] = pd.concat([frames[typ], get_df_game_summary(processed_json[where + '_' + typ])])
                    frames[typ]['game_pk'] = fnam
                    frames[typ]['playing'] = where

        print(df_batting)
        print(frames['batting'])
        # df.to_sql('game_summaries', conn, if_exists='append')

process_game_batch(2)
