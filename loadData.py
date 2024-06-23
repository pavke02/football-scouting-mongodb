import pandas as pd
import json
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Directory containing CSV files
data_dir = './data/'

# Helper function to parse date strings in the format 'YYYY-MM-DD'
def parse_date(date_str):
    try:
        return pd.to_datetime(date_str, format='%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        return ''
    
def safe_int(value):
    try:
        if pd.isna(value):
            return 0
        return int(value)
    except ValueError:
        return 0

# Function to safely convert values to floats
def safe_float(value):
    try:
        if pd.isna(value):
            return 0
        return float(value)
    except ValueError:
        return 0

# Function to process players data
def process_players():
    logging.info('Starting to process players data...')
    
    players_df = pd.read_csv(os.path.join(data_dir, 'players.csv'))
    logging.info(f'Read {players_df.shape[0]} rows from players.csv')

    player_valuations_df = pd.read_csv(os.path.join(data_dir, 'player_valuations.csv'))
    logging.info(f'Read {player_valuations_df.shape[0]} rows from player_valuations.csv')

    clubs_df = pd.read_csv(os.path.join(data_dir, 'clubs.csv'))
    logging.info(f'Read {clubs_df.shape[0]} rows from clubs.csv')

    appearances_df = pd.read_csv(os.path.join(data_dir, 'appearances.csv'))
    logging.info(f'Read {appearances_df.shape[0]} rows from appearances.csv')

    players = []
    for index, row in players_df.iterrows():
        player_id = str(row['player_id'])
        player_valuations = player_valuations_df[player_valuations_df['player_id'] == int(player_id)]
        valuations = [
            {
                'date': parse_date(val_row['date']),
                'market_value_in_eur': int(val_row['market_value_in_eur'])
            }
            for _, val_row in player_valuations.iterrows()
        ]
        
        current_club = clubs_df[clubs_df['club_id'] == row['current_club_id']].iloc[0] if not clubs_df[clubs_df['club_id'] == row['current_club_id']].empty else None
        
        player = {
            'player_id': player_id,
            'first_name': row['first_name'],
            'last_name': row['last_name'],
            'name': row['name'],
            'date_of_birth': parse_date(row['date_of_birth']),
            'position': row['position'],
            'sub_position': row['sub_position'],
            'foot': row['foot'],
            'height_in_cm': row['height_in_cm'],
            'current_club': {
                'club_id': str(current_club['club_id']),
                'name': current_club['name'],
                'domestic_competition_id': str(current_club['domestic_competition_id'])
            } if current_club is not None else None,
            'valuations': valuations,
            'stats': {
                'appearances': int(appearances_df[appearances_df['player_id'] == int(player_id)].shape[0]),
                'goals': int(appearances_df[appearances_df['player_id'] == int(player_id)]['goals'].sum()),
                'assists': int(appearances_df[appearances_df['player_id'] == int(player_id)]['assists'].sum()),
                'yellow_cards': int(appearances_df[appearances_df['player_id'] == int(player_id)]['yellow_cards'].sum()),
                'red_cards': int(appearances_df[appearances_df['player_id'] == int(player_id)]['red_cards'].sum())
            }
        }
        players.append(player)
        
        if index % 1000 == 999:
            logging.info(f'Processed {index + 1} players')
    
    with open('players_v2.json', 'w') as f:
        json.dump(players, f, indent=4)
        logging.info('Saved players data to players.json')

def process_games():
    logging.info('Starting to process games data...')
    
    games_df = pd.read_csv(os.path.join(data_dir, 'games.csv'))
    logging.info(f'Read {games_df.shape[0]} rows from games.csv')

    clubs_df = pd.read_csv(os.path.join(data_dir, 'clubs.csv'))
    logging.info(f'Read {clubs_df.shape[0]} rows from clubs.csv')

    game_events_df = pd.read_csv(os.path.join(data_dir, 'game_events.csv'))
    logging.info(f'Read {game_events_df.shape[0]} rows from game_events.csv')

    game_lineups_df = pd.read_csv(os.path.join(data_dir, 'game_lineups.csv'))
    logging.info(f'Read {game_lineups_df.shape[0]} rows from game_lineups.csv')

    club_games_df = pd.read_csv(os.path.join(data_dir, 'club_games.csv'))
    logging.info(f'Read {club_games_df.shape[0]} rows from club_games.csv')

    appearances_df = pd.read_csv(os.path.join(data_dir, 'appearances.csv'))
    logging.info(f'Read {appearances_df.shape[0]} rows from appearances.csv')

    # Merge lineups with appearances to get minutes_played
    game_lineups_df = game_lineups_df.merge(appearances_df[['game_id', 'player_id', 'minutes_played']], on=['game_id', 'player_id'], how='left')

    games = []
    for index, row in games_df.iterrows():
        home_club_row = clubs_df[clubs_df['club_id'] == row['home_club_id']]
        away_club_row = clubs_df[clubs_df['club_id'] == row['away_club_id']]

        if home_club_row.empty:
            logging.warning(f"Skipping game_id {row['game_id']} due to missing home club data (club_id {row['home_club_id']})")
            continue

        if away_club_row.empty:
            logging.warning(f"Skipping game_id {row['game_id']} due to missing away club data (club_id {row['away_club_id']})")
            continue

        home_club = home_club_row.iloc[0]
        away_club = away_club_row.iloc[0]

        game_events = game_events_df[game_events_df['game_id'] == row['game_id']]
        events = []
        for _, event_row in game_events.iterrows():
            event = {
                'event_id': str(event_row['game_event_id']),
                'minute': int(event_row['minute']),
                'type': event_row['type'],
                'club_id': str(event_row['club_id']),
                'player_id': str(event_row['player_id']),
                'description': event_row['description']
            }
            if 'player_assist_id' in event_row and pd.notna(event_row['player_assist_id']):
                event['assist_id'] = str(event_row['player_assist_id'])
            events.append(event)
        
        game_lineups = game_lineups_df[game_lineups_df['game_id'] == row['game_id']]
        lineups = [
            {
                'player_id': str(lineup_row['player_id']),
                'club_id': str(lineup_row['club_id']),
                'player_name': lineup_row['player_name'],
                'position': lineup_row['position'],
                'number': safe_int(lineup_row['number']),
                'team_captain': bool(lineup_row['team_captain']),
                'minutes_played': safe_int(lineup_row['minutes_played'])  # Get minutes played from merged data
            }
            for _, lineup_row in game_lineups.iterrows()
        ]
        
        if not lineups:  # Skip games with empty lineups
            continue
        
        game = {
            'game_id': str(row['game_id']),
            'competition_id': str(row['competition_id']),
            'season': row['season'],
            'round': row['round'],
            'date': parse_date(row['date']),
            'home_club': {
                'club_id': str(home_club['club_id']),
                'name': home_club['name'],
                'goals': int(row['home_club_goals']),
                'position': row['home_club_position'],
                'manager_name': row['home_club_manager_name'],
                'formation': row['home_club_formation'],
                'stadium_name': home_club['stadium_name'],
                'stadium_seats': int(home_club['stadium_seats'])
            },
            'away_club': {
                'club_id': str(away_club['club_id']),
                'name': away_club['name'],
                'goals': int(row['away_club_goals']),
                'position': row['away_club_position'],
                'manager_name': row['away_club_manager_name'],
                'formation': row['away_club_formation']
            },
            'stadium': row['stadium'],
            'attendance': safe_int(row['attendance']),
            'referee': row['referee'],
            'events': events,
            'lineups': lineups,
            'is_win': bool(club_games_df[(club_games_df['game_id'] == row['game_id']) & (club_games_df['club_id'] == row['home_club_id'])]['is_win'].iloc[0]),
            'aggregate': row['aggregate'],
            'competition_type': row['competition_type']
        }
        games.append(game)
        
        if index % 1000 == 0:
            logging.info(f'Processed {index + 1} games')
    
    with open('games_v2.json', 'w') as f:
        json.dump(games, f, indent=4)
        logging.info('Saved games data to games.json')

def process_clubs():
    logging.info('Starting to process clubs data...')

    clubs_df = pd.read_csv(os.path.join(data_dir, 'clubs.csv'))
    logging.info(f'Read {clubs_df.shape[0]} rows from clubs.csv')

    players_df = pd.read_csv(os.path.join(data_dir, 'players.csv'))
    logging.info(f'Read {players_df.shape[0]} rows from players.csv')

    # Fill missing numeric values with 0
    numeric_cols = ['squad_size', 'average_age', 'foreigners_number', 'foreigners_percentage', 'national_team_players', 'stadium_seats']
    clubs_df[numeric_cols] = clubs_df[numeric_cols].apply(pd.to_numeric, errors='coerce').fillna(0)

    clubs = []
    for index, row in clubs_df.iterrows():
        club_players = players_df[players_df['current_club_id'] == row['club_id']]
        players = [
            {'player_id': int(player_row['player_id']), 'name': player_row['name']}
            for _, player_row in club_players.iterrows()
        ]

        club = {
            'club_id': str(row['club_id']),
            'club_code': row['club_code'],
            'name': row['name'],
            'domestic_competition_id': row['domestic_competition_id'],
            'total_market_value': 0,
            'squad_size': int(row['squad_size']),
            'average_age': float(row['average_age']),
            'foreigners_number': int(row['foreigners_number']),
            'foreigners_percentage': float(row['foreigners_percentage']),
            'national_team_players': int(row['national_team_players']),
            'stadium_name': row['stadium_name'],
            'stadium_seats': int(row['stadium_seats']),
            'net_transfer_record': row['net_transfer_record'] if pd.notna(row['net_transfer_record']) else 0,
            'coach_name': row['coach_name'] if 'coach_name' in row else "",
            'last_season': row['last_season'],
            'players': players,
            'url': row['url']
        }
        clubs.append(club)

        if index % 1000 == 0:
            logging.info(f'Processed {index + 1} clubs')

    with open('clubs_v2.json', 'w') as f:
        json.dump(clubs, f, indent=4)
        logging.info('Saved clubs data to clubs.json')

if __name__ == '__main__':
    process_players()
    #process_games()
    #process_clubs()
