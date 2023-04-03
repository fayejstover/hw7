# Your name: Faye Stover
# Your student id: 5833 6219
# Your email: fstover@umich.edu
# List who you have worked with on this project: n/a

import unittest
import sqlite3
import json
import os

def read_data(filename):
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path)
    file_data = f.read()
    f.close()
    json_data = json.loads(file_data)
    return json_data

def open_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def make_positions_table(data, cur, conn):
    positions = []
    for player in data['squad']:
        position = player['position']
        if position not in positions:
            positions.append(position)
            cur.execute("CREATE TABLE IF NOT EXISTS Positions (id INTEGER PRIMARY KEY, position TEXT UNIQUE)")
            for i in range(len(positions)):
                cur.execute("INSERT OR IGNORE INTO Positions (id, position) VALUES (?,?)",(i, positions[i]))
            conn.commit()
            
            
## [TASK 1]: 25 points
# Finish the function make_players_table
# This function takes 3 arguments: JSON data,
# the database cursor, and the database connection object
# It iterates through the JSON data to get a list of players in the squad
# and loads them into a database table called 'Players'
# with the following columns:
# id ((datatype: int; Primary key) - note this comes from the JSON
# name (datatype: text)
# position_id (datatype: integer)
# birthyear (datatype: int)
# nationality (datatype: text)
# To find the position_id for each player, you will have to look up
# the position in the Positions table we
# created for you -- see make_positions_table above for details.

def make_players_table(data, cur, conn):
    # Create Positions table
    cur.execute('CREATE TABLE IF NOT EXISTS Positions (id INTEGER PRIMARY KEY, name TEXT, position TEXT UNIQUE)')
    positions = ["Goalkeeper", "Defender", "Midfielder", "Forward"]
    
    for i, position in enumerate(positions):
        cur.execute("INSERT INTO Positions (id, name, position) VALUES (?, ?, ?)", (i+1, position, position))

    conn.commit()

    # Create Players table
    cur.execute('CREATE TABLE IF NOT EXISTS Players (id INTEGER PRIMARY KEY, name TEXT, position_id INTEGER, birthyear INTEGER, nationality TEXT)')
    players = data['squad']
    
    for player in players:
        birthdate = player['dateOfBirth']
        birthyear = int(birthdate[:4])
        age = 2023 - birthyear
        name = player['name']
        nationality = player['nationality']
        position_name = player['position']
        cur.execute("SELECT id FROM Positions WHERE name=?", (position_name,))
        position_id = cur.fetchone()[0]
        cur.execute("INSERT INTO Players (id, name, position_id, birthyear, nationality) VALUES (?, ?, ?, ?, ?)", (player['id'], name, position_id, birthyear, nationality))
   
    conn.commit()


## [TASK 2]: 10 points
# Finish the function nationality_search
# This function takes 3 arguments as input: a list of countries,
# the database cursor, and database connection object.
# It selects all the players from any of the countries in the list
# and returns a list of tuples. Each tuple contains:
# the player's name, their position_id, and their nationality.


def nationality_search(countries, cur, conn):
    results = []
    
    for country in countries:
        cur.execute("SELECT name, position_id, nationality FROM Players WHERE nationality=?", (country,))
        rows = cur.fetchall()
        results.extend(rows)
    
    return results


def birthyear_nationality_search(age, country, cur, conn):
    birth_year = 2023 - age
    cur.execute('''SELECT Players.name, Players.nationality, Players.birthyear FROM Players
                JOIN Positions ON Players.position_id = Positions.id
                WHERE Players.birthyear < ?
                AND Players.nationality = ?''', (birth_year, country,))
    result = cur.fetchall()
    return result

## [TASK 3]: 10 points
# finish the function birthyear_nationality_search
# This function takes 4 arguments as input:
# an age in years (int),
# a country (string), the database cursor,
# and the database connection object.
# It selects all the players from the country passed to the function
# that were born BEFORE (2023 minus the year passed)
# for example: if we pass 19 for the year, it should return
# players with birthdates BEFORE 2004
# This function returns a list of tuples each containing
# the players name, position, and birth year.
# HINT: You'll have to use JOIN for this task.

def position_birth_search(position, age, cur, conn):
    year = 2023 - age
    cur.execute("""
        SELECT Players.name, Positions.name, Players.birth_year
        FROM Players
        JOIN Positions
        ON Players.position_id = Positions.id
        WHERE Positions.name = ? AND Players.birth_year > ?
        """, (position, year))
    result = cur.fetchall()
    return result


# [EXTRA CREDIT]
# YouSeasons' with the following columns:
# id (datatype: int; Primary key) - note this comes from the JSON
# winner_id (datatype: text)
# end_year (datatype: int)
# NOTE: Skip seasons with no winner!
# To find the winner_id for each season, you will have to
# look up the winner's name in the Winners table
# see make_winners_table above for details
# The third function takes in a year (string), the database cursor,
# and the database connection object. It returns a dictionary of how many
# times each team has won the Premier League since the passed year.
# In the dict, each winning team's (full) name is a key,
# and the value associated with each team is the number of times
# they have won since the year passed, including the season that ended
# the passed year.


def make_winners_table(data, cur, conn):
    """
    Creates a table named 'Winners' with 2 columns: id (datatype: int; Primary key) and name (datatype: text).
    Inserts data from the JSON into the table.
    """
    cur.execute('''CREATE TABLE Winners
                    (id INT PRIMARY KEY,
                    name TEXT)''')
    for team in data['teams']:
        cur.execute("INSERT INTO Winners (id, name) VALUES (?, ?)", (team['id'], team['name'])) 
    conn.commit()
    
def make_seasons_table(data, cur, conn):
    """
    Creates a table named 'Seasons' with 3 columns: id (datatype: int; Primary key), winner_id (datatype: text), and end_year (datatype: int).
    Inserts data from the JSON into the table.
    """
    cur.execute('''CREATE TABLE Seasons
                    (id INT PRIMARY KEY,
                    winner_id TEXT,
                    end_year INT)''')
    for season in data['seasons'][:-1]:
        if 'winner' in season:
            winner_id = cur.execute("SELECT id FROM Winners WHERE name=?", (season['winner'],)).fetchone()
            if winner_id is not None:
                cur.execute("INSERT INTO Seasons (id, winner_id, end_year) VALUES (?, ?, ?)", (season['id'], winner_id[0], season['yearEnd']))
    conn.commit()
    
def winners_since_search(year, cur, conn):
    """
    Takes in a year (string), the database cursor, and the database connection object.
    Returns a dictionary of how many times each team has won the Premier League since the passed year.
    In the dict, each winning team's (full) name is a key, and the value associated with each team is the number of times they have won since the year passed, including the season that ended the passed year.
    """
    year = int(year)
    winners_dict = {}
    for row in cur.execute('''SELECT Winners.name, COUNT(*) as count
                                FROM Seasons
                                INNER JOIN Winners ON Seasons.winner_id=Winners.id
                                WHERE end_year>=?
                                GROUP BY Winners.name''', (year,)):
        winners_dict[row[0]] = row[1]
    return winners_dict


def make_winners_table(data, cur, conn):

    cur.execute('''CREATE TABLE Winners
                    (id INT PRIMARY KEY,
                    name TEXT)''')
    for team in data['teams']:
        cur.execute("INSERT INTO Winners (id, name) VALUES (?, ?)", (team['id'], team['name'])) 
    conn.commit()
    
def make_seasons_table(data, cur, conn):

    cur.execute('''CREATE TABLE Seasons
                    (id INT PRIMARY KEY,
                    winner_id TEXT,
                    end_year INT)''')
    for season in data['seasons'][:-1]:
        if 'winner' in season:
            winner_id = cur.execute("SELECT id FROM Winners WHERE name=?", (season['winner'],)).fetchone()
            if winner_id is not None:
                cur.execute("INSERT INTO Seasons (id, winner_id, end_year) VALUES (?, ?, ?)", (season['id'], winner_id[0], season['yearEnd']))
    conn.commit()
    
def winners_since_search(year, cur, conn):

    year = int(year)
    winners_dict = {}
    for row in cur.execute('''SELECT Winners.name, COUNT(*) as count
                                FROM Seasons
                                INNER JOIN Winners ON Seasons.winner_id=Winners.id
                                WHERE end_year>=?
                                GROUP BY Winners.name''', (year,)):
        winners_dict[row[0]] = row[1]
    return winners_dict



class TestAllMethods(unittest.TestCase):
    def setUp(self):
        path = os.path.dirname(os.path.abspath(__file__))
        self.conn = sqlite3.connect(path+'/'+'Football.db')
        self.cur = self.conn.cursor()
        self.conn2 = sqlite3.connect(path+'/'+'Football_seasons.db')
        self.cur2 = self.conn2.cursor()
        
    def test_players_table(self):
        self.cur.execute('SELECT * from Players')
        players_list = self.cur.fetchall()
        self.assertEqual(len(players_list), 30)
        self.assertEqual(len(players_list[0]),5)
        self.assertIs(type(players_list[0][0]), int)
        self.assertIs(type(players_list[0][1]), str)
        self.assertIs(type(players_list[0][2]), int)
        self.assertIs(type(players_list[0][3]), int)
        self.assertIs(type(players_list[0][4]), str)
        
    def test_nationality_search(self):
        x = sorted(nationality_search(['England'], self.cur,
        self.conn))
        self.assertEqual(len(x), 11)
        self.assertEqual(len(x[0]), 3)
        self.assertEqual(x[0][0], "Aaron Wan-Bissaka")
        y = sorted(nationality_search(['Brazil'], self.cur,
        self.conn))
        self.assertEqual(len(y), 3)
        self.assertEqual(y[2],('Fred', 2, 'Brazil'))
        self.assertEqual(y[0][1], 3)
        
    def test_birthyear_nationality_search(self):
        a = birthyear_nationality_search(24, 'England', self.cur,
        self.conn)
        self.assertEqual(len(a), 7)
        self.assertEqual(a[0][1], 'England')
        self.assertEqual(a[3][2], 1992)
        self.assertEqual(len(a[1]), 3)
        b = sorted(position_birth_search('Goalkeeper', 35,
        self.cur, self.conn))
        self.assertEqual(len(b), 2)
        self.assertEqual(type(b[0][0]), str)
        self.assertEqual(type(b[1][1]), str)
        self.assertEqual(len(b[1]), 3)
        self.assertEqual(b[1], ('Jack Butland', 'Goalkeeper',
        1993))
        c = sorted(position_birth_search("Defence", 23, self.cur,
        self.conn))
        self.assertEqual(len(c), 1)
        self.assertEqual(c, [('Teden Mengi', 'Defence', 2002)])
        
        
    # test extra credit
    def test_make_winners_table(self):
        self.cur2.execute('SELECT * from Winners')
        winners_list = self.cur2.fetchall()

    
    def test_make_seasons_table(self):
        self.cur2.execute('SELECT * from Seasons')
        seasons_list = self.cur2.fetchall()

    def test_winners_since_search(self):
        pass
    
    
    def main():
    #### FEEL FREE TO USE THIS SPACE TO TEST OUT YOUR FUNCTIONS
    
        json_data = read_data('football.json')
        cur, conn = open_database('Football.db')
        make_positions_table(json_data, cur, conn)
        make_players_table(json_data, cur, conn)
        conn.close()
        seasons_json_data = read_data('football_PL.json')
        cur2, conn2 = open_database('Football_seasons.db')
        make_winners_table(seasons_json_data, cur2, conn2)
        make_seasons_table(seasons_json_data, cur2, conn2)
        conn2.close()
        
    if __name__ == "__main__":
        main()
    unittest.main(verbosity = 2)
