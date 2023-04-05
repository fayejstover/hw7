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


def make_players_table(json_data, cursor, connection):
    # Load positions from Positions table
    cursor.execute("SELECT * FROM Positions")
    positions = {row[1]: row[0] for row in cursor.fetchall()}

    # Create Players table
    cursor.execute("CREATE TABLE IF NOT EXISTS Players (id INTEGER PRIMARY KEY, name TEXT, position_id INTEGER, birthyear INTEGER, nationality TEXT)")

    # Iterate through the squad list and insert players into Players table
    squad = json_data.get('squad', [])
    for player in squad:
        id = player.get('id')
        name = player.get('name')
        position_name = player.get('position')
        position_id = positions.get(position_name)
        birthdate = player.get('dateOfBirth')
        if birthdate:
            birthyear = int(birthdate.split('-')[0])
        else:
            birthyear = None
        nationality = player.get('nationality')

        # Insert player into Players table
        cursor.execute("INSERT INTO Players (id, name, position_id, birthyear, nationality) VALUES (?, ?, ?, ?, ?)", (id, name, position_id, birthyear, nationality))

    # Commit changes to database
    connection.commit()


## [TASK 2]: 10 points
# Finish the function nationality_search
# This function takes 3 arguments as input: a list of countries,
# the database cursor, and database connection object.
# It selects all the players from any of the countries in the list
# and returns a list of tuples. Each tuple contains:
# the player's name, their position_id, and their nationality.


def nationality_search(countries, cur, conn):
    # Create a string with the list of countries to use in the query
    countries_str = ', '.join(['?' for _ in countries])
    
    # Query the database for players from the given countries
    query = f'''
        SELECT Players.name, Players.position_id, Players.nationality
        FROM Players
        INNER JOIN Countries ON Players.country_id = Countries.id
        WHERE Countries.name IN ({countries_str})
    '''
    cur.execute(query, countries)
    results = cur.fetchall()
    
    return tuple(results)



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
    # Calculate the birth year of the players
    birth_year = 2023 - age
    
    # Create the SQL query
    query = f'''
        SELECT Players.name, Positions.name, strftime('%Y', Players.date_of_birth) as birth_year
        FROM Players
        INNER JOIN Positions ON Players.position_id = Positions.id
        WHERE Positions.name = ? AND birth_year > ?
    '''
    
    # Execute the query and fetch the results
    cur.execute(query, (position, birth_year))
    results = cur.fetchall()
    
    # Return the results as a list of tuples
    return results



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

'''
def make_winners_table(data, cur, conn):
     pass


def make_seasons_table(data, cur, conn):
     pass


def winners_since_search(year, cur, conn):
     pass
'''

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
        x = sorted(nationality_search(['England'], self.cur, self.conn))
        self.assertEqual(len(x), 11)
        self.assertEqual(len(x[0]), 3)
        self.assertEqual(x[0][0], "Aaron Wan-Bissaka")
        y = sorted(nationality_search(['Brazil'], self.cur, self.conn))
        self.assertEqual(len(y), 3)
        self.assertEqual(y[2],('Fred', 2, 'Brazil'))
        self.assertEqual(y[0][1], 3)

    def test_birthyear_nationality_search(self):
        a = birthyear_nationality_search(24, 'England', self.cur, self.conn)
        self.assertEqual(len(a), 7)
        self.assertEqual(a[0][1], 'England')
        self.assertEqual(a[3][2], 1992)
        self.assertEqual(len(a[1]), 3)
        b = sorted(position_birth_search('Goalkeeper', 35, self.cur, self.conn))
        self.assertEqual(len(b), 2)
        self.assertEqual(type(b[0][0]), str)
        self.assertEqual(type(b[1][1]), str)
        self.assertEqual(len(b[1]), 3)
        self.assertEqual(b[1], ('Jack Butland', 'Goalkeeper', 1993))
        c = sorted(position_birth_search("Defence", 23, self.cur, self.conn))
        self.assertEqual(len(c), 1)
        self.assertEqual(c, [('Teden Mengi', 'Defence', 2002)])
'''
    # test extra credit
    def test_make_winners_table(self):
        self.cur2.execute('SELECT * from Winners')
        winners_list = self.cur2.fetchall()
        pass

    def test_make_seasons_table(self):
        self.cur2.execute('SELECT * from Seasons')
        seasons_list = self.cur2.fetchall()
        pass

    def test_winners_since_search(self):
        pass
'''

def main():
    #### FEEL FREE TO USE THIS SPACE TO TEST OUT YOUR FUNCTIONS
    json_data = read_data('football.json')
    cur, conn = open_database('Football.db')
    make_positions_table(json_data, cur, conn)
    make_players_table(json_data, cur, conn)
    conn.close()
    seasons_json_data = read_data('football_PL.json')
    cur2, conn2 = open_database('Football_seasons.db')
    #make_winners_table(seasons_json_data, cur2, conn2)
    #make_seasons_table(seasons_json_data, cur2, conn2)
    conn2.close()

if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)
