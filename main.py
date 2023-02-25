import psycopg2
import pandas
import numpy as np


def connect_db():
    """
    Connects to the remote database.
    :return: conn: the database connection
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host="footy-db.cid6owlghdgm.us-west-2.rds.amazonaws.com",
            database="initial_db",
            user="postgres",
            password="TangoMango2")
        print("Connected to database.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return conn


def create_tables(conn):
    """
    Creates tables for the database. Should only be called once.

    :param conn: the database connection
    :return: 0 on success
    """
    match_query = 'CREATE TABLE Match(' \
                  'match_id INT NOT NULL,' \
                  'date DATE,' \
                  'score VARCHAR(10),' \
                  'competition VARCHAR(40),' \
                  'attendance INT,' \
                  'PRIMARY KEY(match_id)' \
                  ')'

    team_query = 'CREATE TABLE Team(' \
                 'team_name VARCHAR(255) NOT NULL,' \
                 'PRIMARY KEY(team_name)' \
                 ')'

    team_match_stats_query = 'CREATE TABLE Team_Match_Stats(' \
                             'match_id INT NOT NULL,' \
                             'team_name VARCHAR(255) NOT NULL,' \
                             'is_home BOOLEAN,' \
                             'formation VARCHAR(50),' \
                             'possession INT,' \
                             'shots INT,' \
                             'shots_on_goal INT,' \
                             'fouls INT,' \
                             'corners INT,' \
                             'offside INT,' \
                             'PRIMARY KEY(match_id, team_name),' \
                             'FOREIGN KEY(match_id) REFERENCES Match(match_id),' \
                             'FOREIGN KEY(team_name) REFERENCES Team(team_name)' \
                             ')'

    cur = conn.cursor()
    cur.execute(match_query)
    cur.execute(team_query)
    cur.execute(team_match_stats_query)
    conn.commit()
    status = 0

    return status


def parse_csv_and_update_db(filename, conn):
    """
    Parses through a csv file with data from https://football-lineups.com that contains
    all matches for a given team (who's team_name is the filename). Data from each match is then
    uploaded to the database line by line.

    :param filename: the csv file to parse for the given team who's team_name is the filename
    :param conn: the database connection
    :return:
    """

    # open file
    raw_data = pandas.read_csv(filename, encoding='unicode_escape')
    # TODO: Deal with matches that went to pens (score of pens goes to next row with everything else blank)
    print(raw_data)
    current_month_year = ''
    # for each line
    for match in raw_data.values.tolist():
        # GET DATE
        date = str(match[0])
        print(date)
        # if date == nan, set to None
        if date == 'nan':
            date = 'got'
        # else if full date given, update current_month_year
        elif len(date) > 2:
            current_month_year = date[-7:]
        # else if short date, append current_month_year
        elif len(date) > 0:
            date = date + current_month_year
        # else, set to None
        else:
            date = None
        # GET IS_HOME, OPPONENT, COMPETITION

        # GET SCORE
        # ensure format is 'X-X'

        # GET REMAINING STATS FOR THIS TEAM


def main():
    """
    Main function to load the database.
    Connects to the database, processes the files, extracts data, forms queries, and sends queries to the database.
    """
    # connect to database
    conn = connect_db()
    cur = conn.cursor()
    cur.execute('SELECT * FROM team')

    # TODO: Add team 'Man.Utd' to Team table and parse thru their data and add to database

    results = cur.fetchall()
    conn.commit()
    parse_csv_and_update_db('Man.Utd.csv', conn)


if __name__ == '__main__':
    main()

