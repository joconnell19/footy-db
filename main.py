import datetime

import psycopg2
import pandas
import numpy as np
import math

import pyodbc


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
    # fix pen score issue
    raw_data = fix_penalty_scores(raw_data)

    # TODO: upload this team (filename) to database
    team_name = filename[:-4]
    # upload_team(conn, team_name)

    current_month_year = ''
    # for each match, extract stats, upload to db
    for match in raw_data.values.tolist():
        # GET DATE
        date, current_month_year = format_date(match[0], current_month_year)

        if date is not None:
            # GET IS_HOME, OPPONENT, COMPETITION
            is_home = match[1]
            if is_home == 'home':
                is_home = True
            else:
                is_home = False
            opponent = match[2]
            competition = match[3]

            # GET SCORE
            # ensure format is 'X-X'
            if type(match[4]) == float:
                score = None
            else:
                score = match[4].replace('/', '-')

            # GET REMAINING STATS FOR THIS TEAM
            if type(match[6]) == float:
                formation = None
            else:
                formation = match[6][4:]

            shots = convert_to_int(match[9])
            shots_on_goal = convert_to_int(match[10])
            fouls = convert_to_int(match[11])
            corners = convert_to_int(match[12])
            offside = convert_to_int(match[13])
            possession = convert_to_int(match[14])
            attendance = convert_to_int(match[15])

            # TODO: UPLOAD TO DB
            match_id = upload_match(conn, date, score, competition, attendance)
            # upload_team(conn, opponent)
            # upload_team_match_stats(conn, match_id, team_name, is_home, formation,
            # possession, shots, shots_on_goal, fouls, corners, offside)


def fix_penalty_scores(raw_data):
    """
    Iterates through each match and makes sure that the score of penalties (if applicable), is copied from the
    proceeding row and appended to the relevant match's score.

    :param raw_data: pandas dataframe of all matches
    :return: raw_data updated with fixed penalty scores
    """

    for match in raw_data.values.tolist():
        # if no date
        if type(match[0]) != str:
            if math.isnan(match[0]):
                # get score
                score = match[4]
                # if score has '('
                if score.__contains__('('):
                    # append to prev score
                    raw_data['score'][np.where(raw_data['score'] == score)[0] - 1] = \
                        raw_data['score'][np.where(raw_data['score'] == score)[0] - 1] + ' ' + score

    for match in raw_data.values.tolist():
        score = str(match[4])
        if score.count('(') > 1:
            raw_data['score'][np.where(raw_data['score'] == score)[0]] = score[:score.rfind('(')]

    return raw_data


def format_date(raw_date, current_month_year):
    """
    Converts a raw date from the CSV file into a proper datetime.date() object.

    :param raw_date: from the CSV file
    :param current_month_year: the month and year of this iteration of the parse_csv_and_update_db() loop
    :return: formatted date and the updated current month year if applicable
    """
    date = None
    # ENSURE WE HAVE DAY MONTH AND YEAR
    # if date == nan, set to None
    if type(raw_date) == float:
        date = None
    # else if full date given, update current_month_year
    elif len(raw_date) > 2:
        current_month_year = raw_date[-7:]
    # else if short date, append current_month_year
    elif len(raw_date) > 0:
        date = raw_date + current_month_year

    if date is not None:
        # ENSURE WE HAVE 4 DIGIT YEAR
        if date.index('-') == 1:
            date = '0' + date
        year = date[-2:]
        if int(year) <= int((str(datetime.date.today().year))[-2:]):
            year = '20' + year
        else:
            year = '19' + year
        date = date[0:-2] + year

        # CONVERT TO DATETIME DATE
        date = datetime.datetime.strptime(date, '%d-%b-%Y').date()

    return date, current_month_year


def convert_to_int(stat):
    """
    Converts a given stat to an int if numeric, else None
    :param stat: a stat from the CSV file
    :return: an int if numeric, else None
    """
    if stat.isnumeric():
        stat = int(stat)
    else:
        stat = None

    return stat


def upload_match(conn, date, score, competition, attendance):
    """
    Uploads a match to the database with the given params and the next consecutively available match id.

    :param conn: the database connection
    :param date: the date of the match as a datetime.date()
    :param score: the score e.g. '0-0'
    :param competition: the competition of the match as a str
    :param attendance: the match attendance as an int
    :return:
    """

    try:
        cur = conn.cursor()
        cur.execute('SELECT max(match_id) FROM Match')
        response = cur.fetchall()[0][0]
        match_id = 1
        if response is not None:
            match_id = response + 1

        cur.execute("INSERT INTO Match (match_id, date, score, competition, attendance)"
                    "VALUES (%s, %s, %s, %s, %s);", (match_id, date, score, competition, attendance))
        conn.commit()
        print('UPLOADED TO DATABASE!:', match_id, date, score, competition, attendance)

    except pyodbc.Error as err:
        print(err)
    except psycopg2.DatabaseError as db_err:
        print(db_err)

    return match_id


def main():
    """
    Main function to load the database.
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
