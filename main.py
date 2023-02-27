import psycopg2
import pandas
import numpy as np
import math


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
        date = match[0]
        # if date == nan, set to None
        if type(date) == float:
            date = None
        # else if full date given, update current_month_year
        elif len(date) > 2:
            current_month_year = date[-7:]
        # else if short date, append current_month_year
        elif len(date) > 0:
            date = date + current_month_year
        # else, set to None
        else:
            date = None

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

            shots = match[9]
            shots_on_goal = match[10]
            fouls = match[11]
            corners = match[12]
            offside = match[13]
            possession = match[14]
            attendance = match[15]

            # TODO: UPLOAD TO DB
            # match_id = upload_match(conn, date, score, competition, attendance)
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
