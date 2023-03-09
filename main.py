import datetime

import psycopg2
import pandas
import numpy as np
import math
import pyodbc
import requests
from scipy import stats
import matplotlib.pyplot as plt


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

    # upload this team (filename) to database
    team_name = filename[:-4]
    upload_team(conn, team_name)

    current_month_year = ''
    # for each match, extract stats, upload to db
    line = 0
    for match in raw_data.values.tolist():
        line += 1
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
            # ensure home goals are first
            if not is_home and score is not None:
                if score.__contains__('('):
                    score = score[2] + score[1] + score[0] + score[3:]
                else:
                    scores = score.split('-')
                    score = scores[1] + '-' + scores[0]

            # GET REMAINING STATS FOR THIS TEAM
            if type(match[6]) == float:
                formation = None
            else:
                # get formation at end of bogus string (1st good char is number)
                formation = None
                for i in range(0, len(match[6])):
                    if match[6][i].isnumeric() and formation is None:
                        formation = match[6][i:]

            shots = convert_to_int(match[9])
            shots_on_goal = convert_to_int(match[10])
            fouls = convert_to_int(match[11])
            corners = convert_to_int(match[12])
            offside = convert_to_int(match[13])
            possession = convert_to_int(match[14])
            attendance = convert_to_int(match[15])

            # UPLOAD TO DB
            # query for opponent
            cur = conn.cursor()
            cur.execute('SELECT team_name FROM team WHERE team_name = %s', (opponent,))
            response = cur.fetchall()
            # if opponent doesn't exist, upload to team
            if len(response) == 0:
                upload_team(conn, opponent)

            # identify home & away teams then query for match with date and teams
            if is_home:
                home_team = team_name
                away_team = opponent
            else:
                home_team = opponent
                away_team = team_name
            cur.execute("SELECT match_id FROM match WHERE home_team = %s AND "
                        "away_team = %s AND date = %s", (home_team, away_team, date))
            match_exists = cur.fetchall()

            # if match exists
            if len(match_exists) > 0:
                # get id
                match_id = match_exists[0][0]
            # otherwise
            else:
                # upload match and get new match id
                match_id = upload_match(conn, date, score, competition, attendance, home_team, away_team)

            # upload to team match stats
            upload_team_match_stats(conn, match_id, team_name, is_home, formation,
                                    possession, shots, shots_on_goal, fouls, corners, offside)
        print('\nPERCENT COMPLETE: %.2f' % ((line / len(raw_data.values.tolist())) * 100), '%')


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
                if type(score) == float:
                    score = None
                elif score.__contains__('('):
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


def upload_team(conn, team_name):
    """
    Uploads a given team to the database.

    :param conn: the database connection
    :param team_name: name of team
    """
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO Team (team_name) VALUES (%s);", (team_name,))
        conn.commit()
        print('\tUPLOADED TEAM TO DATABASE!:', team_name)
    except pyodbc.Error as err:
        print(err)
    except psycopg2.DatabaseError as db_err:
        print(db_err)
        conn.rollback()


def upload_match(conn, date, score, competition, attendance, home_team, away_team):
    """
    Uploads a match to the database with the given params and the next consecutively available match id.

    :param conn: the database connection
    :param date: the date of the match as a datetime.date()
    :param score: the score e.g. '0-0'
    :param competition: the competition of the match as a str
    :param attendance: the match attendance as an int
    :param home_team: the home team
    :param away_team: the away team
    :return:
    """
    match_id = 1

    try:
        cur = conn.cursor()
        cur.execute('SELECT max(match_id) FROM Match')
        response = cur.fetchall()[0][0]
        if response is not None:
            match_id = response + 1

        cur.execute("INSERT INTO Match (match_id, date, score, competition, attendance, home_team, away_team)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s);", (match_id, date, score, competition, attendance, home_team,
                                                             away_team))
        conn.commit()
        print('\tUPLOADED MATCH TO DATABASE!:', match_id, date, score, competition, attendance, home_team, away_team)

    except pyodbc.Error as err:
        print(err)
    except psycopg2.DatabaseError as db_err:
        print(db_err)
        conn.rollback()

    return match_id


def upload_team_match_stats(conn, match_id, team_name, is_home, formation, possession, shots, shots_on_goal,
                            fouls, corners, offside):
    """
    Uploads team match stats to the database with the given params.

    :param conn: the database connection
    :param match_id: the id of the match in question
    :param team_name: the id of the team playing in the given match
    :param is_home: if this team (team_name) was home for this match
    :param formation: the formation this team used
    :param possession: how much possession this team has as a percentage
    :param shots: how many shots this team had
    :param shots_on_goal: how many shots on target this team had
    :param fouls: how many fouls this team committed
    :param corners: how many corners this team won
    :param offside: how many times this team was offside
    """
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO Team_Match_Stats (match_id, team_name, is_home, formation, possession, shots,"
                    "shots_on_goal, fouls, corners, offside) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (match_id, team_name, is_home, formation, possession, shots, shots_on_goal,
                     fouls, corners, offside))
        conn.commit()
        print('\tUPLOADED TEAM MATCH STATS TO DATABASE!:', match_id, team_name)
    except pyodbc.Error as err:
        print(err)
    except psycopg2.DatabaseError as db_err:
        print(db_err)
        conn.rollback()


def get_win_loss_per_year(matches):
    wins = {}
    years = []

    for match in matches.values:
        year = match[0].year
        if year not in wins.keys():
            years.append(year)
            wins.update({year: [0, 0]})
        is_433_home = match[4]
        if is_433_home:
            if match[1][0] > match[1][2]:
                wins[year][0] += 1
            elif match[1][0] < match[1][2]:
                wins[year][1] += 1
        else:
            if match[1][0] < match[1][2]:
                wins[year][0] += 1
            elif match[1][0] > match[1][2]:
                wins[year][1] += 1
    win_loss_per_year = []

    for year in wins:
        if wins[year][1] == 0:
            wins[year][1] = 1

        win_loss_per_year.append(wins[year][0] / wins[year][1])

    return np.array(win_loss_per_year), np.array(years)


def main():
    """
    Main function to load the database.
    """
    # connect to database
    conn = connect_db()

    # parse_csv_and_update_db('West Ham.csv', conn)

    # GET vs 442 ratio
    vs_442_query = "SELECT date, score, a.team_name, a.formation, a.is_home, b.team_name, b.formation, b.is_home " \
             "FROM team_match_stats a JOIN team_match_stats b " \
             "ON a.match_id = b.match_id " \
             "JOIN match ON match.match_id = a.match_id " \
             "WHERE a.formation = '4-3-3' AND b.formation = '4-4-2' " \
             "ORDER BY date"
    vs_442_matches = pandas.read_sql(vs_442_query, conn)
    vs_442_win_loss, vs_442_years = get_win_loss_per_year(vs_442_matches)

    # GET vs other formations ratio
    vs_other_query = "SELECT date, score, a.team_name, a.formation, a.is_home, b.team_name, b.formation, b.is_home " \
                     "FROM team_match_stats a JOIN team_match_stats b " \
                     "ON a.match_id = b.match_id " \
                     "JOIN match ON match.match_id = a.match_id " \
                     "WHERE a.formation = '4-3-3' AND b.formation != '4-4-2' " \
                     "ORDER BY date"
    vs_other_matches = pandas.read_sql(vs_other_query, conn)
    vs_other_win_loss, vs_other_years = get_win_loss_per_year(vs_other_matches)

    # plot hist of vs 442
    plt.bar(vs_442_years, vs_442_win_loss)
    plt.title('Win-Loss Ratio of 4-3-3 vs 4-4-2 over the years')
    plt.minorticks_on()
    plt.xlabel('Year')
    plt.ylabel('W/L Ratio')
    plt.show()

    # plot hist of vs other
    plt.bar(vs_other_years, vs_other_win_loss)
    plt.title('Win-Loss Ratio of 4-3-3 vs Other Formation over the years')
    plt.minorticks_on()
    plt.xlabel('Year')
    plt.ylabel('W/L Ratio')
    plt.show()

    # run standard test
    x = np.concatenate((vs_442_win_loss, vs_other_win_loss))
    k2, p = stats.normaltest(x)

    # run shapiro test
    shapiro_test = stats.shapiro(x)
    p_shapiro = shapiro_test.pvalue


    # perform Mann-Whitney U test
    planned_result = stats.mannwhitneyu(vs_442_win_loss, vs_other_win_loss, alternative='greater')
    p_planned = planned_result.pvalue

    print('done')


if __name__ == '__main__':
    main()
