import psycopg2


def main():
    """
    Main function to load the database.
    Connects to the database, processes the files, extracts data, forms queries, and sends queries to the database.
    """
    # connect to database
    conn = connect_db()
    cur = conn.cursor()
    cur.execute('INSERT INTO Team VALUES (\'Chelsea\')')
    cur.execute('SELECT * FROM team')

    # TODO: Add team 'Man.Utd' to Team table and parse thru their data and add to database

    results = cur.fetchall()
    conn.commit()
    print(results)


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


if __name__ == '__main__':
    main()

