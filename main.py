import mysql.connector
from mysql.connector import errorcode

import psycopg2
import csv
import sys

def queryBIDB(query):
    # Connect to RedShift
    conn_string = ""
    print "Connecting to database\n        ->%s" % (conn_string)
    conn = psycopg2.connect(conn_string);
    cursor = conn.cursor();
    #Captures Column Names
    column_names = [];
    cursor.execute("Select * from dwh.dwh_fact_daily limit 0;");
    column_names = [desc[0] for desc in cursor.description]
    all_cols=', '.join([str(x) for x in column_names])
    print all_cols;
    conn.close()


def queryAppsDB(query):
    config = {
        'user': 'appsdb_ro',
        'password': '',
        'host': '',
        'database': 'TabTale_DB',
        'raise_on_warnings': True,
    }
    try:
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor = cnx.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        print("Close cursor")
        cnx.close()
        print("Close cnx")
        return result;

def loadSqlFromFile(filename):
    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    return sqlCommands;

def runAppsDBQuery():
    sqlCommands = loadSqlFromFile("kpiAppsFromAppsDB.sql");

    #Assumption: only the first query in the file is interesting
    query = sqlCommands[0];

    rows = queryAppsDB(query);

    print "Open output file"
    f = open('out.csv', 'wt')
    try:
        #writer = csv.writer(f)
        #writer.writerow(('Title 1', 'Title 2', 'Title 3'))
        for row in rows:
            f.write(str(row))
    finally:
        f.close()
    print "Close output file"



def executeScriptsFromFile(filename, cursor):

    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    results = [];
    i = 0

    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            cursor.execute(command)
            results[i] = cursor.fetchall()
        except mysql.connector.Error, msg:
            print "Command skipped: ", msg

    return results;



def main():
    print("Start")
    runAppsDBQuery();


main()
