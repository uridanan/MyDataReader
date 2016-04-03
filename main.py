from MySQLDB import MySQLDB
from MySqlFile import MySqlFile
from MyQuery import MyQuery

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


def queryAppsDB():

    #Get connection to the DB
    appsDB = MySQLDB('tt-db-prod.c1ejtvimm1e1.us-east-1.rds.amazonaws.com',
                     '',
                     'TabTale_DB',
                     'appsdb_ro',
                     'KbBqbVe18e',
                     )
    if appsDB.connect() == False:
        return

    #Load query from file
    script = MySqlFile("kpiAppsFromAppsDB.sql")
    script.load()
    queryString = script.getCommand(0)

    #Run query
    query = MyQuery(queryString)
    query.run(appsDB.getConnection())

    #Test result
    for c in query.getColumns():
        print c

    #Create map with bundleId as key
    map = query.getDataMap(1)

    print "Done"


def printToFile(rows):

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




def main():
    print("Start")
    queryAppsDB()


main()
