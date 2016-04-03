from MySQLDB import MySQLDB
from MySqlFile import MySqlFile
from MyQuery import MyQuery
from MyRedShiftDB import MyRedShiftDB


import csv
import sys


def queryBI():
    bi = MyRedShiftDB('matrix-bi.ck2h68yqtpzh.eu-west-1.redshift.amazonaws.com',
                      '5439',
                      'dev',
                      'matrix',
                      'Th3r3!sN0Sp00n'
                      )

    if bi.connect() == False:
        return

    # Load query from file
    # script = MySqlFile("kpiAppsFromAppsDB.sql")
    # script.load()
    # queryString = script.getCommand(0)

    # Run query
    query = MyQuery("Select * from dwh.dwh_fact_daily limit 0;")
    query.run(bi.getConnection())

    # Test result
    for c in query.getColumns():
        print c

    # Create map with bundleId as key
    # Next step: allow composite keys
    map = query.getDataMap(1)

    print "BI Ready"

    bi.closeCnx()

    return map


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
    #Next step: allow composite keys
    map = query.getDataMap(1)

    print "AppsDB Ready"

    MySQLDB.closeCnx()

    return map


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
    #queryAppsDB()
    queryBI()


main()
