from mysqldb import MySQLDB
from mysqlfile import MySqlFile
from myquery import MyQuery
from myredshiftdb import MyRedShiftDB
from mydbfactory import myDBFactory

import sys
import os
import six
import json
import csv



#TODO
#refactor
#Load data from csv
#Mock data
#Tests
#Prepare for distribution
#Use decorators

#LINKS
#Use *args for arbitrary arg list https://docs.python.org/3.3/tutorial/controlflow.html#tut-unpacking-arguments
#Data structures in python https://docs.python.org/3.3/tutorial/datastructures.html
#Use Pandas.dataframe http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html






def loadjsondata(filename):
    with open(filename) as data_file:
        data = json.load(data_file)
    return data

def getMapFromDB(type, configFile, sqlFile, *keys):
    # Load configuration from file
    config = loadjsondata(configFile)['config']

    # Get DB connector instance
    db = mydbfactory().getInstance(type, config)

    if db.connect() == False:
        return

    # Load query from file
    script = MySqlFile(sqlFile)
    script.load()
    queryString = script.getCommand(0)

    # Run query
    query = MyQuery(queryString)
    query.run(db.getConnection())

    # Test result
    for c in query.getColumns():
        print c

    #Concatenate columns in *keys to create the key
    map = query.getMap(keys)

    print "Map Ready"

    db.closeCnx()

    return map


def getBIData():
    # Load configuration from file
    config = loadjsondata('bidbconfig.json')['config']

    # Get connection to the DB
    bi = MyRedShiftDB(
        config['host'],
        config['port'],
        config['database'],
        config['user'],
        config['password']
    )

    if bi.connect() == False:
        return

    # Load query from file
    script = MySqlFile("avgdailyimpressions.sql")
    script.load()
    queryString = script.getCommand(0)

    # Run query
    query = MyQuery(queryString)
    query.run(bi.getConnection())

    # Test result
    for c in query.getColumns():
        print c

    #Concatenate columns 1 & 2 to create the key
    map = query.getMap(1,2)

    print "BI Ready"

    bi.closeCnx()

    return map


def getAppsDBData():

    #Load configuration from file
    config = loadjsondata('appsdbconfig.json')['config']

    #Get connection to the DB
    appsDB = MySQLDB(
                     config['host'],
                     config['port'],
                     config['database'],
                     config['user'],
                     config['password']
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

    #Concatenate columns 1 & 2 to create the key
    map = query.getMap(1,2)

    print "AppsDB Ready"

    appsDB.closeCnx()

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


def join(left, right):
    # This routine assumes that both maps have the same key
    # If both maps have the same column, that column will be merged with precedence to right
    result = left.copy()
    for column in right.keys():
        if result.has_key(column):
            result[column].update(right[column])
        else:
            result[column] = right[column]
    print "Join Ready"
    return result

def transpose(map):
    # Use DataFrames from Pandas?
    result = dict()
    columns = map.keys()
    firstColumn = columns.__getitem__(0)
    rows = map[firstColumn].keys()
    for row in rows:
        result[row] = dict()
        for col in columns:
            try:
                result[row][col] = map[col][row]
            except KeyError:
                result[row][col] = "Missing Data"
    return result

def writeln(file,string):
    file.write(string)
    file.write('\n')

def getfirstentry(map):
    for entry in map.itervalues():
        return entry

#Implicitely extract all columns directly from the map when exporting
def export2csv(filename, map):
    #Extract column names
    firstRow = getfirstentry(map)
    columns = firstRow.keys()
    #Call the explicit method
    exportcolumns2csv(filename, map, columns)

#Explicitely specify the columns you want to export and in what order
def exportcolumns2csv(filename, map, columns):
    print "Open output file"
    f = open(filename, 'wt')
    try:
        #Write headers
        coma = ','
        headers = coma.join(columns)
        writeln(f,headers)

        #Write data
        rows = map.itervalues()
        for row in rows:
            values = list()
            for c in columns:
                value = ""
                if isinstance(row[c], six.string_types):
                    value = row[c].encode('UTF8')
                else:
                    value = str(row[c]).encode('UTF8')
                values.append(doublequote(value))
            line = coma.join(values)
            writeln(f,line)

    except Exception as err:
        print(err)
    finally:
        f.close()
        print "Close output file"


def wrap(char, string):
    return char + string + char


def doublequote(string):
    return wrap('"',string)


def main():
    print("Start")

    # Title
    # BundleId
    # Store
    # Account
    # Platform
    # Graphical Engine
    # Studio
    # Orientation
    # SDK
    # PSDK
    # Update
    # InitialRelease
    appsDB = getAppsDBData()

    # title
    # bundleid
    # store
    # banners_avg_daily_imp
    # inter_avg_daily_imp
    # rv_avg_daily_imp
    bi = getBIData()

    merge = join(bi,appsDB)
    r2c = transpose(merge)
    #export2csv('out.csv', r2c)
    exportcolumns2csv('out.csv',r2c,['title', 'bundleid', 'store', 'account', 'platform',
                                     'graphical engine', 'studio', 'orientation',
                                     'sdk', 'psdk', 'update', 'initialrelease',
                                     'banners_avg_daily_imp', 'inter_avg_daily_imp', 'rv_avg_daily_imp'])


main()
