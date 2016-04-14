from mysqlfile import MySqlFile
from myquery import MyQuery
from mydbfactory import myDBFactory
import json


#TODO
#refactor
#Mock data
#Tests
#Prepare for distribution
#Use decorators

#LINKS
#Use *args for arbitrary arg list https://docs.python.org/3.3/tutorial/controlflow.html#tut-unpacking-arguments
#Data structures in python https://docs.python.org/3.3/tutorial/datastructures.html
#Use Pandas.dataframe http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html


###################################################################################################
#Support methods

def loadjsondata(filename):
    with open(filename) as data_file:
        data = json.load(data_file)
    return data

###################################################################################################

###################################################################################################
#Map Manipulation methods

def getMapFromDB(type, configFile, sqlFile, *keys):
    # Load configuration from file
    config = loadjsondata(configFile)['config']

    # Get DB connector instance
    factory = myDBFactory()
    db = factory.getInstance(type, config)

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
        print (c)

    #Concatenate columns in *keys to create the key
    map = query.getMap(*keys)

    print ("Map Ready")

    db.closeCnx()

    return map


def join(left, right):
    # This routine assumes that both maps have the same key
    # If both maps have the same column, that column will be merged with precedence to right
    result = left.copy()
    for column in right.keys():
        if result.has_key(column):
            result[column].update(right[column])
        else:
            result[column] = right[column]
    print ("Join Ready")
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

###################################################################################################

