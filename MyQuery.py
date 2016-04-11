from collections import defaultdict

class MyQuery:
    query = ""
    result = []
    columns = []

    def __init__(self, command):
        self.query = command

    def run(self, cnx):
        cursor = cnx.cursor()
        cursor.execute(self.query)
        self.columns = [desc[0] for desc in cursor.description]
        self.result = cursor.fetchall()
        cursor.close()

    def getQuery(self):
        return self.query

    def getColumns(self):
        return self.columns

    def getResult(self):
        return self.result

    #Use a string to pass multiple indices for the key
    def getDataMap(self, keys):

        #parse keysString into an array of indices
        keyIndexList = keys.split(',')

        # creating a dict of dicts
        datamap = dict()
        c = 0
        for column in self.columns:
            col = column.lower()
            datamap[col] = dict()
            for row in self.result:
                # values = row.split(',')
                values = row
                key = self.getCompositeKey(keyIndexList,values)
                value = values[c]
                datamap[col][key] = value
            c += 1
        return datamap

    #Use arbitrary arguments list to pass multiple indices for the key
    def getMap(self, *keyIndexList):
        # creating a dict of dicts
        datamap = dict()
        c = 0
        for column in self.columns:
            col = column.lower()
            datamap[col] = dict()
            for row in self.result:
                values = row
                key = self.getCompositeKey(keyIndexList, values)
                value = values[c]
                datamap[col][key] = value
            c += 1
        return datamap

    def getCompositeKey(self, keys, values):
        key =  ""
        for i in keys:
            key = key + values[int(i)]
        return key
