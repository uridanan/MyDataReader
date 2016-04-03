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

    def getDataMap(self,keyIndex):
         #creating a dict of dicts
        datamap = dict()
        c = 0
        for col in self.columns:
            datamap[col] = dict()
            for row in self.result:
                #values = row.split(',')
                values = row
                key = values[keyIndex]
                value = values[c]
                datamap[col][key] = value
            c += 1
        return datamap