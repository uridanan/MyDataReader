
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
