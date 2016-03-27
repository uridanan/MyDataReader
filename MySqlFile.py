
class MySqlFile:
    sqlFile = ""
    sqlCommands = []

    def __init__(self, fileName):
        self.sqlFile = fileName

    def load(self):
        # Open and read the file as a single buffer
        # all SQL commands (split on ';')
        fd = open(self.sqlFile, 'r')
        self.sqlCommands = fd.read().split(';')
        fd.close()

    def getcommands(self):
        return self.sqlCommands
