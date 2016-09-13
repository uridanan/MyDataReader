
class MySqlFile:
    sqlFile = ""
    sqlCommands = []

    def __init__(self, fileName):
        self.sqlFile = fileName

    def load(self):
        # Open and read the file as a single buffer
        # all SQL commands (split on ';')
        try:
            fd = open(self.sqlFile, 'r')
            self.sqlCommands = fd.read().split(';')
            fd.close()
        except IOError as err:
            print(err)

    def getcommands(self):
        return self.sqlCommands

    def getCommand(self, index):
        try:
            return self.sqlCommands[index]
        except LookupError as err:
            print(err)
