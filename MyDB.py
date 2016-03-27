class MyDB:
    host = ""
    port = ""
    db = ""
    user = ""
    pswd = ""
    cnx = 0
    cursor = 0

    def __init__(self, h, p, d, u, pwd):
        self.host = h
        self.port = p
        self.db = d
        self.user = u
        self.pswd = pwd

    def getConnectionString(self):
        pass

    def connect(self):
        pass

    def getConnection(self):
        return self.cnx

    def getCursor(self):
        return self.cursor

    def closeCnx(self):
        self.cnx.close()

    def closeCursor(self):
        self.cursor.close()



