from MyDB import MyDB
import MySQLdb

class MySQLDB(MyDB):

    def __init__(self, h, p, d, u, pwd):
        MyDB.__init__(self, h, p, d, u, pwd)

    def getConnectionString(self):
        return {
            'user': self.user,
            'passwd': self.pswd,
            'host': self.host,
            'db': self.db,
        }

    def connect(self):
        config = self.getConnectionString()
        try:
            self.cnx = MySQLdb.connect(**config)
        except MySQLdb.Error as err:
            print(err)
        else:
            return True

        return False
