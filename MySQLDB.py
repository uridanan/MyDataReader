from MyDB import MyDB
import pymysql

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
            self.cnx = pymysql.connect(**config)
        except pymysql.Error as err:
            print(err)
        else:
            return True

        return False
