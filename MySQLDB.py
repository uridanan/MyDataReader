from MyDB import MyDB
import mysql.connector
from mysql.connector import errorcode


class MySQLDB(MyDB):

    def __init__(self, h, p, d, u, pwd):
        MyDB.__init__(self, h, p, d, u, pwd)

    def getConnectionString(self):
        return {
            'user': self.user,
            'password': self.pswd,
            'host': self.host,
            'database': self.db,
            'raise_on_warnings': True
        }

    def connect(self):
        config = self.getConnectionString()
        try:
            self.cnx = mysql.connector.connect(**config)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            return True

        return False
