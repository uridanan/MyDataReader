from MyDB import MyDB
import psycopg2

class MyRedShiftDB(MyDB):

    def __init__(self, h, p, d, u, pwd):
        MyDB.__init__(self, h, p, d, u, pwd)

    def getConnectionString(self):
        return "dbname='" +self.db+"' port='"+self.port+"' user='" +self.user+"' password='"+self.pswd+"' host='"+self.host+"'"

    def connect(self):
        conn_string = self.getConnectionString()
        print "Connecting to database\n        ->%s" % (conn_string)

        try:
            self.cnx = psycopg2.connect(conn_string)
        except Exception as err:
            print(err)
        else:
            return True
        return False


# def queryBIDB(query):
#     # Connect to RedShift
#     conn_string
#     print "Connecting to database\n        ->%s" % (conn_string)
#     conn = psycopg2.connect(conn_string)
#     cursor = conn.cursor()
#     #Captures Column Names
#     column_names = []
#     cursor.execute("Select * from dwh.dwh_fact_daily limit 0;")
#     column_names = [desc[0] for desc in cursor.description]
#     all_cols=', '.join([str(x) for x in column_names])
#     print all_cols
#     conn.close()