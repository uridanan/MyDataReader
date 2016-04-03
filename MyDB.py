from abc import ABCMeta, abstractmethod


class MyDB(object):

    __metaclass__=ABCMeta
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

    @abstractmethod
    def getConnectionString(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    def getConnection(self):
        return self.cnx

    def closeCnx(self):
        self.cnx.close()
