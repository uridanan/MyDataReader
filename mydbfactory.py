from mysqldb import MySQLDB
from myredshiftdb import MyRedShiftDB

def mydbfactory():
    def getInstance(type, config):
        switch = {
            'mysql': MySQLDB(
                config['host'],
                config['port'],
                config['database'],
                config['user'],
                config['password']
            ),
            'redshift': MyRedShiftDB(
                config['host'],
                config['port'],
                config['database'],
                config['user'],
                config['password']
            )
        }
        return switch[type]
