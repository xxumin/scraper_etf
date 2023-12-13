import pyodbc
import pandas as pd
from MyInfo import *


class DBManager:
    myconstr = ""

    def __init__(self):

        self.ip = MyInfo.myserver
        self.db = MyInfo.mydb
        self.id = MyInfo.myid
        self.pwd = MyInfo.mypwd

        # DB Connect
        self.myconn = self.dbconnect()
        self.cursor = self.myconn.cursor()

    def dbconnect(self):
        self.myconstr = ";".join(["DRIVER={SQL Server}", "SERVER={sv}".format(sv=self.ip),
                                  "DATABASE={db}".format(db=self.db),
                                  "UID={id}".format(id=self.id),
                                  "PWD={pwd}".format(pwd=self.pwd)])

        print("Connection Information : " + self.myconstr)

        return pyodbc.connect(self.myconstr)

    def __del__(self):
        self.myconn.close()

    def dbSelect(self, query, *args):
        mydf = pd.read_sql_query(query, self.myconn, params=args)

        return mydf

    def dbExecute(self, query, args):
        cur = self.myconn.cursor()
        cur.executemany(query, args)
        cur.commit()


#mydb = DBManager()
#sql = "select * from tc_cmETFMaster Where 1 = 1"
#df = mydb.dbSelect(sql)
#print(df)

# sql = "update tc_cmETFMaster set ipuser = ? where ISINCODE = ?"
# para = ["testing2", "AAAA"]
# print(sql)

# mydb.dbExecute(sql, para)

# print(df)










