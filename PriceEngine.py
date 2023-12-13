import pandas_datareader as pdr
import math
from MyDBSql import MyDBSql as Sql
from PyQt5.QtCore import QThread, pyqtSignal


class PriceEngine(QThread):
    signal_init_data_pr = pyqtSignal(str)
    signal_start_data_pr = pyqtSignal(str, str)
    signal_end_data_pr = pyqtSignal(str)
    signal_termiate_data_pr = pyqtSignal(int, int)

    def __init__(self, mydb, df, frdate, todate):
        QThread.__init__(self)
        self.mydb = mydb
        self.df = df
        self.frdate = frdate
        self.todate = todate
        self.count = 0

    def __del__(self):
        self.wait()

    def run(self):
        self.getprice()

    def getprice(self):

        self.signal_init_data_pr.emit(str(len(self.df)))

        for i in self.df.index:
            row = self.df.iloc[i]

            bticker = row["BTICKER"]
            prcsrc = row["PRCSRC"]
            ticker = row["TICKER"]
            isin = row["ISINCODE"]
            etfname = row["ETFNAME"]

            delparamlist = []
            paramlist = []

            print("### Now BTicker : " + bticker + ", Ticker : " + ticker +
                  ", Source : " + prcsrc + ", INSICODE : " + isin + ", ETFNAME : " + etfname)

            # prcsrc에 따라 Yahoo, Google Financial에서 긁어옴.
            para = "google" if prcsrc == "G" else "yahoo"

            # 1. DataReader API를 통해서 @yahoo finance의 주식 종목 데이터를 가져온다.
            # 2. 일정 기간동안의 주식 정보를 가져오는 방법임.
            rs = pdr.DataReader(ticker, para, self.frdate, self.todate)
            print(rs)

            # Signal 보내~
            self.signal_start_data_pr.emit(bticker, "yahoo finance")

            # 일자별로 loop
            for idxworkday in rs.index:
                workday = idxworkday.strftime("%Y-%m-%d")
                data = rs.loc[idxworkday]

                # NaN : Not a Number
                if math.isnan(data["Open"]) or math.isnan(data["High"]) or \
                        math.isnan(data["Low"]) or math.isnan(data["Close"]):
                    continue

                # delete 조건 (workday, bticker)
                delparamlist.append((workday, bticker))

                if para == "yahoo":
                    paramlist.append((workday, bticker, isin, ticker, prcsrc, etfname,
                                      data["Open"], data["High"], data["Low"], data["Close"], data["Adj Close"]))
                else:
                    paramlist.append((workday, bticker, isin, ticker, prcsrc, etfname,
                                      data["Open"], data["High"], data["Low"], data["Close"], data["Close"]))

            # DataBase에 입력할 값 Data List
            if paramlist:
                self.mydb.dbExecute(Sql.delPrice(), delparamlist)
                self.mydb.dbExecute(Sql.insertPrice(), paramlist)

            self.count += 1
            self.signal_end_data_pr.emit("완료")

        self.signal_termiate_data_pr.emit(self.count, len(self.df))
















