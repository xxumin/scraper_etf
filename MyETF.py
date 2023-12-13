import sys
import traceback

from PyQt5 import uic
from PyQt5.QtCore import QDate, pyqtSlot, QThread
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import *

from DBManager import *
from MyDBSql import MyDBSql as Sql
from PortfolioEngine import PortfolioEngine
from PriceEngine import PriceEngine

# UI파일 연결
# 단, UI파일은 Python 코드 파일과 같은 디렉토리에 위치해야한다.
form_class = uic.loadUiType("MyETF_GUI.ui")[0]


# 화면을 띄우는데 사용되는 Class 선언
class MyETF(QMainWindow, form_class):

    def __init__(self):
        super().__init__()

        self.worker_thread = None
        self.setupUi(self)
        self.init_ui()

        # Date Setting
        self.nowdate = QDate.currentDate()

        # DB Connect
        self.mydb = DBManager()

        # UI Init
        self.init_price()
        self.init_portfolio()
        self.connectevent()

    def init_ui(self):
        self.setWindowIcon(QIcon("etficon.png"))
        self.setWindowTitle("해외 ETF 정보 수집 프로그램")
        self.pbar.setValue(0)
        self.tabWidget.setCurrentIndex(0)

    # Price Tab GUI 초기화
    def init_price(self):
        # Get Date Value
        df = self.mydb.dbSelect(Sql.getfrdate(self.nowdate.toString("yyyy-MM-dd")))

        self.qfrdate.setDate(QDate.fromString(df.loc[0][0], "yyyy-MM-dd"))
        self.qtodate.setDate(self.nowdate)

    # Portfolio Tab GUI 초기화
    def init_portfolio(self):
        self.qstddate.setDate(self.nowdate)
        self.cboURL.addItem("MornigStar Detail(주식 수집 가능)")
        self.cboURL.addItem("MoringStar Summary(주식, 채권 수집가능)")
        self.rdoStock.setChecked(True)

    def connectevent(self):
        self.btnRun.clicked.connect(self.btnrun_clicked)
        self.btnExt.clicked.connect(app.quit)

    def btnrun_clicked(self):
        asset = ""
        url = "Detail"
        sql = ""

        if self.rdoStock.isChecked():
            asset = "FST"
        elif self.rdoBond.isChecked():
            asset = "FBD"

        ticker = self.txtTickerIN.text().strip() if self.chkIN.isChecked() else ""
        nticker = self.txtTickerNotIN.text().strip() if self.chkNotIN.isChecked() else ""

        tickers = listtostr(strtolist(ticker))
        ntickers = listtostr(strtolist(nticker))

        if self.tabWidget.currentIndex() == 0:
            sql = Sql.getprclist(asset, tickers, ntickers)
        elif self.tabWidget.currentIndex() == 1:
            sql = Sql.getptflist(asset, tickers, ntickers)
        else:
            sql = ""

        if self.cboURL.currentIndex() == 1:
            url = "Summary"

        try:
            print(sql)
            # DataFrame에 SQL 결과값 넣기
            df = self.mydb.dbSelect(sql) if sql != "" else []

            # 크롤링 하는 핵심 Class
            if len(df) != 0:
                pmax = len(df)
                self.pbar.setValue(0)
                self.pbar.setMaximum(pmax)

                frdate = self.qfrdate.date().toPyDate()
                todate = self.qtodate.date().toPyDate()
                stdate = self.qstddate.date().toPyDate()

                if self.tabWidget.currentIndex() == 0:
                    reply = QMessageBox.question(self, "작업 실행 확인", "ETF 가격을 수집하시겠습니까?",
                                                 QMessageBox.Yes | QMessageBox.No)

                    if reply == QMessageBox.Yes:
                        self.worker_thread = PriceEngine(self.mydb, df, frdate, todate)
                        self.connectsingals_price()

                elif self.tabWidget.currentIndex() == 1:
                    reply = QMessageBox.question(self, "작업 실행 확인", "ETF 포트폴리오를 수집하시겠습니까?",
                                                 QMessageBox.Yes | QMessageBox.No)

                    if reply == QMessageBox.Yes:
                        self.worker_thread = PortfolioEngine(self.mydb, df, stdate, url, asset)
                        self.connectsignals()

            else:
                if self.tabWidget.currentIndex() == 0:
                    self.LogPrice.append("해당 Ticker가 존재하지 않습니다. 마스터 테이블을 확인하세요.")
                elif self.tabWidget.currentIndex() == 1:
                    self.LogPtf.append("해당 Ticker가 존재하지 않습니다. 마스터 테이블을 확인하세요.")

        except Exception as e:
            print(e.args)
        finally:
            print("-------------------------------------------------------------------------------------")

    def connectsignals(self):
        self.worker_thread.signal_init_data.connect(self.initsignaldata)
        self.worker_thread.signal_start_data.connect(self.startsignaldata)
        self.worker_thread.signal_end_data.connect(self.endsignaldata)
        self.worker_thread.signal_termiate_data.connect(self.terminatesignaldata)

        if self.worker_thread.isRunning():
            self.worker_thread.terminate()

        self.worker_thread.start()

    def connectsingals_price(self):
        self.worker_thread.signal_init_data_pr.connect(self.initsignaldata_price)
        self.worker_thread.signal_start_data_pr.connect(self.startsignaldata_price)
        self.worker_thread.signal_end_data_pr.connect(self.endsignaldata_price)
        self.worker_thread.signal_termiate_data_pr.connect(self.terminatesignaldata_price)

        if self.worker_thread.isRunning():
            self.worker_thread.terminate()

        self.worker_thread.start()

    @pyqtSlot(str)
    def initsignaldata(self, count):
        self.pbar.setValue(0)
        self.LogPtf.append("---------------------------------------------------------------")
        self.LogPtf.append("*** {}개의 ETF 포트폴리오 수집 크롤링을 시작합니다.".format(count))
        self.LogPtf.append("---------------------------------------------------------------")

    @pyqtSlot(str)
    def initsignaldata_price(self, count):
        self.pbar.setValue(0)
        self.LogPrice.append("---------------------------------------------------------------")
        self.LogPrice.append("*** {}개의 ETF 가격 수집 크롤링을 시작합니다.".format(count))
        self.LogPrice.append("---------------------------------------------------------------")

    @pyqtSlot(str, str)
    def startsignaldata(self, bticker, url):
        self.LogPtf.append("# This Ticker is '" + bticker + "', URL : " + url + "  크롤링 진행...")

    @pyqtSlot(str, str)
    def startsignaldata_price(self, bticker, url):
        self.LogPrice.append("# This Ticker is '" + bticker + "', URL : " + url + "  크롤링 진행...")

    @pyqtSlot(str)
    def endsignaldata(self, status):
        self.pbar.setValue(self.pbar.value() + 1)

        if status != "완료":
            self.LogPtf.append(status)

    @pyqtSlot(str)
    def endsignaldata_price(self, status):
        self.pbar.setValue(self.pbar.value() + 1)

    @pyqtSlot(int, int, list)
    def terminatesignaldata(self, success, total, errlist):
        self.LogPtf.append("---------------------------------------------------------------")
        self.LogPtf.append("*** 포트폴리오 수집을 완료하였습니다. ({}/{})".format(success, total))
        self.LogPtf.append("---------------------------------------------------------------")

        if errlist:
            self.LogPtf.append("에러가 발생한 Ticker 리스트 : " + str(errlist))

    @pyqtSlot(int, int)
    def terminatesignaldata_price(self, success, total):
        self.LogPrice.append("---------------------------------------------------------------")
        self.LogPrice.append("*** 가격 수집을 완료하였습니다. ({}/{})".format(success, total))
        self.LogPrice.append("---------------------------------------------------------------")


def strtolist(param):
    paramlist = param.split(",") if param.find(",") else [param]
    returnlist = []

    # 양쪽 공백 제거
    for p in paramlist:
        returnlist.append(p.strip())

    return returnlist


def listtostr(param):
    # ,를 붙여서 문자열을 만듦.
    return "', '".join(param)


if __name__ == "__main__":
    # QApplication : 프로그램을 실행시켜주는 클래스
    app = QApplication(sys.argv)

    # WindowClass의 인스턴스 생성
    myWindow = MyETF()

    # 프로그램 화면을 보여주는 코드
    myWindow.show()

    # 프로그램을 이벤트루프로 진입시키는(프로그램을 작동시키는) 코드
    app.exec_()
