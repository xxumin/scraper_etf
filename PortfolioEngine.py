from selenium import webdriver
from bs4 import BeautifulSoup
from MyDBSql import MyDBSql as Sql
from PyQt5.QtCore import QThread, pyqtSignal
from urllib.error import HTTPError
import time
import os

mydirectory = os.getcwd()
errmsg_chromedriver = " 경로에 chromedriver.exe가 존재하지 않거나 버전 업그레이드가 필요합니다."


class PortfolioEngine(QThread):
    signal_init_data = pyqtSignal(str)
    signal_start_data = pyqtSignal(str, str)
    signal_end_data = pyqtSignal(str)
    signal_termiate_data = pyqtSignal(int, int, list)

    def __init__(self, mydb, df, stdate, url, asset):
        QThread.__init__(self)
        self.mydb = mydb
        self.df = df
        self.stddate = stdate
        self.url = url
        self.asset = asset
        self.count = 0
        # MorningStar Detail  : https://www.morningstar.com/etfs/@Exchange/@Ticker/portfolio(주식)
        # MorningStar Summary : https://portfolios.morningstar.com/fund/summary?t=@Ticker(채권)

    def __del__(self):
        self.wait()

    def run(self):
        if self.url == "Summary" and self.asset == "FBD":
            self.getportfolio_summary_bond()
        elif self.url == "Summary":
            self.getportfolio_summary()
        elif self.url == "Detail":
            self.getportfolio_detail()

    @staticmethod
    def initbrowser():
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--window-size=1280,1024")
        options.add_argument("disabled-gpu")

        # Chrome 드라이버 업그레이드시 새버전 다운로드 해야 함.
        # 웹 드라이버를 다운받아 둔 경로 입력.

        if os.path.isfile(mydirectory + "/chromedriver.exe"):
            return webdriver.Chrome(mydirectory + "/chromedriver.exe", chrome_options=options)
        else:
            return None

    def getportfolio_detail(self):

        browser = self.initbrowser()

        if not browser:
            print(str(mydirectory) + errmsg_chromedriver)
            self.signal_end_data.emit(str(mydirectory) + errmsg_chromedriver)
            return 0

        self.signal_init_data.emit(str(len(self.df)))
        errlist = []

        for i in self.df.index:
            try:
                row = self.df.iloc[i]

                bticker = row["BTICKER"]
                ticker = row['TICKER']
                exchange = row['EXCHANGE']

                connectingurl = "https://www.morningstar.com/etfs/{0}/{1}/portfolio".format(exchange, ticker)

                print("# This Ticker is '" + bticker + "', URL : " + connectingurl)
                # Signal 보내~
                self.signal_start_data.emit(bticker, connectingurl)

                browser.get(connectingurl)

                if browser is not None:
                    bs = gethtml(browser)

                    # Portfolio 기준일
                    pftag = bs.select(getselector("pftstdday_detail"))

                    if pftag:
                        if pftag[0].find("time") is not None:
                            workday = pftag[0].find("time").get("datetime")
                            print("포트폴리오 기준일 : {0}".format(workday))

                    # 우선 웹에 있는 기준일 사용 X
                    workday = self.stddate.strftime('%Y-%m-%d')

                    # Sector Table
                    sectortbl = bs.select(".sal-sector-exposure__sector-table > tbody > tr")
                    sectorlist = getdatalist(sectortbl, bticker, "S02", workday)

                    if sectorlist:
                        dellist = [(bticker, "S02", workday)]

                        self.mydb.dbExecute(Sql.delptfattr(), dellist)
                        self.mydb.dbExecute(Sql.insertptfattr(), sectorlist)
                    else:
                        errlist.append(bticker)
                        self.signal_end_data.emit("해당 Ticker의 url에 접속해서 확인 바랍니다.")
                        continue

                    elem = browser.find_element_by_xpath(getxpath("region"))

                    if elem:
                        elem.click()
                        bs = gethtml(browser)

                        # Region Table
                        regiontbl = bs.select('.sal-region-exposure__region-table > tbody > tr')
                        regionlist = getdatalist(regiontbl, bticker, "S01", workday)

                        # 포트폴리오 속성 DB 입력
                        if regionlist:
                            dellist = [(bticker, "S01", workday)]

                            self.mydb.dbExecute(Sql.delptfattr(), dellist)
                            self.mydb.dbExecute(Sql.insertptfattr(), regionlist)
                        else:
                            print("Region List is Empty")

                    self.count += 1
                    self.signal_end_data.emit("완료")

            except HTTPError as httperr:
                print(httperr)
            except Exception as err:
                self.signal_end_data.emit(str(err))
                errlist.append(bticker)
                continue
            finally:
                print(bticker, "포트폴리오 수집 끝")

        self.signal_termiate_data.emit(self.count, len(self.df), errlist)
        # browser.close()

    def getportfolio_summary(self):
        browser = self.initbrowser()

        if not browser:
            print(str(mydirectory) + errmsg_chromedriver)
            self.signal_end_data.emit(str(mydirectory) + errmsg_chromedriver)
            return 0

        workday = self.stddate.strftime('%Y-%m-%d')
        errlist = []
        self.signal_init_data.emit(str(len(self.df)))

        for i in self.df.index:
            row = self.df.iloc[i]

            bticker = row["BTICKER"]
            ticker = row['TICKER']

            connectingurl = "https://portfolios.morningstar.com/fund/summary?t={0}".format(ticker)
            print("# This Ticker is '" + bticker + "', URL : " + connectingurl)
            self.signal_start_data.emit(bticker, connectingurl)

            browser.get(connectingurl)
            bs = gethtml(browser)

            # Sector Table
            sectortbl = bs.select("#sector_we_tab > tbody > tr:nth-child(n+4):not(.hr)")
            datalist = getdatalist(sectortbl, bticker, "S02", workday, "Summary")

            if datalist:
                dellist = [(bticker, "S02", workday)]

                self.mydb.dbExecute(Sql.delptfattr(), dellist)
                self.mydb.dbExecute(Sql.insertptfattr(), datalist)

                print("Sector Table Complete.")

            # Region Table
            regiontbl = bs.select("#world_regions_tab > tbody > tr:nth-child(n+2):not(.hr):not(.hr1):not(.fontsize_10)")
            regiontbl.pop()
            regiontbl.pop()
            regiontbl.pop()
            datalist = getdatalist(regiontbl, bticker, "S01", workday, "Summary")

            if datalist:
                dellist = [(bticker, "S01", workday)]

                self.mydb.dbExecute(Sql.delptfattr(), dellist)
                self.mydb.dbExecute(Sql.insertptfattr(), datalist)

                print("Region Table Complete.")

            self.count += 1
            self.signal_end_data.emit("완료")

        self.signal_termiate_data.emit(self.count, len(self.df), errlist)

    def getportfolio_summary_bond(self):

        browser = self.initbrowser()

        if not browser:
            print(str(mydirectory) + errmsg_chromedriver)
            self.signal_end_data.emit(str(mydirectory) + errmsg_chromedriver)
            return 0

        workday = self.stddate.strftime('%Y-%m-%d')
        errlist = []
        self.signal_init_data.emit(str(len(self.df)))

        for i in self.df.index:
            row = self.df.iloc[i]

            bticker = row["BTICKER"]
            ticker = row['TICKER']

            connectingurl = "https://portfolios.morningstar.com/fund/summary?t={0}".format(ticker)
            print("# This Ticker is '" + bticker + "', URL : " + connectingurl)

            self.signal_start_data.emit(bticker, connectingurl)

            browser.get(connectingurl)
            bs = gethtml(browser)

            # Statistics Table(B01 ~ B05)
            statisticstbl = bs.select("#bond_style_tab > div > div:nth-child(5) > table > tbody > tr:not(.hr)")
            datalist = getdatalist(statisticstbl, bticker, "B00", workday, "Summary")

            if datalist:
                dellist = [(bticker, "B01", workday), (bticker, "B02", workday), (bticker, "B03", workday),
                           (bticker, "B04", workday), (bticker, "B05", workday)]

                self.mydb.dbExecute(Sql.delptfattr(), dellist)
                self.mydb.dbExecute(Sql.insertptfattr2(), datalist)

            # Credit Table(B06)
            credittbl = bs.select("#bond_style_tab > div > table > tbody > tr:not(.hr)")
            credittbl.pop()  # 마지막줄 제거

            datalist = getdatalist(credittbl, bticker, "B06", workday, "Summary")

            if datalist:
                dellist = [(bticker, "B06", workday)]

                self.mydb.dbExecute(Sql.delptfattr(), dellist)
                self.mydb.dbExecute(Sql.insertptfattr(), datalist)

            # Country BreakDown Table(B07)
            countrytbl = bs.select("#sectorWeightings > div:nth-child(3) > table > tbody > tr:not(.hr)")
            countrytbl.pop()
            countrytbl.pop()

            datalist = getdatalist(countrytbl, bticker, "B07", workday, "Summary")

            if datalist:
                dellist = [(bticker, "B07", workday)]

                self.mydb.dbExecute(Sql.delptfattr(), dellist)
                self.mydb.dbExecute(Sql.insertptfattr(), datalist)

            # Sector Table
            sectortbl = bs.select("#sector_wb_tab > tbody:nth-child(n+4) > tr.text1.str")
            datalist = getdatalist(sectortbl, bticker, "B08", workday, "Summary")

            if datalist:
                dellist = [(bticker, "B08", workday)]

                self.mydb.dbExecute(Sql.delptfattr(), dellist)
                self.mydb.dbExecute(Sql.insertptfattr(), datalist)

                print("Sector Table Complete.")

            self.count += 1
            self.signal_end_data.emit("완료")

        self.signal_termiate_data.emit(self.count, len(self.df), errlist)


def gethtml(browser):
    time.sleep(10)

    # 모든 자원이 로드될 때까지 최대 10초 대기
    browser.implicitly_wait(10)

    html = browser.page_source
    bs = BeautifulSoup(html, "html.parser")

    return bs


def getdatalist(htmlinfo, bticker, attrcode, workday, code="Detail"):
    datalist = []
    i = 1
    for tr in htmlinfo:
        if attrcode == "S02" and code == "Summary":
            # Summary Page
            datas = tr.find_all(["th", "td"])

            item = [bticker, attrcode, workday, '2999-12-31']

            for data in datas[1:3]:
                item.append(data.text)

            # sql dlist
            if len(item) == 6:
                if item[5] != '':
                    datalist.append(item[0:])

        elif attrcode == "S01" and code == "Summary":
            # Summary Page - World Regions
            datas = tr.find_all(["th", "td"])
            item = [bticker, attrcode, workday, '2999-12-31']

            for i in range(2):
                print(i)
                if i == 1:
                    if datas[1].text.strip() == "—":
                        item.append(None)
                    else:
                        item.append(datas[1].text.strip())
                else:
                    item.append(datas[i].text.strip())

            # sql dlist
            if len(item) == 6:
                if item[5] != '':
                    datalist.append(item[0:])

        elif attrcode[0:1] == "S":
            # Detail Page
            datas = tr.find_all('span', 'ng-binding')

            item = [bticker, attrcode, workday, '2999-12-31']

            for data in datas:
                item.append(data.text)

            # sql dlist
            datalist.append(item[0:-1])

        elif attrcode == "B06":
            datas = tr.find_all(["th", "td"])
            item = [bticker, attrcode, workday, '2999-12-31']

            for i in range(2):
                if i == 1:
                    if datas[1].text.strip() == "—":
                        item.append(None)
                    else:
                        item.append(datas[1].text.strip())
                else:
                    item.append(datas[i].text.strip())

            datalist.append(item[0:])

        elif attrcode[0:] == "B07":
            datas = tr.find_all(["th", "td"])

            item = [bticker, attrcode, workday, '2999-12-31']

            for data in datas[0:11]:
                item.append(data.text.strip())

            # sql dlsit
            datalist.append(item[0:-3])

        elif attrcode[0:] == "B08":
            datas = tr.find_all(["th", "td"])

            item = [bticker, attrcode, workday, '2999-12-31']

            for n in range(1, 3):
                if n == 2:
                    if datas[2].text.strip() == "—":
                        item.append(None)
                    else:
                        item.append(datas[2].text.strip())
                else:
                    item.append(datas[n].text.strip())

            print(item[0:])
            #
            datalist.append(item[0:])

        else:
            datas = tr.find_all(["th", "td"])
            item = [bticker, ("B0"+str(i)), workday, '2999-12-31']

            if i == 3:
                item.append(datas[0].text.strip())
                item.append(None)
                item.append(datas[1].text.strip())
            else:
                item.append(datas[0].text.strip())

                if datas[1].text.strip() == "—":
                    item.append(None)
                else:
                    item.append(datas[1].text.strip())

                item.append("")
            i += 1

            # sql dlist
            datalist.append(item[0:])
    print(datalist)
    return datalist


def getselector(flag):
    selector = ""

    if flag == "pftstdday_detail":
        # Detail > Portfolio 기준일
        selector = "#__layout > div > div.mdc-page-shell__content.mds-page-shell__content > " \
               "main > div.etf__header > header > div > div.mdc-security-header__rating-description"
    return selector


def getxpath(flag):
    xpath = ""

    if flag == "region":

        xpath = "/html/body/div[2]/div/div/div[2]/div[3]/main/div[2]/div/div/div[1]/sal-components/section/" \
                "div/div/div/div/div[2]/div[1]/div[5]/div[1]/div/div/div[2]/div/div/div[2]/div[1]/" \
                "sal-components-segment-band/div/div/mwc-tabs/div/mds-button-group/div/slot/div/" \
                "mds-button[2]/label/input"
    return xpath

