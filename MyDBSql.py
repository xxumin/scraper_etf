class MyDBSql:

    @staticmethod
    def getfrdate(todate):
        sql = "SELECT FRDATE = CONVERT(VARCHAR(10), KFRCOM.DBO.FN_PREVWORKDAY('{mydate}'), 120)" \
            .format(mydate=todate)
        return sql

    @staticmethod
    def getprclist(asset, tickers, ntickers):
        sql = ""

        if len(tickers) > 0 and len(ntickers) > 0:
            sql = "SELECT BTICKER, PRCSRC, " \
                  "TICKER = CASE PRCSRC WHEN 'Y' THEN YTICKER " \
                  "WHEN 'G' THEN GTICKER " \
                  "WHEN 'I' THEN ITICKER " \
                  "ELSE BTICKER END, " \
                  "ISINCODE, ETFNAME " \
                  "FROM KFRCOM.DBO.TC_CMETFMASTER_NEW " \
                  "WHERE USEYN = 'Y' " \
                  "AND EDAY IS NULL " \
                  "AND ASSETTYPE = '{asset}' " \
                  "AND BTICKER IN ('{tickers}') " \
                  "AND BTICKER NOT IN ('{ntickers}')".format(asset=asset, tickers=tickers, ntickers=ntickers)
        elif len(tickers) > 0 and len(ntickers) == 0:
            sql = "SELECT BTICKER, PRCSRC, " \
                  "TICKER = CASE PRCSRC WHEN 'Y' THEN YTICKER " \
                  "WHEN 'G' THEN GTICKER " \
                  "WHEN 'I' THEN ITICKER " \
                  "ELSE BTICKER END, " \
                  "ISINCODE, ETFNAME " \
                  "FROM KFRCOM.DBO.TC_CMETFMASTER_NEW " \
                  "WHERE USEYN = 'Y' " \
                  "AND EDAY IS NULL " \
                  "AND ASSETTYPE = '{asset}' " \
                  "AND BTICKER IN ('{tickers}')".format(asset=asset, tickers=tickers)
        elif len(tickers) == 0 and len(ntickers) > 0:
            sql = "SELECT BTICKER, PRCSRC, " \
                  "TICKER = CASE PRCSRC WHEN 'Y' THEN YTICKER " \
                  "WHEN 'G' THEN GTICKER " \
                  "WHEN 'I' THEN ITICKER " \
                  "ELSE BTICKER END, " \
                  "ISINCODE, ETFNAME " \
                  "FROM KFRCOM.DBO.TC_CMETFMASTER_NEW " \
                  "WHERE USEYN = 'Y' " \
                  "AND EDAY IS NULL " \
                  "AND ASSETTYPE = '{asset}' " \
                  "AND BTICKER NOT IN ('{ntickers}')".format(asset=asset, ntickers=ntickers)
        else:
            sql = "SELECT BTICKER, PRCSRC, " \
                  "TICKER = CASE PRCSRC WHEN 'Y' THEN YTICKER " \
                  "WHEN 'G' THEN GTICKER " \
                  "WHEN 'I' THEN ITICKER " \
                  "ELSE BTICKER END, " \
                  "ISINCODE, ETFNAME " \
                  "FROM KFRCOM.DBO.TC_CMETFMASTER_NEW " \
                  "WHERE USEYN = 'Y' " \
                  "AND EDAY IS NULL " \
                  "AND ASSETTYPE = '{asset}' ".format(asset=asset)

        return sql

    @staticmethod
    def getptflist(asset, tickers, ntickers):
        sql = ""

        if len(tickers) > 0 and len(ntickers) > 0:
            sql = "SELECT BTICKER, TICKER = MTICKER, EXCHANGE, ETFNAME " \
                  "FROM KFRCOM.DBO.TC_CMETFMASTER_NEW " \
                  "WHERE USEYN = 'Y' " \
                  "AND M_YN = 'Y' " \
                  "AND EDAY IS NULL " \
                  "AND ASSETTYPE = '{asset}' " \
                  "AND BTICKER IN ('{tickers}') " \
                  "AND BTICKER NOT IN ('{ntickers}')".format(asset=asset, tickers=tickers, ntickers=ntickers)
        elif len(tickers) > 0 and len(ntickers) == 0:
            sql = "SELECT BTICKER, TICKER = MTICKER, EXCHANGE, ETFNAME " \
                  "FROM KFRCOM.DBO.TC_CMETFMASTER_NEW " \
                  "WHERE USEYN = 'Y' " \
                  "AND M_YN = 'Y' " \
                  "AND EDAY IS NULL " \
                  "AND ASSETTYPE = '{asset}' " \
                  "AND BTICKER IN ('{tickers}') ".format(asset=asset, tickers=tickers)
        elif len(tickers) == 0 and len(ntickers) > 0:
            sql = "SELECT BTICKER, TICKER = MTICKER, EXCHANGE, ETFNAME " \
                  "FROM KFRCOM.DBO.TC_CMETFMASTER_NEW " \
                  "WHERE USEYN = 'Y' " \
                  "AND M_YN = 'Y' " \
                  "AND EDAY IS NULL " \
                  "AND ASSETTYPE = '{asset}' " \
                  "AND BTICKER NOT IN ('{ntickers}')".format(asset=asset, ntickers=ntickers)
        else:
            sql = "SELECT BTICKER, TICKER = MTICKER, EXCHANGE, ETFNAME " \
                  "FROM KFRCOM.DBO.TC_CMETFMASTER_NEW " \
                  "WHERE USEYN = 'Y' " \
                  "AND M_YN = 'Y' " \
                  "AND EDAY IS NULL " \
                  "AND ASSETTYPE = '{asset}'".format(asset=asset)

        return sql

    @staticmethod
    def delPrice():
        return "DELETE FROM KFRCOM.DBO.TD_CMETFPRICE_NEW WHERE WORKDAY = ? AND BTICKER = ?"

    @staticmethod
    def insertPrice():
        return "INSERT INTO KFRCOM.DBO.TD_CMETFPRICE_NEW VALUES (?,?,?,?,?,?,?,?,?,?,?,'etfBatch',GETDATE())"

    @staticmethod
    def delpricetest():
        return "DELETE FROM KFRCOM.DBO.TD_CMETFPRICE WHERE WORKDAY = ? AND TICKER = ?"

    @staticmethod
    def insertpricetest():
        return "INSERT INTO KFRCOM.DBO.TD_CMETFPRICE VALUES (?,?,?,?,?,?,?,?,?,'testing',GETDATE())"

    @staticmethod
    def delptfattr():
        return "DELETE FROM KFRCOM.DBO.TC_CMETFATTR WHERE BTICKER = ? AND ATTRTYPE  = ? AND SDAY = ?"

    @staticmethod
    def insertptfattr():
        return "INSERT INTO KFRCOM.DBO.TC_CMETFATTR VALUES (?,?,?,?,?,?,'',NULL,'etfBatch',GETDATE())"

    @staticmethod
    def insertptfattr2():
        # parameter 7개
        return "INSERT INTO KFRCOM.DBO.TC_CMETFATTR VALUES (?,?,?,?,?,?,?,NULL,'etfBatch',GETDATE())"
