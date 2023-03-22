import pymysql

class Mysql:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def connect(self):
        self.conn = pymysql.connect(host=self.host, port=3360, user=self.user, password=self.password, database=self.database)
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def execute(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return 'execute Success'
        except Exception as e:
            self.conn.rollback()
            raise e

    def exceute_many(self, sql, value_params):
        try:
            self.cursor.executemany(sql, value_params)
            self.conn.commit()
            return 'execute_many Success'
        except Exception as e:
            self.conn.rollback()
            raise e

    def fetch_all(self, sql):
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            raise e

    def fetch_one(self, sql):
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchone()
        except Exception as e:
            raise e
    
    def _get_signal_type(self, source_table_header):
        if source_table_header == "WEEK_VOL":
            return "vol"
        elif source_table_header == "WEEK_PUMPING":
            return "pumping"
        elif source_table_header == "WEEK_DEAD_CROSS":
            return "deadcross"
        elif source_table_header == "WEEK_GOLDEN_CROSS":
            return "goldencross"
        else:
            raise ValueError("source_table_header is not valid")

    def load_week_info(self, target_table, value_params):
        sql = f"""
                INSERT INTO {target_table}(
                                  ID
                                , STD_DT
                                , COIN_CD
                                , KOR_NM
                                , ENG_NM
                            )
                VALUES (
                              %(_id)s
                            , %(stdDt)s
                            , %(coinCode)s
                            , %(koreaName)s
                            , %(englishName)s
                    );
            """
        return self.exceute_many(sql, value_params)

    def load_week_time_dtl(self, target_table, value_params):
        sql = f"""
                INSERT INTO {target_table}(
                                ID
                              , SIGNAL_TIME
                            )
                VALUES (
                              %(_id)s
                            , %(signalTime)s
                    );
            """
        return self.exceute_many(sql, value_params)

    def load_week_day_dtl(self, target_table, value_params):
        sql = f"""
                INSERT INTO {target_table}(
                                ID
                              , SIGNAL_DAY
                            )
                VALUES (
                              %(_id)s
                            , %(signalDay)s
                    );
            """
        return self.exceute_many(sql, value_params)



    def load_week_time(self, source_table_header, date):
        signal_type = self._get_signal_type(source_table_header)

        sql = f"""
                INSERT INTO WEEK_SIGNAL_TIME
                (
                    	  ID 
                        , STD_DT
                        , SIGNAL_TYPE
                        , SIGNAL_TIME
                        , SIGNAL_DT
                        , SIGNAL_TM
                        , COIN_CD
                        , KOR_NM
                        , ENG_NM
                )
                SELECT 
                          A.ID
                        , A.STD_DT
                        , '{signal_type}'
                        , B.SIGNAL_TIME
                        , DATE_FORMAT(B.SIGNAL_TIME,'%Y-%m-%d')
                        , DATE_FORMAT(B.SIGNAL_TIME,'%T')
                        , A.COIN_CD
                        , A.KOR_NM 
                        , A.ENG_NM
                FROM 
                        {source_table_header}_INFO A
                        JOIN {source_table_header}_TIME_DTL B
                            ON A.ID = B.ID
                WHERE
		                A.STD_DT = '{date.strftime("%Y-%m-%d")}'
                ORDER BY
                        STD_DT DESC, KOR_NM, SIGNAL_TIME;
            """
        return self.execute(sql)

    def load_week_day(self, source_table_header, date):
        signal_type = self._get_signal_type(source_table_header)

        sql = f"""
                INSERT INTO WEEK_SIGNAL_DAY
                (
                    	  ID
                        , SIGNAL_TYPE
                        , SIGNAL_DAY
                        , STD_DT
                        , COIN_CD
                        , KOR_NM
                        , ENG_NM
                )
                SELECT 
                          A.ID
                        , '{signal_type}'
                        , B.SIGNAL_DAY
                        , A.STD_DT
                        , A.COIN_CD
                        , A.KOR_NM 
                        , A.ENG_NM
                FROM 
                        {source_table_header}_INFO A
                        JOIN {source_table_header}_DAY_DTL B
                            ON A.ID = B.ID
                WHERE
		                A.STD_DT = '{date.strftime("%Y-%m-%d")}'
                ORDER BY
                        STD_DT DESC, KOR_NM;
            """
        return self.execute(sql)