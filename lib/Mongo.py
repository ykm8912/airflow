from pymongo import MongoClient
from datetime import timedelta, datetime
import pandas as pd
from lib.config import MONGO_URI

class Mongo:
    def __init__(self, db):
        self.conn = MongoClient(
                                    MONGO_URI
                                    ,uuidRepresentation='standard')
        self.db = self.conn[db]

    def _find_day_yet_golden_cross(self, date):
        return self.db.day_yet_golden_cross.aggregate([
                    {
                        '$match' : {"stdDt": {"$gte": date.strftime("%Y-%m-%d")}}
                    },
                    {
                        '$group': {
                                    '_id' : ['$coinCode', '$koreaName', '$englishName'], 'maxCreatedTime' : {'$max' : '$goldenCrossTime'}
                        }
                    }
                ])

    def _find_max_values(self, col, type=None, min_dt=None):
        if col == "day_cross":
            match = {'type' : type}
        elif col == "day_pumping":
            match = {"stdDt": {"$gte": min_dt.to_pydatetime().strftime("%Y-%m-%d")}}

        return self.db[col].aggregate([
                    {
                        '$match' : match
                    },
                    {
                        '$group': {
                                    '_id' : ['$coinCode', '$koreaName', '$englishName'], 'maxCreatedTime' : {'$max' : '$createdTime'}
                        }
                    }
                ])

    def _find_week_time(self, col, date):
        monday = date - timedelta(days=date.weekday()+7)
        print(f"monday : {monday.strftime('%Y-%m-%d')}")

        if col == "day_pumping":
            code = "pumpingCode"
        elif col == "day_vol":
            code = "volCode"
        elif col == "day_cross":
            code = "crossCode"
        else:
            raise ValueError("col 값이 잘못되었습니다.")
            
        return  self.db[col].find(
                                    {
                                        "stdDt" : {"$gte": monday.strftime("%Y-%m-%d"), "$lt": date.strftime("%Y-%m-%d")}
                                        
                                    }
                                    ,{
                                        "_id": 0,
                                        "stdDt": 1,
                                        "stdDay": 1,
                                        code: 1,
                                        "coinCode": 1,
                                        "englishName": 1,
                                        "koreaName": 1,
                                        "createdTime": 1,
                                    }
                                )
    def _make_gc_pumping_df(self, date):
        gc_list = list(self._find_day_yet_golden_cross(date))
        min_dt = pd.DataFrame(gc_list)["maxCreatedTime"].min()
        pumping_list = list(self._find_max_values(col="day_pumping", min_dt=min_dt))
        return self._make_df(gc_list), self._make_df(pumping_list)

    def _make_df(self, source_list):
        df = pd.DataFrame(source_list)
        df[['coinCode', 'koreaName', 'englishName']] = pd.DataFrame(df['_id'].tolist(), index= df.index)
        df = df.drop(['_id'], axis=1, inplace=False)
        df = df[['coinCode', 'koreaName', 'englishName', 'maxCreatedTime']]
        return df

    def _make_created_time_list_values(self, source):
        return pd.DataFrame(source).groupby(["coinCode", "koreaName", "englishName"])["createdTime"]\
                .apply(list).reset_index(name="signalTimeList").sort_values(by="signalTimeList", ascending=True)

    def _make_day_list_values(self, source):
        return pd.DataFrame(source).groupby(["coinCode", "koreaName", "englishName"])["stdDay"]\
                .apply(list).reset_index(name="signalDayList").sort_values(by="signalDayList", ascending=True)

    def _merge_df(self, df_time_group, df_day_group):
        return pd.merge(df_time_group, df_day_group, on=["coinCode", "koreaName", "englishName"])

    def _make_gc_dc_df(self):
        dc_list = list(self._find_max_values(col="day_cross", type="deadcross"))
        gc_list = list(self._find_max_values(col="day_cross", type="goldencross"))
        return self._make_df(dc_list), self._make_df(gc_list)

    def _make_pumping_not_yet_df(self, date):
        gc_df, pumping_df = self._make_gc_pumping_df(date)
        gc_pumping_df = pd.merge(gc_df, pumping_df, on=['coinCode', 'koreaName', 'englishName'], how='left')
        pumping_not_yet_df = gc_pumping_df[gc_pumping_df['maxCreatedTime_x'] > gc_pumping_df['maxCreatedTime_y']]
        pumping_not_yet_df = pumping_not_yet_df.rename(columns={'maxCreatedTime_y': 'lastPumpingTime',
                                                                'maxCreatedTime_x': 'goldenCrossTime'})
        pumping_not_yet_df["createdTime"] = datetime.now() + timedelta(hours=9)
        pumping_not_yet_df.insert(0, "stdDt", date.strftime("%Y-%m-%d"))
        return pumping_not_yet_df
    
    def _make_pumping_yet_df(self, date):
        gc_df, pumping_df = self._make_gc_pumping_df(date)
        gc_pumping_df = pd.merge(gc_df, pumping_df, on=['coinCode', 'koreaName', 'englishName'], how='left')
        pumping_yet_df = gc_pumping_df[gc_pumping_df['maxCreatedTime_x'] < gc_pumping_df['maxCreatedTime_y']]
        pumping_yet_df = pumping_yet_df.rename(columns={'maxCreatedTime_y': 'pumpingTime',
                                                        'maxCreatedTime_x': 'goldenCrossTime'})
        pumping_yet_df["createdTime"] = datetime.now() + timedelta(hours=9)
        pumping_yet_df.insert(0, "stdDt", date.strftime("%Y-%m-%d"))
        return pumping_yet_df

    def _make_not_yet_golden_cross_list_values(self):
        dc_df, gc_df = self._make_gc_dc_df()
        dc_gc_df = pd.merge(dc_df, gc_df, on=['coinCode', 'koreaName', 'englishName'], how='left')
        gc_not_yet_df = dc_gc_df[dc_gc_df['maxCreatedTime_x'] > dc_gc_df['maxCreatedTime_y']]
        gc_not_yet_df = gc_not_yet_df.rename(columns={'maxCreatedTime_x': 'deadCrossTime'})
        gc_not_yet_df = gc_not_yet_df.drop(['maxCreatedTime_y'], axis=1, inplace=False)
        gc_not_yet_df["createdTime"] = datetime.now() + timedelta(hours=9)
        gc_not_yet_df.insert(0, "stdDt", gc_not_yet_df["createdTime"].dt.strftime("%Y-%m-%d"))
        gc_not_yet_df = gc_not_yet_df.sort_values(by='deadCrossTime', ascending=True)

        return gc_not_yet_df.to_dict('records')
    
    def _make_yet_golden_cross_list_values(self):
        dc_df, gc_df = self._make_gc_dc_df()
        dc_gc_df = pd.merge(dc_df, gc_df, on=['coinCode', 'koreaName', 'englishName'], how='left')
        gc_yet_df = dc_gc_df[dc_gc_df['maxCreatedTime_x'] < dc_gc_df['maxCreatedTime_y']]
        gc_yet_df = gc_yet_df.rename(columns={'maxCreatedTime_y': 'goldenCrossTime'})
        gc_yet_df = gc_yet_df.drop(['maxCreatedTime_x'], axis=1, inplace=False)
        gc_yet_df["createdTime"] = datetime.now() + timedelta(hours=9)
        gc_yet_df.insert(0, "stdDt", gc_yet_df["createdTime"].dt.strftime("%Y-%m-%d"))
        gc_yet_df = gc_yet_df.sort_values(by='goldenCrossTime', ascending=True)
        
        return gc_yet_df.to_dict('records')

    def _make_source(self, source_ori):
        source = list()
        for ori in source_ori:
            src = dict()
            src["stdDt"] = datetime.strftime(ori["createdTime"], "%Y-%m-%d")
            src["stdMon"] = int(datetime.strftime(ori["createdTime"], "%m"))
            src["stdDay"] = int(datetime.strftime(ori["createdTime"], "%w"))
            src["stdWeekOrd"] = int(datetime.strftime(ori["createdTime"], "%U"))
            src["stdTime"] = datetime.strftime(ori["createdTime"], "%H:%M:%S")

            for k, v in ori.items():
                src[k] = v
            source.append(src)
        
        return source

    def load_day_pumping_not_yet(self, date):
        try:
            source = self._make_pumping_not_yet_df(date)
            if len(source) == 0:
                print(f"day_pumping_not_yet 적재할 데이터가 없습니다.")
                return

            self.db["day_pumping_not_yet"].insert_many(source.to_dict('records'))
        except Exception as e:
            raise Exception(e)
        else:
            print(f"day_pumping_not_yet 적재 완료")
    
    def load_day_pumping_yet(self, date):
        try:
            source = self._make_pumping_yet_df(date)
            if len(source) == 0:
                print(f"day_pumping_yet 적재할 데이터가 없습니다.")
                return

            self.db["day_pumping_yet"].insert_many(source.to_dict('records'))
        except Exception as e:
            raise Exception(e)
        else:
            print(f"day_pumping_yet 적재 완료")

    def load_day_not_yet_golden_cross(self):
        try:
            source = self._make_not_yet_golden_cross_list_values()
            if len(source) == 0:
                print(f"day_not_yet_golden_cross 적재할 데이터가 없습니다.")
                return

            self.db["day_not_yet_golden_cross"].insert_many(source)
        except Exception as e:
            raise Exception(e)
        else:
            print(f"day_not_yet_golden_cross 적재 완료")

    def load_day_yet_golden_cross(self):
        try:
            source = self._make_yet_golden_cross_list_values()
            if len(source) == 0:
                print(f"day_yet_golden_cross 적재할 데이터가 없습니다.")
                return

            self.db["day_yet_golden_cross"].insert_many(source)
        except Exception as e:
            raise Exception(e)
        else:
            print(f"day_yet_golden_cross 적재 완료") 

    def load_week_vol_time(self, date):
        try:
            source = list(self._find_week_time("day_vol", date))
            if len(source) == 0:
                print(f"week_vol_time 적재할 데이터가 없습니다.")
                return

            df_merge = self._merge_df(self._make_created_time_list_values(source), self._make_day_list_values(source))
            df_merge["createdTime"] = datetime.now() + timedelta(hours=9)
            df_merge.insert(0, "stdDt", date.strftime("%Y-%m-%d"))
            self.db["week_vol_time"].insert_many(df_merge.to_dict("records"))
        except Exception as e:
            raise Exception(e)
        else:
            print(f"week_vol_time 적재 완료")

    def load_week_cross_time(self, date):
        def load_week_golden_cross_time(date):
            try:
                source = list(self._find_week_time("day_cross", date))
                if len(source) == 0:
                    print(f"week_golden_cross_time 적재할 데이터가 없습니다.")
                    return
                
                df_merge = self._merge_df(self._make_created_time_list_values(source), self._make_day_list_values(source))
                df_merge["createdTime"] = datetime.now() + timedelta(hours=9)
                df_merge.insert(0, "stdDt", date.strftime("%Y-%m-%d"))
                self.db["week_golden_cross_time"].insert_many(df_merge.to_dict("records"))
            except Exception as e:
                raise Exception(e)
            else:
                print(f"week_golden_cross_time 적재 완료")
        def load_week_dead_cross_time(date):
            try:
                source = list(self._find_week_time("day_cross", date))
                if len(source) == 0:
                    print(f"week_dead_cross_time 적재할 데이터가 없습니다.")
                    return
                
                df_merge = self._merge_df(self._make_created_time_list_values(source), self._make_day_list_values(source))
                df_merge["createdTime"] = datetime.now() + timedelta(hours=9)
                df_merge.insert(0, "stdDt", date.strftime("%Y-%m-%d"))
                self.db["week_dead_cross_time"].insert_many(df_merge.to_dict("records"))
            except Exception as e:
                raise Exception(e)
            else:
                print(f"week_dead_cross_time 적재 완료")
        
        load_week_golden_cross_time(date)
        load_week_dead_cross_time(date)

    def load_week_pumping_time(self, date):
        try:
            source = list(self._find_week_time("day_pumping", date))
            if len(source) == 0:
                print(f"week_pumping_time 적재할 데이터가 없습니다.")
                return
            
            df_merge = self._merge_df(self._make_created_time_list_values(source), self._make_day_list_values(source))
            df_merge["createdTime"] = datetime.now() + timedelta(hours=9)
            df_merge.insert(0, "stdDt", date.strftime("%Y-%m-%d"))
            self.db["week_pumping_time"].insert_many(df_merge.to_dict("records"))
        except Exception as e:
            raise Exception(e)
        else:
            print(f"week_pumping_time 적재 완료")

    def find_week_source_col(self, col, date):
        def find_week_source(col, date):
            return self.db[col].find(
                                        {
                                            "stdDt": date.strftime("%Y-%m-%d")
                                        }
                                        ,{
                                        }
                                    )

        try:
            source = list(find_week_source(col, date))
            return source
        except Exception as e:
            raise Exception(e)

    def load_day_coins(self, col, date):
        def validate_col(col):
            if col == "pumping":
                target = "day_pumping"
            elif col == "vol":
                target = "day_vol"
            elif col == "cross":
                target = "day_cross"
            else:
                raise ValueError("col is not valid")
            
            return target

        def find_day_coins(col, date):
            return self.db[col].find(
                                        {
                                            "createdTime": {
                                                "$gte": date,
                                                "$lt": date + timedelta(days=1)
                                            }
                                        }
                                        ,{
                                            "_id": 0
                                        }
                                    )

        try:
            target = validate_col(col)
            source_ori = list(find_day_coins(col, date))
            source = self._make_source(source_ori)

            if len(source) == 0:
                print(f"{target} 적재할 데이터가 없습니다.")
                return

            self.db[target].insert_many(source)
        except Exception as e:
            raise Exception(e)
        else:
            print(f"{target} 적재 완료")