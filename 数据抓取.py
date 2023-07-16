import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re


#初始化数据库插入语句
sql_str = "insert into day_dat(totalissueno,tradingday,regname,varname,whabbrname,wrtwghts,wrtchange,wghtunit,update_date) values"
#初始化类目和单位
varname = ""
wghtunit = ""


#获取数据
def get_dat(day_str):
    global sql_str
    #20140519数据加载方式更新为ajax
    if day_str>="20140519":
        dat_url = "https://www.shfe.com.cn/data/dailydata/{0}dailystock.dat".format(day_str)
        header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"}
        daily_stock = requests.get(url=dat_url,headers=header)
        req_staus = daily_stock.status_code
        if req_staus == 404:
            print("{}数据不存在".format(day_str))
            return ""
        else:
            print("{}数据获取成功".format(day_str))
        daily_stock = daily_stock.json()

        tradingday = daily_stock['o_tradingday']
        totalissueno = daily_stock['o_totalissueno']

        for d in daily_stock['o_cursor']:
            varname = d['VARNAME'].split("$$")[0]
            regname = d['REGNAME'].split("$$")[0]
            whabbrname = d['WHABBRNAME'].split("$$")[0]
            wrtwghts = d['WRTWGHTS']
            wrtchange = d['WRTCHANGE']
            wghtunit = d['WGHTUNIT']
            if wghtunit == "0":
                wghtunit = '克'
            elif wghtunit == "1":
                wghtunit = '千克'
            elif wghtunit == "2":
                wghtunit = '吨'
            elif wghtunit == "3":
                wghtunit = '桶'
            else:
                wghtunit = ''
            sql_str = sql_str + "({0},{1},{2},{3},{4},{5},{6},{7}),".format(totalissueno, tradingday, regname, varname,
                                                                                 whabbrname, wrtwghts, wrtchange, wghtunit
                                                                                 )
    else:
        #旧版本数据获取
        dat_url = "https://www.shfe.com.cn/data/dailydata/{0}dailystock.html".format(day_str)
        header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"}
        daily_stock = requests.get(url=dat_url, headers=header)
        req_staus = daily_stock.status_code
        if req_staus==404:
            print("{}数据不存在".format(day_str))
            return ""
        else:
            print("{}数据获取成功".format(day_str))
        text = daily_stock.text
        soup = BeautifulSoup(text, 'html.parser')
        table = soup.find('table')
        df = pd.read_html(str(table))[0]
        df = df[(df[1]!="合 计")&(df[1]!="总 计")&(df[1]!="保税总计")&(df[1]!="完税总计")&(df[1]!="合计")&(df[1]!="仓库")&(df[1]!="上期所指定")]
        df = df[pd.isna(df[0])==False]

        try:
            df['totalissueno'] = re.findall("\d+",df.iloc[1,2])[-1]
            df['tradingday'] = pd.to_datetime(df.iloc[1, 0].split("日期：")[-1], format="%Y年%m月%d日").strftime('%Y%m%d')
        except:

            df['totalissueno'] = re.findall("\d+", df.iloc[1, 3])[-1]
            df['tradingday'] = pd.to_datetime(df.iloc[1,0].split("日期：")[-1], format="%Y年%m月%d日").strftime('%Y%m%d')
        df[3].fillna(0,inplace=True)
        df = df[2:-2]
        df.to_csv("t.csv")
        def get_sql_str(x):
            global varname
            global wghtunit
            global sql_str

            if pd.isna(x[1]):
                varname = x[0]
                wghtunit = x[3].split("：")[-1]
            elif isinstance(x[2],str):
                sql_str = sql_str + "({0},{1},{2},{3},{4},{5},{6},{7}),".format(x['totalissueno'], x['tradingday'],
                                                                                x[0],
                                                                                varname,
                                                                                x[1], x[2], x[3],
                                                                                wghtunit
                                                                                )

        df.apply(get_sql_str,axis=1)

    return sql_str


if __name__ == '__main__':
    #20081006第一期，20140519获取数据方式改变
    dat = get_dat("20081006")
    print(dat)

