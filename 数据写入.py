import pymysql
from 数据抓取 import get_dat
from datetime import datetime


#数据表创建
def table_creat():
    mydb =pymysql.connect(
        host = "",
        port = "",
        user = "",
        password = "",
        database = "",
        charset = ''
    )

    mycursor = mydb.cursor()
    create_table = "CREATE TABLE day_dat (id int auto_increment primary key, totalissueno varchar(255), tradingday varchar(255), regname varchar(255), varname varchar(255), whabbrname varchar(255), wrtwghts int, wrtchange int, wghtunit varchar(255)"
    mycursor.execute(create_table)
    mycursor.close()
    mydb.close()

#数据插入
def dat_insert(sql_str,mydb):

    mycursor = mydb.cursor()
    mycursor.execute(sql_str)
    mycursor.commit()


if __name__ == '__main__':
    #table_creat()
    mydb = pymysql.connect(
        host="",
        port="",
        user="",
        password="",
        database="",
        charset=''
    )
    #每日晚八点50分更新当日数据
    while True:
        now = datetime.now()
        day_str = now.strftime("%Y%m%d")
        if now.hour == "20" and now.minute == "50":
            str_sql  = get_dat(day_str)
            dat_insert(str_sql, str_sql)
    mydb.close()
