# -*- coding: utf-8 -*-
'''
@Author: coldplay 
@Date: 2017-05-13 16:32:04 
@Last Modified by:   coldplay 
@Last Modified time: 2017-05-13 16:32:04 
'''

import sys
import datetime
import os
import shutil
import time
import logging
import logging.config
import dbutil

    
def handle2():
    sql = "delete from vm_cur_task where status =1 and \
    UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(start_time)>2400"
    ret = dbutil.execute_sql(sql)
    print(sql, ret)
    sql = "delete from vm_cur_task where status =-1 and \
    UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(start_time)>180"
    ret = dbutil.execute_sql(sql)
    print(sql, ret)
    sql = "delete from vm_cur_task where status =2 and \
    UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(succ_time)>1200"
    ret = dbutil.execute_sql(sql)
    print(sql, ret)
        

def handle():
    sql = "update vm_cur_task set status=30 where status =1 and \
    UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(start_time)>2400"
    ret = dbutil.execute_sql(sql)
    print(sql, ret)
    sql = "update vm_cur_task set status=30 where status =-1 and \
    UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(start_time)>180"
    ret = dbutil.execute_sql(sql)
    print(sql, ret)
    sql = "update vm_cur_task set status=30 where status =2 and \
    UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(succ_time)>1200"
    ret = dbutil.execute_sql(sql)
    print(sql, ret)
    sql = "update vm_cur_task set status=30 where status =0 and \
    UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(start_time)>1200"
    ret = dbutil.execute_sql(sql)
    print(sql, ret)
        

if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm4"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    while True:
        try:
            handle()
            #handle2(50000)
        except Exception,e:
            print (e)
            time.sleep(20)
            continue
        time.sleep(120)

