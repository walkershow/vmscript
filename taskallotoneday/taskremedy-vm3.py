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
from taskgroup import TaskGroup
import dbutil
import random

def get_last_resttimes(start_time, task_id):
    sql = '''select order_id, allot_times-ran_times from vm_task_allot_impl where 
    start_time<'{0}' and 
    task_id={1} order by start_time desc limit 1'''.format(start_time, task_id)
    res = dbutil.select_sql(sql)
    if not res or len(res)<=0:
        return None,None
    return res[0][0],res[0][1]

def remedy(order_id, task_id, remedy_num):
    sql = '''update vm_task_allot_impl set allot_times = allot_times+{0} where
    order_id ={1} and task_id={2}'''.format(remedy_num, order_id, task_id)
    print sql
    ret = dbutil.execute_sql(sql)

def record_remedy_status(order_id):
    sql = ''' update vm_task_allot_impl set remedy = 1 where
    order_id={0}'''.format(order_id)
    ret = dbutil.execute_sql(sql)
    
def handle():
    sql = '''select task_id,order_id,start_time from vm_task_allot_impl where 
            time_to_sec(NOW()) BETWEEN time_to_sec(start_time)
            AND time_to_sec(end_time) and remedy=0 order by task_id'''
    res = dbutil.select_sql(sql)
    if not res or len(res)<=0:
        return None
    for r in res:
        tid = r[0]
        oid = r[1]
        start_time = r[2]
        print "taskid", tid, "order id:", oid
        loid, remedy_num = get_last_resttimes(start_time ,tid)
        if oid == loid:
            print 'oid==loid', oid ,loid
            continue
        print "loid:", loid, "remedynum:", remedy_num
        if remedy_num:
            remedy(oid, tid, remedy_num)
            record_remedy_status(oid)
        

def handle2(tid):
    sql = '''select task_id,order_id,start_time from vm_task_allot_impl where 
            remedy=0 and task_id={0} and time_to_sec(end_time)<time_to_sec(NOW())  order by task_id,start_time'''.format(tid)
    res = dbutil.select_sql(sql)
    if not res or len(res)<=0:
        return None
    for r in res:
        tid = r[0]
        oid = r[1]
        start_time = r[2]
        print "taskid", tid, "order id:", oid
        loid, remedy_num = get_last_resttimes(start_time ,tid)
        if oid == loid:
            print 'oid==loid', oid ,loid
            continue
        print "loid:", loid, "remedynum:", remedy_num
        if remedy_num:
            remedy(oid, tid, remedy_num)
            record_remedy_status(oid)

if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm3"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    while True:
        try:
            handle()
            #handle2(50000)
        except Exception,e:
            print (e)
            time.sleep(10)
            continue
        time.sleep(90)

