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

def current_times(task_id):
    sql = '''select count(*) from vm_cur_task where start_time>current_date and
    cur_task_id={0} and succ_time is not null'''.format(task_id)
    res = dbutil.select_sql(sql)
    if not res or len(res)<1:
        return 0
    times = res[0][0]
    return times

def sum_impl_times(task_id, oid):
    sql = '''select sum(ran_times) from vm_task_allot_impl where task_id={0}
    and order_id<={1}'''.format(task_id, oid)
    print sql
    res = dbutil.select_sql(sql)
    if not res or len(res)<1:
        return 0
    times = res[0][0]
    if times is None:
        return 0
    return times


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

def adjust_rantimes(order_id, task_id, ran_times):
    sql = '''update vm_task_allot_impl set ran_times = ran_times-{0} where
    order_id ={1} and task_id={2}'''.format(ran_times, order_id, task_id)
    print sql
    ret = dbutil.execute_sql(sql)

def record_remedy_status(order_id):
    sql = ''' update vm_task_allot_impl set remedy = 1 where
    order_id={0}'''.format(order_id)
    ret = dbutil.execute_sql(sql)
    
def handle():
    sql = '''select task_id,order_id,start_time from vm_task_allot_impl a,
            vm_task b where 
            time_to_sec(NOW()) BETWEEN time_to_sec(a.start_time)
            AND time_to_sec(a.end_time) and a.remedy=0 and a.task_id=b.id and
            b.status=1 order by a.task_id'''
    print sql
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
        if loid is None:
            continue
        print "loid:", loid, "remedynum:", remedy_num
        sum_times = sum_impl_times(tid, loid)
        cur_times = current_times(tid)
        adjust_num = sum_times- cur_times
        print "taskid:", tid,"sum_impl:",  sum_times, "curtimes:", cur_times
        print "taskid:", tid, "adjust time:", adjust_num
        if adjust_num<=0:
            adjust_num = 0
        else:
            adjust_rantimes(oid, tid, adjust_num)
        if remedy_num:
            remedy(oid, tid, remedy_num)
            record_remedy_status(oid)
        
#for test
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
    dbutil.db_name = "vm4"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    while True:
        try:
        #if True:
            handle()
            #handle2(50000)
        except Exception,e:
            print (e)
            time.sleep(10)
            continue
        time.sleep(90)

