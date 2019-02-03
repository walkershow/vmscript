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

    
def ranout_task():
    sql = '''select task_id,times,ran_times from vm_task_group a,vm_task b where a.task_id=b.id
    and b.status=1 and a.ran_times>=a.times and a.times>0'''
    res = dbutil.select_sql(sql)
    if not res or len(res)<1:
        return None
    tasks = {}
    for r in res:
        tasks.update({r[0]:(r[1],r[2])})
    return tasks

def current_times(task_id):
    sql = '''select count(*) from vm_cur_task where start_time>current_date and
    cur_task_id={0} and succ_time is not null'''.format(task_id)
    res = dbutil.select_sql(sql)
    if not res or len(res)<1:
        return None
    times = res[0][0]
    return times

def adjust_times():
    sql = '''update vm_task_group set ran_times={0} where task_id={1}'''
    sql_impl = '''update vm_task_allot_impl set allot_times={0} where time_to_sec(NOW()) BETWEEN time_to_sec(start_time)
                AND time_to_sec(end_time) and task_id={1}'''
    tasks = ranout_task()
    if not tasks:
        return 
    for id,times_set in tasks.items():
        times = times_set[0]
        ran_times = times_set[1]
        cur_times = current_times(id)
        logger.info("task_id:%d, times:%d,ran_times:%d,cur_times:%d", id,
                times,ran_times, cur_times)
        if ran_times == cur_times:
            logger.info("task_id:%d, ran_time eqaul cur_times",id)
            continue
        sqltmp = sql.format(cur_times,id)
        logger.info(sqltmp)
        ret = dbutil.execute_sql(sqltmp)
        left_times = times-cur_times
        logger.info("task_id:%d, left_times:%d", id, left_times)
        if left_times == 0:
            continue
        sqlimpl = sql_impl.format(left_times,id)
        logger.info(sqlimpl)
        ret = dbutil.execute_sql(sqlimpl)

    
def handle():
    sql = "delete from vm_cur_task where status =1 and \
    UNIX_TIMESTAMP(NOW())-UNIX_TIMESTAMP(start_time)>1800"
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
        

if __name__ == '__main__':
    logconf_file = 'adjust.log.conf'

    if not os.path.exists(logconf_file):
        print 'no exist:', options.logconf
        sys.exit(1)

    logging.config.fileConfig(logconf_file)
    global logger
    logger = logging.getLogger()
    
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm4"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    while True:
        try:
            adjust_times()
            #handle2(50000)
        except Exception,e:
	    logger.error('exception on main_loop', exc_info=True)
            time.sleep(120)
            continue
        time.sleep(300)

