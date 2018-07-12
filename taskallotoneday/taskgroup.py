# -*- coding: utf-8 -*-
'''
@Author: coldplay 
@Date: 2017-05-13 11:39:55 
@Last Modified by:   coldplay 
@Last Modified time: 2017-05-13 11:39:55 
'''

from __future__ import division
import sys
import datetime
import os
import shutil
import time
import logging
import logging.config
import dbutil
from random import choice
logger = None

class TaskGroupError(Exception):
    pass

class TaskGroup(object):
    '''任务组'''
    tasks = []
    def __init__(self, id,  db):
        self.id = id
        self.db = db 

    @staticmethod
    def getDefaultTask(db):
        sql = "select id,task_id,ran_times from vm_task_group where  id=0"
        res = db.select_sql(sql)
        if not res:
            raise TaskGroupError,"%s sql get empty res"%(sql)
        row = res[0]
        task = {'task_id':row[1], 'start_time':0, 'end_time':0, 'times':9999999, 'ran_times':row[2], 'is_default': True}
        return Task(task["task_id"], True, db)
        
    def add_ran_times(self, task_id):
        sql ="update vm_task_group set ran_times=ran_times+1 where id=%d and task_id=%d"%(self.id, task_id)
        print sql
        ret = self.db.execute_sql(sql)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql, ret)

    @staticmethod        
    def add_default_ran_times(db):
        sql ="update vm_task_group set ran_times=ran_times+1 where id=0"
        ret = db.execute_sql(sql)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql, ret)

    #如果刚好过那个时间点,次数可能会对不上
    def add_impl_ran_times(self, task_id):
        sql ="update vm_task_allot_impl set ran_times=ran_times+1 where id=%d and task_id=%d \
             and time_to_sec(NOW()) between time_to_sec(start_time) and time_to_sec(end_time)"%(self.id, task_id)

        ret = self.db.execute_sql(sql)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql, ret)

    @staticmethod
    def impl_task_templ(db, task_group_id=None):
        sql = ""
        if not task_group_id: 
            sql_trunc =" truncate table vm_task_allot_impl_tmp"
            ret = db.execute_sql(sql_trunc)
            if ret<0:
                raise TaskGroupError,"%s excute error;ret:%d"%(sql_trunc, ret)
            sql = "select id,sum(times) as total,templ_id from vm_task_group where id>0 group by id order by id"
        else:
            sql_del = "delete from vm_task_allot_impl_tmp where id=%d"%(task_group_id)
            ret = db.execute_sql(sql_del)
            if ret<0:
                raise TaskGroupError,"%s excute error;ret:%d"%(sql_del, ret)
            sql = "select id,sum(times) as total,templ_id from vm_task_group where id=%d group by id order by id"%(task_group_id)
        sql_templ = "select percent,time_to_sec(start_time),time_to_sec(end_time),id,sub_id,detail_id from vm_task_allot_templ where id=%d order by sub_id"
        sql_alltask = "select task_id, times from vm_task_group  where id=%d "
        #sql_alltask = "select task_id, times from vm_task_group a,vm_task b where id=%d and a.task_id=b.id and b.status=1"

        res = db.select_sql(sql)
        print res
        for r in res:
            print r 
            id = r[0]
            total = r[1]
            templ_id = r[2]
            sql = sql_alltask%(id)
            print "task_group_id:", id, "-----total:", total
            if total == 0 or total is None:
                print "continue to next "
                continue
            # print "all_task:",sql
            res_alltask = db.select_sql(sql)
            task_dict = {}
            task_templ_dict = {}
            for t in res_alltask:
                if t[1] > 0:
                    task_dict[t[0]] = t[1]
                    task_templ_dict[t[0]] = 0
            print "task_dict:",task_dict
            sql = sql_templ%(templ_id)
            print "templ:",sql
            res = db.select_sql(sql)
            for r in res:
                print "templ row:", r
                p = r[0]
                start_time = int(r[1])
                end_time = int(r[2])
                # print "start_time,end_time",start_time,end_time
                templ_id = r[3]
                templ_sub_id = r[4]
                detail_id = r[5]
                cur_allot_num = int(round(total * p/100))
                print "cur_allot_num", cur_allot_num
                
                if cur_allot_num == 0:
                    continue
                allot_total = cur_allot_num
                # for i in range(cur_allot_num):
                while True:
                    if allot_total <= 0:
                        break
                    if not task_dict:
                        break
                    for t, v in task_dict.items():
                        task_id = t
                    # task_id = choice(task_dict.keys())
                        task_dict[task_id]= task_dict[task_id]- 1
                        task_templ_dict[task_id]= task_templ_dict[task_id] + 1
                        allot_total = allot_total - 1
                        # print task_dict
                        cnt = task_dict[task_id]
                        if cnt <=0:
                            task_dict.pop(task_id)
                        if allot_total <= 0:
                            break
                print task_templ_dict
                for t,n in task_templ_dict.items():
                    allot_times = n
                    if allot_times == 0:
                        continue
                    task_id = t
                    sql = '''insert into vm_task_allot_impl_tmp(id,task_id,start_time,end_time,allot_times,templ_id,templ_sub_id,detail_id, update_time)
                        values(%d,%d,sec_to_time(%d),sec_to_time(%d),%d,%d, %d,%d, CURRENT_TIMESTAMP) on duplicate key update allot_times=allot_times+1 ''' 
                    sql_impl = sql%(id,task_id,start_time,end_time, allot_times, templ_id, templ_sub_id,detail_id)
                    # print sql_impl
                    task_templ_dict[task_id] = 0
                    ret = db.execute_sql(sql_impl)
                    if ret<0:
                        raise TaskGroupError,"%s excute error;ret:%d"%(sql_impl, ret)
            print "=========task_group_id:", id, "-----total:", total," finish!!!!"
    
    @staticmethod
    def impl_task_templ_detail(db, task_group_id=None):
        sql = ""
        if not task_group_id:
            sql_trunc =" truncate table vm_task_allot_impl"
            ret = db.execute_sql(sql_trunc)
            if ret<0:
                raise TaskGroupError,"%s excute error;ret:%d"%(sql_impl, ret)
            sql = '''select id,task_id,allot_times,templ_id,templ_sub_id,detail_id,time_to_sec(start_time),time_to_sec(end_time) from vm_task_allot_impl_tmp order by id,task_id'''
        else:
            sql_del = "delete from vm_task_allot_impl where id=%d"%(task_group_id)
            ret = db.execute_sql(sql_del)
            if ret<0:
                raise TaskGroupError,"%s excute error;ret:%d"%(sql_del, ret)
            sql = '''select id,task_id,allot_times,templ_id,templ_sub_id,detail_id,time_to_sec(start_time),time_to_sec(end_time) 
            from vm_task_allot_impl_tmp where id=%d order by id,task_id'''%(task_group_id)
        res = db.select_sql(sql)
        for r in res:
            id,task_id,allot_times,templ_id,templ_sub_id,detail_id,start_time,end_time = r
            # print r
            # print allot_times
            sql = "select id,start_min,end_min from vm_task_allot_templ_detail where id=%d order by sub_id"%(detail_id)
            res = db.select_sql(sql)
            if not res:
                print sql, " empty"
                continue
            pos = len(res) + 1
            for td in res:
                start = td[1]
                end = td[2]
                pos = pos - 1
                one_times = int(round(allot_times/pos))
                if one_times<=0:
                    continue

                sql = '''insert into vm_task_allot_impl(id,task_id,start_time,end_time,allot_times,templ_id,templ_sub_id, update_time)
                    values(%d,%d,date_add(sec_to_time(%d), interval %d minute),date_add(sec_to_time(%d), interval %d minute),
                    %d,%d, %d, CURRENT_TIMESTAMP) on duplicate key update allot_times=allot_times+1 ''' 
                sql_impl = sql%(id,task_id,start_time, start, start_time, end, one_times, templ_id, templ_sub_id)
                # print sql_impl
                ret = db.execute_sql(sql_impl)
                if ret<0:
                    raise TaskGroupError,"%s excute error;ret:%d"%(sql_impl, ret)
                allot_times = allot_times - one_times

    @staticmethod
    def reset_rantimes_by_task_group_id(db, task_group_id):
        '''更新当天任务'''

        sql ="update vm_task_group set times=FLOOR(times_start_range + (RAND() * (times_end_range-times_start_range)))" \
        "where templ_id>0 and id=%d"%(task_group_id) 
        print sql
        ret = db.execute_sql(sql)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql, ret)
            
    @staticmethod
    def reset_rantimes_today(db):
        '''更新当天任务'''

        sql ="update vm_task_group set ran_times_lastday = ran_times ,ran_times=0,allot_times=0, \
            times=FLOOR(times_start_range + (RAND() * (times_end_range-times_start_range))) where templ_id>0" 
        ret = db.execute_sql(sql)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql, ret)

    @staticmethod
    def reset_rantimes_alltask(db):
        '''更新所有任务包括跨天'''

        sql ="update vm_task_group set ran_times_lastday = ran_times ,ran_times=0,allot_times=0, \
            times=FLOOR(times_start_range + (RAND() * (times_end_range-times_start_range))) " 
        ret = db.execute_sql(sql)
        if ret<0:
            raise TaskGroupError,"%s excute error;ret:%d"%(sql, ret)

    @staticmethod
    def reset_rantimes_allot_impl(db, task_group_id=None):
        TaskGroup.impl_task_templ(db, task_group_id)
        TaskGroup.impl_task_templ_detail(db, task_group_id)
        
if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm-test"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    t=TaskGroup(111,dbutil)
    # TaskGroup.reset_rantimes_allot_impl(dbutil)
    t.add_default_ran_times(dbutil)
    t.add_impl_ran_times(111)
    t.add_ran_times(111)
    # t.choose_vaild_task()
