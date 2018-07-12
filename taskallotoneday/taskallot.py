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
import fnmatch
import logging
import logging.config
from taskgroup import TaskGroup
import dbutil
import random

logger = None

class TaskAllotError(Exception):
    pass

class TaskAllot(object):
    '''任务分配'''

    #cur_date = None

    def __init__(self,want_init, server_id, db):
        self.db = db 
        self.cur_date = None
        self.want_init = want_init
        self.server_id = server_id

    def reset_when_newday(self):
        '''新的一天重置所有运行次数'''
        today = datetime.date.today()
        # print today,self.cur_date
        if today != self.cur_date:
            
            #统一到一个w = 1的进程进行更新
            if self.want_init == 1:
                print "start new day to reinit..."
                #logger.info("start new day to reinit...")
                TaskGroup.reset_rantimes_today(self.db)
                TaskGroup.reset_rantimes_allot_impl(self.db)
                self.cur_date = today
                print "cur_date",self.cur_date
                print "end new day to reinit..."
                #logger.info("end new day to reinit...")

if __name__ == '__main__':
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm-test2"
    dbutil.db_user = "dba"
    dbutil.db_port = 3310
    dbutil.db_pwd = "chinaU#2720"
    #t=TaskAllot(0, 1, dbutil)
    task_group_id = None
    print len(sys.argv)
    if len(sys.argv)>1:
        task_group_id = int(sys.argv[1])
        print task_group_id
        TaskGroup.reset_rantimes_by_task_group_id(dbutil, task_group_id)
        TaskGroup.reset_rantimes_allot_impl(dbutil, task_group_id)
    t=TaskAllot(1,1, dbutil)
    if task_group_id :
        exit(0)
    while True:
        t.reset_when_newday()
        # try:
        #     t.reset_when_newday()
        #     time.sleep(10)
        # except Exception,e:
        #     print "except",e
        #     time.sleep(5)
        #     continue
