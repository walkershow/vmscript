# -*- coding: utf-8 -*-
'''
@Author: coldplay 
@Date: 2017-05-12 16:29:06 
@Last Modified by:   coldplay 
@Last Modified time: 2017-05-12 16:29:06 
'''

import sys
import datetime
import os
import time
import threading
import logging
import logging.config
import dbutil

logger = None


class ShowTask(object):
    '''任务
    '''
    def __init__(self, db,logger):
        self.db = db 
        self.logger = logger
    def exist_showtask(self, gid,tid):
        sql = '''select 1 from vm_task_allot_impl where id={0} and
        task_id={1}'''.format(gid, tid)
        res = self.db.select_sql(sql)
        if res:
            return True
        return False
        

    def impl_allot_time(self, gid, tid, times):
        sql = '''insert into vm_task_allot_impl(id,task_id,start_time,end_time,allot_times,templ_id,templ_sub_id, update_time)
            values(%d,%d,current_date
                       ,date_add(current_date, interval 1 day),
            %d,99, 1, CURRENT_TIMESTAMP) on duplicate key update allot_times=%d''' 
        sql_impl = sql%(gid,tid, times,times )
        print sql_impl
        ret = self.db.execute_sql(sql_impl)
        if ret<0:
            raise Exception,"%s excute error;ret:%d"%(sql_impl, ret)

    def update_allot_time(self, gid,tid,times):
        sql = '''update vm_task_allot_impl set allot_times={0} where id={1} and
        task_id={2}'''.format(times, gid, tid)
        ret = self.db.execute_sql(sql)
        if ret<0:
            raise Exception,"%s excute error;ret:%d"%(sql, ret)
        

    def gen_allot_time(self ):
        sql = '''select task_group_id,task_id,show_times/click_gap as allot_times
        from site_show_times where status=1 order by id'''
        print sql
        res = self.db.select_sql(sql)
        if not res or len(res)<=0:
           return None
        for r in res:
            gid = r[0]
            tid = r[1]
            allot_time = r[2]
            if not self.exist_showtask(gid, tid):
                self.impl_allot_time(gid, tid, allot_time)
            else:
                self.update_allot_time(gid, tid, allot_time)

    def del_allot_time(self,gid,tid):
        sql = '''delete from vm_task_allot_impl where id={0} and
        task_id={1}'''.format(gid, tid)
        ret = self.db.execute_sql(sql)
        if ret<0:
            raise Exception,"%s excute error;ret:%d"%(sql_impl, ret)


    def clean_show_task_allot(self):
        sql = '''select task_group_id,task_id,show_times/click_gap as allot_times
        from site_show_times where status=1 order by id'''
        print sql
        res = self.db.select_sql(sql)
        if not res or len(res)<=0:
           return None
        for r in res:
            gid = r[0]
            tid = r[1]
            self.del_allot_time(gid, tid)

        
    def reset_when_newday(self):
        '''新的一天重置所有运行次数'''
        today = datetime.date.today()
        if today != self.cur_date:
            self.cur_date = today
            self.clean_show_task_allot()


    def main_loop(self):
        while True:
            reset_when_newday()
            st = ShowTask(dbutil,logger)
            st.gen_allot_time()
            time.sleep(10)
            



def main():
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm3"
    dbutil.db_user = "vm"
    dbutil.db_port = 3306
    dbutil.db_pwd = "123456"
    logconf_file = 'allotshowtask.log.conf'

    if not os.path.exists(logconf_file):
        print 'no exist:', options.logconf
        sys.exit(1)

    logging.config.fileConfig(logconf_file)
    global logger
    logger = logging.getLogger()
    st = ShowTask(dbutil,logger)
    st.gen_allot_time()

if __name__ == '__main__':
    main()
