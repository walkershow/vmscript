# -*- coding: utf-8 -*-
"""
@Author: coldplay 
@Date: 2017-05-13 16:32:04 
@Last Modified by:   coldplay 
@Last Modified time: 2017-05-13 16:32:04 
"""

import sys
import datetime
import os
import shutil
import time

# import logging
# import logging.config
from loguru import logger
import dbutil
# from loguru import logger

logger.add("sub.log", rotation="13:00")


def set_ran_times(task_id, times):
    sql = """
    update vm_task_allot_impl
    set allot_times=allot_times+{1}
    where 
      task_id={0}
      and time_to_sec(now()) between time_to_sec(start_time) and time_to_sec(end_time)
            """.format(
        task_id, times
    )
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret < 0:
        raise Exception("%s excute error;ret:%d" % (sql, ret))


def get_time_range(task_id):
    sql = """
    select start_time,
           end_time, now(), order_id,templ_id
    from vm_task_allot_impl
    where now() between start_time and end_time
      and task_id={0}
    """.format(
        task_id
    )
    res = dbutil.select_sql(sql)
    if not res or len(res) < 1:
        return None, None, None, None, None
    return res[0][0], res[0][1], res[0][2], res[0][3], res[0][4]


def get_running_task():
    sql = """
    select a.id
    from vm_task a,
         vm_task_group b
    where a.id=b.task_id
      and a.status=1
      and b.times>0
      """
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if not res or len(res) < 1:
        return []
    ids = []
    for r in res:
        ids.append(r[0])
    return ids


def is_task_done(task_id, start_time, end_time):
    sql = """
    select count(*)
    from vm_cur_task
    where status in(-1,1,2)
      and start_time between '{0}' and '{1}'
      and cur_task_id={2}
    """.format(
        start_time, end_time, task_id
    )
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if not res or len(res) < 1:
        return True
    count = res[0][0]
    if count == 0:
        return True
    return False


def set_task_done(task_id, order_id, ran_times):
    """TODO: Docstring for set_task_done.
    :returns: TODO

    """
    sql = """
    update vm_task_allot_impl set is_done=1,ran_times={1} where order_id={0}
    ;""".format(
        order_id, ran_times
    )
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret < 0:
        raise Exception("update wrong:{0}".format(order_id))
    sql = """ select allot_times-ran_times from vm_task_allot_impl where order_id={0}""".format(
        order_id
    )
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if not res or len(res) < 1:
        return None
    count = res[0][0]
    logger.info("id:", task_id, "ran left", " count:", count)
    if count != 0:
        set_ran_times(task_id, count)


def get_succ_count(task_id, start_time, end_time):
    sql = """
    select count(*)
    from vm_cur_task
    where (succ_time is not null or status in(-1,1,2,4))
      and start_time between '{0}' and '{1}'
      and cur_task_id={2}
    """.format(
        start_time, end_time, task_id
    )
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if not res or len(res) < 1:
        return None
    count = res[0][0]
    logger.info("id:", task_id, "is handling", " count:", count)
    return count


def get_day_succ_count(task_id):
    sql = """
    select count(*)
    from vm_cur_task
    where (succ_time is not null or status in(-1,0,1,2,4))
      and start_time>current_date 
      and cur_task_id={0}
    """.format(
        task_id
    )
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if not res or len(res) < 1:
        return None
    count = res[0][0]
    logger.info("id:", task_id, "is handling", " count:", count)
    return count


def set_group_rantimes(task_id):
    count = get_day_succ_count(task_id)
    sql = """update vm_task_group set ran_times={0} where task_id={1}""".format(
        count, task_id
    )
    logger.info(sql)
    ret = dbutil.execute_sql(sql)
    if ret < 0:
        raise (Exception, "%s excute error;ret:%d" % (sql, ret))


def get_notdone_task_timerange(task_id, start_time):
    sql = """
    select start_time,
           end_time,
           order_id
    from vm_task_allot_impl
    where is_done=0
      and task_id={0}
      and allot_times>0
      and start_time<'{1}'
    ;""".format(
        task_id, start_time
    )
    logger.info(sql)
    res = dbutil.select_sql(sql)
    if not res or len(res) < 1:
        return None
    return res


def handle2():
    """判断当前任务该时间段之前的时间段是否没有运行中的任务，如果没有，计算该时间段的成功数
       更新is_done 为1，后续不会继续处理这个时间段
       计算该时间段是否有剩余次数（可正负），然后将计算结果与当前正在运行的时间段的allot_time相加（补次数）
       最后每个时间段都计算下当天任务的成功总数，更新到vm_task_group的ran_times字段
    """
    r_ids = get_running_task()
    for id in r_ids:
        logger.info("id:", id, "-------------")
        start_time, end_time, now_time, time_id, templ_id = get_time_range(id)
        if start_time is None:
            continue
        if templ_id != 99:
            res = get_notdone_task_timerange(id, start_time)
            if res is None:
                msg = "{0} [{1}] no task not done".format(id, start_time)
                logger.info(msg)
                continue
            for r in res:
                done_start_time, done_end_time, order_id = r
                ret = is_task_done(id, done_start_time, done_end_time)
                if ret:
                    msg = "{0} [{1}--{2}]:没有在运行任务,开始统计".format(
                        id, done_start_time, done_end_time
                    )
                    logger.info(msg)
                    succ_count = get_succ_count(id, done_start_time, done_end_time)
                    set_task_done(id, order_id, succ_count)
                    msg = "{0} [{1}--{2}]:统计结束".format(
                        id, done_start_time, done_end_time
                    )
                    logger.info(msg)
        else:
            succ_count = get_succ_count(id, start_time, end_time)
            set_ran_times(id, succ_count)
        set_group_rantimes(id)


if __name__ == "__main__":
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm4"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    while True:
        # if True:
        try:
            handle2()
        except Exception as e:
            logger.info(e)
            time.sleep(20)
            continue
        time.sleep(20)
