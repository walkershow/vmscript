# -*- coding: utf-8 -*-
"""
@Author: coldplay
@Date: 2017-03-20 10:56:47
@Last Modified by: coldplay
@Last Modified time: 2018-01-31 21:29:21
"""

import time
import threading
import dbutil
import logging
import logging.config
import sys

"""重置任务状态"""


def handle():
    sql = """update vm_task_group a,
           vm_task b
    set b.status=0
    where a.task_id=b.id
      and a.start_time is not null
      and b.status>0
      and now() not between a.start_time and a.end_time"""
    print sql
    ret = dbutil.execute_sql(sql)
    print ("execute ret", ret)


def get_default_self_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # console self.logger
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(process)d] [%(module)s::%(funcName)s::%(lineno)d] [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == "__main__":
    dbutil.db_host = "192.168.1.21"
    dbutil.db_name = "vm4"
    dbutil.db_user = "dba"
    dbutil.db_port = 3306
    dbutil.db_pwd = "chinaU#2720"
    logger = get_default_self_logger()
    while True:
        try:
            handle()
            time.sleep(60)
        except:
            print ("some except happened")
            time.sleep(30)
