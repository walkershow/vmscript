#!/usr/bin/env python
# coding=utf-8
import telnetlib, sys
import re
import MySQLdb
import logging
import logging.config
from time import sleep
import requests
workpath = sys.path[0]
db_conn = None
logger = None

##################################################
def create_connection():
        ''' connect OK, return a connection handle; connect fail, return None'''
        db_host = "192.168.1.21"
        db_port = 3306
        db_user = "vm"
        db_pwd  = "123456"
        db_name = "vm4"
        db_charset = "utf8"
        global logger
        if logger is None:
                logger = get_default_logger()

        global db_conn
        if db_conn is None:
                try:
                        db_conn = MySQLdb.connect(host=db_host, port=db_port, user=db_user, \
                                        passwd=db_pwd, db=db_name, charset=db_charset, use_unicode=False)
                except Exception, e:
                        db_conn = None
                        logger.error('exception', exc_info = True)
                        if e.args is not None and len(e.args) > 0:
                                global db_lasterrcode
                                db_lasterrcode = e.args[0]
        return db_conn

def close_connection():
        global db_conn
        if db_conn is not None:
                try:
                        db_conn.close()
                except Exception, e:
                        logger.error('exception', exc_info = True)
                db_conn = None

def select_sqlwithdict(sql):
        ''' result set: None--fail, empty[]--OK, no any data set, else[]--OK, has a data set '''

        #logger.info(sql)
        if db_conn is None:
                create_connection()
        if db_conn is None:
                logger.error('db_conn is None')
                return None

        try:
                cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)
                cursor.execute(sql)
                ret = cursor.fetchall()
                cursor.close()
                db_conn.commit()
                return ret
        except Exception, e:
                logger.error('exception', exc_info = True)
                if e.args is not None and len(e.args) > 0:
                        global db_lasterrcode
                        db_lasterrcode = e.args[0]
                        if e.args[0] == 2003 or e.args[0] == 1152 or e.args[0] == 1042 or e.args[0] == 2006:
                                close_connection()
                return None


def execute_sql(sql):
        ''' -1--connect fail, -2--execute exception,
                0--execute OK, but no effect, >0--execute OK, effect rows '''

        logger.info(sql)
        if db_conn is None:
                create_connection()
        if db_conn is None:
                logger.error('db_conn is None')
                return -1

        try:
                cursor = db_conn.cursor()
                ret = cursor.execute(sql)
                cursor.close()
                logger.info("ret=%d" % ret)
                return ret
        except Exception, e:
                logger.error('exception', exc_info = True)
                if e.args is not None and len(e.args) > 0:
                        global db_lasterrcode
                        db_lasterrcode = e.args[0]
                        if e.args[0] == 2003 or e.args[0] == 1152 or e.args[0] == 1042 or e.args[0] == 2006:
                                close_connection()
                return -2

def commit():
    global db_conn
    db_conn.commit()


def get_default_logger():
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        # console logger
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter("[%(asctime)s] [%(process)d] [%(module)s::%(funcName)s::%(lineno)d] [%(levelname)s]: %(message)s")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger
##################################################

def telentRouter(HOST=None):
    tn = telnetlib.Telnet()
    try:
        tn.open(HOST)
    except:
        # print "Cannot open host"
        return

    return tn

def getMessage(tn):
    tn.read_until("Login:")
    tn.write("Default\n")
    tn.read_until("Password:")
    tn.write("\n")
    tn.read_until("hiper%")
    tn.write("show ip interface\n")
    tn.write("quit\n")
    tmp = tn.read_all()

    tn.close()
    return tmp

def processFormat(message):
    ipRe = r"->\s+(\S+)\s+"
    m = re.search(ipRe, message)
    return m.group(1)

def cleanDB():
    create_connection()
    execute_sql("delete from route_ip where 1=1")
    commit()
    close_connection()
    
def insertDb(ip_list):
    create_connection()
    dict_res = select_sqlwithdict("select ip from route_ip")
    list_res = [v["ip"] for v in dict_res]
    flag = False
    if(len(set(ip_list).difference(set(list_res))) > 0 or len(list_res) != len(ip_list)):
        execute_sql("delete from route_ip where true")
        clean_whitelist()
        sleep(10)
        for v in ip_list:
            execute_sql("insert into route_ip (ip) value (\"{0}\")".format(v))
            opr_whitelist(v, "add")
            sleep(10)
        commit()
    close_connection()

def clean_whitelist():
    iplist = get_whitelist()
    # print iplist
    for ip in iplist:
        opr_whitelist(ip, "del")

def get_whitelist():
    url = "http://www.moguproxy.com/proxy/ip/list?mobile={0}".format("13353089017")
    # print url
    r = requests.get(url)
    ret = r.json()
    iplist = []
    if ret["code"] == '0':
        iplist = ret["msg"]
    return iplist
        

def opr_whitelist(ip,type):
    url = "http://www.moguproxy.com/proxy/ip/{0}?mobile={1}&ip={2}".format(type,"13353089017",ip)
    # print url
    r = requests.get(url)
    ret = r.json()
    if ret["code"]!="0":
        # print ret["msg"]
        return False
    return True

    

if __name__ == "__main__":
    logging.config.fileConfig("router2.log.conf")
    logger = logging.getLogger()
    while(True):
        try:
            print("get ip")
            tn = telentRouter("192.168.1.251")
            message = getMessage(tn)
            message_list = message.split("\n")
            ip_list = []
            for v in message_list:
                if str.find(v, "->") != -1 and str.find(v, "ptp") != -1 and str.find(v, "ptpdia") == -1:
                    ip = processFormat(v)
                    ip_list.append(ip)
            insertDb(ip_list)
            print("sleep 20s then check whitelist") 
            sleep(20)
            wip_list = get_whitelist()
            if len(wip_list)<4:
                cleanDB()
            sleep(0.5)
        except Exception, e:
            logger.info(e)
