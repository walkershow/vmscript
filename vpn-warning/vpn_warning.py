# -*- coding:utf-8 -*-
import socket
import time
import dbutil
import json
import smtplib
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
from email.mime.text import MIMEText 


dbutil.db_host = "192.168.1.21"
dbutil.db_name = "vm4"
dbutil.db_user = "vm"
dbutil.db_port = 3306
dbutil.db_pwd = "123456"

def _format_addr(s): 
    name, addr = parseaddr(s) 
    return formataddr((Header(name, 'utf-8').encode(), addr)) 
 
def send_mail(subject, to, msg, is_html): 
    try: 
        msg = is_html and MIMEText(msg, 'html', 'utf-8') or MIMEText(msg, 'plain', 'utf-8') 
 
        msg['From'] = _format_addr('6A管理员 <%s>' % "rs1@990g.com") 
        msg['To'] = _format_addr(to.split("@")[0] + ' <%s>' % to) 
        msg['Subject'] = Header(subject, 'utf-8').encode() 
        print(msg.as_string()) 
 
        server = smtplib.SMTP_SSL("smtp.qq.com", 465) 
        #server.starttls() 
        server.set_debuglevel(1) 
        # server.login("rs1@990g.com", "Chinau234") 
        # server.sendmail("rs1@990g.com", [to], msg.as_string()) 
        server.login("3453148073@qq.com", "yczqknlhjhndcjgd") 
        server.sendmail("3453148073@qq.com", [to], msg.as_string()) 
        server.quit() 
        return True 
    except Exception, e: 
        print e
        # logger.error("===============send_mail failed:%s", e) 
        return False 


def get_timeout_res(timeout):
    sql = '''SELECT area from vpn_status where UNIX_TIMESTAMP(update_time)>UNIX_TIMESTAMP(current_date)-3600*24  
    and TIMESTAMPDIFF(minute,  update_time, CURRENT_TIMESTAMP())>60'''
    res = dbutil.select_sql(sql)
    timeout_array = []
    timeout_none = []
    if not res or len(res)<=0:
        return None
    for r in res:
        print r
        server_id = r[0]
        timeout_array.append(server_id)
    if not timeout_array:
        return None
    s1 = json.dumps(timeout_array)
    s1_msg = "超时:"+s1

    msg = s1_msg 
    print msg
    return msg

def get_server_list():
    sql = "select id from vm_server_list where status=1"
    res= dbutil.select_sql(sql)
    slist = []
    for r in res:
        slist.append(r[0])
    return slist

def get_proxy_server_list():
    sql = "select id from vm_server_list where proxy_mode=1"
    res= dbutil.select_sql(sql)
    slist = []
    for r in res:
        slist.append(r[0])
    return slist


def get_no_succ_vm():
    no_succ_dict= {}
    server_list = get_server_list()
    for s in server_list:
        for i in range(1,7):
            sql = '''select 1 from vm_cur_task where
            UNIX_TIMESTAMP(update_time)>UNIX_TIMESTAMP(now())-3600
            and succ_time is not null  and server_id={0} and vm_id={1}'''.format(s,i)
            res = dbutil.select_sql(sql)
            if not res or len(res)<=0:
                if not no_succ_dict.has_key(s):
                    no_succ_dict[s]=[i]
                else:
                    no_succ_dict[s].append(i)
    if no_succ_dict:
        s1 = json.dumps(no_succ_dict)
        s1_msg = "1小时内没有成功的vm:"+s1
        return s1_msg
    return None


def get_no_succ_proxy_vm():
    no_succ_dict= {}
    server_list = get_proxy_server_list()
    for s in server_list:
        for i in range(1,7):
            sql = '''select 1 from vm_cur_task where
            UNIX_TIMESTAMP(update_time)>UNIX_TIMESTAMP(now())-3600
            and succ_time is not null  and server_id={0} and vm_id={1}'''.format(s,i)
            res = dbutil.select_sql(sql)
            if not res or len(res)<=0:
                if not no_succ_dict.has_key(s):
                    no_succ_dict[s]=[i]
                else:
                    no_succ_dict[s].append(i)
    if no_succ_dict:
        s1 = json.dumps(no_succ_dict)
        s1_msg = "1小时内没有成功的代理vm:"+s1
        return s1_msg
    return None

def get_failed_vm():
    sql = '''select server_id,vm_id,count(*) num from vm_cur_task where UNIX_TIMESTAMP(update_time)>UNIX_TIMESTAMP(now())-600 
    and status not in(-1,1,2,4,11,12,13) group by server_id,vm_id having num>=5'''

    res = dbutil.select_sql(sql)
    timeout_array = []
    if not res or len(res)<=0:
        return None
    for r in res: 
        print r
        server_id = r[0]
        vm_id = r[1]
        timeout_array.append( (server_id, vm_id) )
    if not timeout_array:
        return None
    s1 = json.dumps(timeout_array)
    s1_msg = "10分钟连续失败的虚拟机:"+s1

    msg = s1_msg 
    print msg
    return msg

def vpn_dial_times_warning():
    sql = '''select area,count(*) from vpn_dial_log where
    UNIX_TIMESTAMP(change_1_time)>(UNIX_TIMESTAMP(now()) -3600)  group by area
    HAVING count(*)<2'''
    res = dbutil.select_sql(sql)
    warn_areas = []
    if not res or len(res)<=0:
        return None
    for r in  res:
        warn_areas.append(r[0])
    return set(warn_areas)
    # w_msg = "1小时拨号不足1次的区域:"+json.dumps(warn_areas)
    # return w_msg
    
def vpn_nodialtimes_warning():
    total_areas=range(31,101)
    sql = '''select area,count(*) from vpn_dial_log where
    UNIX_TIMESTAMP(change_1_time)>(UNIX_TIMESTAMP(now()) -3600)  group by
    area'''
    res = dbutil.select_sql(sql)
    areas = []
    if not res or len(res)<=0:
        return None
    for r in  res:
        areas.append(r[0])
    warn_areas = set(total_areas)-set(areas)
    return warn_areas
    # w_msg = "1小时拨号不足1次的区域:"+json.dumps(warn_areas)
    # return w_msg

def vpn_warning():
    w1 = vpn_dial_times_warning()
    w2 = vpn_nodialtimes_warning()
    w_areas =  w1|w2
    w_areas = list(w_areas)
    w_areas.sort()
    w_msg = "1小时拨号不足1次的区域:"+json.dumps(w_areas)
    return w_msg

def main():
    while True:
        try:
        #if True:
            msg = get_timeout_res(30)
            if msg:
                print "send msg:", msg
                send_mail('vpn超时报警','stskybin@vip.qq.com',msg, False)
            # msg = get_failed_vm()
            msg = vpn_warning()
            if msg:
                print "send msg:", msg
                send_mail('vpn拨号次数报警','stskybin@vip.qq.com',msg, False)
            msg = get_no_succ_vm()
            if msg:
                print "send msg:", msg
                send_mail('vm失败报警','stskybin@vip.qq.com',msg, False)
            # msg = get_no_succ_proxy_vm()
            # if msg:
                # print "send msg:", msg
                # send_mail('代理vm失败报警','stskybin@vip.qq.com',msg, False)
        except Exception,e:
            print "except",e
            time.sleep(100)
            continue
        time.sleep(1800)
        

if __name__ == '__main__':
    main()
