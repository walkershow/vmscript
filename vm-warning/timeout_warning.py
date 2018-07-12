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
dbutil.db_name = "vm"
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
 
        server = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465) 
        #server.starttls() 
        server.set_debuglevel(1) 
        server.login("rs1@990g.com", "QiPPwTrTaVYDm7Q2") 
        server.sendmail("rs1@990g.com", [to], msg.as_string()) 
        server.quit() 
        return True 
    except Exception, e: 
        print e
        # logger.error("===============send_mail failed:%s", e) 
        return False 


# sql = "insert into company_ip(id,ip,update_time) values(1,'%s',CURRENT_TIMESTAMP) on duplicate key update ip='%s',\
#     update_time=CURRENT_TIMESTAMP "
# sql = "SELECT a.server_id,TIMESTAMPDIFF(minute,  max(b.c_time), CURRENT_TIMESTAMP()) AS mins from  vm_oprcode a,\
# vm_task_log b where a.create_time>CURRENT_DATE and a.oprcode=b.oprcode  group by a.server_id"
# while True:
#     ip_port = ('192.168.1.230',8009)
#     sk = socket.socket()
#     sk.connect(ip_port)
#     data = sk.recv(1024)
#     print 'receive:',data
#     sql_tmp = sql %(data,data)
#     print sql_tmp
#     ret = dbutil.execute_sql(sql_tmp)
#     if ret<0:
#         print "sql",sql,"execute error"
#     # inp = input('please input:')
#     # sk.sendall('hello')
#     # if inp == 'exit':
#         # break
#     sk.close()
#     time.sleep(5)
def send_msg(msg):
    ip_port = ('192.168.1.230',8009)
    sk = socket.socket()
    sk.connect(ip_port)
    ret = sk.sendall(msg)
    print ret
    # data = sk.recv(1024)
    sk.close()
def get_timeout_res(timeout):
    sql = "SELECT a.server_id,TIMESTAMPDIFF(minute,  max(b.c_time), CURRENT_TIMESTAMP()) AS mins from  vm_oprcode a,\
    vm_task_log b where a.create_time>CURRENT_DATE and a.oprcode=b.oprcode  group by a.server_id"
    res = dbutil.select_sql(sql)
    timeout_array = []
    timeout_none = []
    for r in res:
        print r
        server_id = r[0]
        mins = r[1]
        if mins>=timeout:
            timeout_array.append(server_id)
        elif mins is None:
            timeout_none.append(server_id)
    if not timeout_array and not timeout_none:
        return None
            
    s1 = json.dumps(timeout_array)
    s2 = json.dumps(timeout_none)
    print s1, s2
    s1_msg = "超时:"+s1
    s2_msg = "没有c_time:" + s2

    msg = s1_msg + "\r\n"+s2_msg
    print msg
    return msg

def main():
    while True:
        msg = get_timeout_res(30)
        if msg:
            print "send msg:", msg
            send_mail('超时报警','stskybin@vip.qq.com',msg, False)
        time.sleep(300)
        

if __name__ == '__main__':
    main()