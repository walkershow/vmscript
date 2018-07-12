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
dbutil.db_name = "vm3"
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
        server.login("3442819978@qq.com", "fwbhileadolsdahg") 
        server.sendmail("3442819978@qq.com", [to], msg.as_string()) 
        server.quit() 
        return True 
    except Exception, e: 
        print e
        # logger.error("===============send_mail failed:%s", e) 
        return False 


def get_timeout_res(timeout):
    sql = '''SELECT serverid from vpn_status where update_time>current_date and TIMESTAMPDIFF(minute,  update_time, CURRENT_TIMESTAMP())>5'''
    res = dbutil.select_sql(sql)
    timeout_array = []
    timeout_none = []
    for r in res:
        print r
        server_id = r[0]
        timeout_array.append(server_id)
            
    s1 = json.dumps(timeout_array)
    s1_msg = "超时:"+s1

    msg = s1_msg 
    print msg
    return msg

def main():
    while True:
        msg = get_timeout_res(30)
        if msg:
            print "send msg:", msg
            # send_mail('超时报警','stskybin@vip.qq.com',msg, False)
            send_mail('vpn超时报警','stskybin@vip.qq.com',msg, False)
        time.sleep(300)
        

if __name__ == '__main__':
    main()