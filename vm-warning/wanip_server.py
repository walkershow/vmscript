# -*- coding: utf-8 -*-

import SocketServer
import smtplib  
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
from email.mime.text import MIMEText  
  
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
        server.login("rs1@990g.com", "va9nKF6v9MpuCHK7") 
        server.sendmail("rs1@990g.com", [to], msg.as_string()) 
        server.quit() 
        return True 
    except Exception, e: 
        print e
        # logger.error("===============send_mail failed:%s", e) 
        return False 


class WanIPServer(SocketServer.BaseRequestHandler):
    def handle(self):
        conn = self.request
        Flag = True
        data = conn.recv(1024)
        print data
        send_mail('good','walkershow2@163.com',data, False)

if __name__ == '__main__':
    server = SocketServer.ThreadingTCPServer(('0.0.0.0',8009),WanIPServer)
    server.serve_forever()
