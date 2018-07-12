#!/bin/sh
#/usr/local/mysql/bin/mysql -uroot -pchinaU#2720 -e "use vm3;delete from vm_users where status=0;"
/usr/local/mysql/bin/mysql -uroot -pchinaU#2720 -e "use vm3;delete from vm_task_log where log_time<date_sub(CURRENT_DATE,interval 2 day);delete from vm_oprcode where update_time<date_sub(CURRENT_DATE,interval 2 day);delete from vm_cur_task where start_time<date_sub(CURRENT_DATE,interval 2 day);delete from vm_task_profile_latest where status in(-2,-1,1,2,3,5,8,9) and start_time<date_sub(CURRENT_DATE,interval 2 day);"
