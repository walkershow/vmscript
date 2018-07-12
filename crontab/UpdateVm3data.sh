#!/bin/sh
/usr/local/mysql/bin/mysql -uroot -pchinaU#2720 -e "use vm3;update zero_schedule_list set ran_times=0,succ_times=0;update vm_task_runtimes_config set users_used_amount=0,allot_succ_times=0;update baiduSearch set had_search_times=0; "
