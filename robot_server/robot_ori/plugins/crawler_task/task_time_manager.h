//  Copyright (c) 2015-2015 The kid Authors. All rights reserved.
//  Created on: 2015年10月12日 Author: kerry

#ifndef KID_CRAWLER_TASK_TIME_MANAGER_H_
#define KID_CRAWLER_TASK_TIME_MANAGER_H_

#include "crawler_task/task_schduler_engine.h"
#include "crawler_task/crawler_task_db.h"
#include "crawler_task/crawler_task_kafka.h"
#include "../../pub/share/manager_info.h"

#define TIME_DISTRIBUTION_TASK       10000
#define TIME_FECTCH_TASK             10001
#define TIME_CLEAN_NO_EFFECTIVE      10005
#define TIME_RECYCLINGTASK           10006
#define TIME_FETCH_TEMP_TASK         10007
#define TIME_DISTRBUTION_TEMP_TASK   10008
#define TIME_UPDATE_EXEC_TASKS		 10009

namespace crawler_task_logic {

class TaskTimeManager {
 public:
    explicit TaskTimeManager(crawler_task_logic::CrawlerTaskDB* task_db);
    virtual ~TaskTimeManager();
 public:
    void TaskTimeEvent(int opcode, int time);
 private:
    void TimeFetchTask();

    void TimeCheckTask();

    void TimeFechTempTask();

    void CleanNoEffectCrawler();

    void UpdateExecTasks();

 private:
    crawler_task_logic::TaskSchdulerManager*               schduler_mgr_;
    scoped_ptr<crawler_task_logic::CrawlerTaskDB>          task_db_;
    CrawlerTaskKafka									   task_kafka_;
    plugin_share::ManagerInfo 							   *manager_info_;
};

}  //  namespace crawler_task_logic



#endif /* TASK_TIME_MANAGER_H_ */
