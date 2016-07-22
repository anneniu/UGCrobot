//  Copyright (c) 2015-2018 The KID Authors. All rights reserved.
//  Created on: 2016.2.18 Author: yangge

#ifndef KID_CRAWLER_TASK_KAFKA_H_
#define KID_CRAWLER_TASK_KAFKA_H_

#include <string>
#include <list>
#include <map>

#include "logic/auto_crawler_infos.h"
#include "basic/basictypes.h"
#include "logic/base_values.h"
#include "basic/scoped_ptr.h"
#include "queue/kafka_consumer.h"

namespace robot_task_logic {

class CrawlerTaskKafka {
 public:
	CrawlerTaskKafka();
    virtual ~CrawlerTaskKafka();

 public:
    bool FectchTasks(std::list<base_logic::RobotTask *> *list);

 private:
    kafka_consumer kafka_consumer_;
};
}  // namespace crawler_task_logic


#endif /* TASK_DB_H_ */
