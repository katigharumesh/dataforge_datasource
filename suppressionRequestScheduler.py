from serviceconfigurations import *
from basicudfs import *
from concurrent.futures import ThreadPoolExecutor
#from sqlconfiguration import mysqlconnection
import time
from datetime import datetime
import traceback
import logging
from suppressionRequestProcessor import *
logging.basicConfig(level=logging.INFO)
logger = create_logger("scheduler_log", SUPP_LOG_PATH)
date= str(datetime.now().date())

class RequestPicker:
    def __init__(self):
        self.mysqlcon = mysql.connector.connect(**MYSQL_CONFIGS)

    def suppressionRequestSchedule(self, updateflag, request,requestList):
        logger.info(f"Suppression request schedule process started ....")
        try:
            mysqlcur = self.mysqlcon.cursor()
            insertquery = f"Insert into {SUPP_SCHEDULE_STATUS_TABLE}(requestID,requestScheduledId,runnumber,createdDate,createdTimestamp,updatedDate) values " \
                          f"({request[1]},{request[0]},{request[2]}+1,date(now()),now(),now())"
            if updateflag:
                logger.info(f"insertquery :: {insertquery}")
                mysqlcur.execute("SET time_zone = 'UTC';")
                mysqlcur.execute(insertquery)
                #mysqlcur.commit()
        except Exception as e:
            logger.error(f"Error in suppressionRequestSchedule() :: {e}")


    def processrequests(self,request,requestList):
        logger.info(f"Request Processing Started .. {datetime.now()}")
        updateflag=None
        id = request[0]
        startDate=str(request[8])
        try:
            if startDate<=date:
                mysqlcur = self.mysqlcon.cursor()
                updatequery = f"update {SUPP_SCHEDULE_TABLE} set status='I',runnumber=runnumber+1 where ID = {id}"
                logger.info(f"updatequery :: {updatequery}")
                mysqlcur.execute("SET time_zone = 'UTC';")
                mysqlcur.execute(updatequery)
                updateflag=True
                self.suppressionRequestSchedule(updateflag,request,requestList)
                request_obj = Suppression_Request()
                request_obj.suppression_request_processor(request[1],request[2]+1,request[3],request[4],request[5],request[6])
            else:
                logger.info(f"Request Scheduled in Future.It will initiate from {startDate} .....")
            #mysqlcur.commit()
        except Exception as e:
            logger.error(f"Error in processrequests() :: {e}")
            logger.error(traceback.print_exc())


    def getrequests(self):
        logger.info(f"Getrequests() :: {datetime.now()}")
        requestList=[]
        try:
            mysqlcur = self.mysqlcon.cursor()
            requestquery = f"select id,requestId,runnumber , if(nextScheduleDue is NULL,now(),nextscheduleDue) as  nextscheduleDue, notificationMails,sendNotificationsFor,wasInActive,type,if(startDate is NULL,date(now()),startDate) as startDate,if(endDate is NULL,date(now()),endDate) as " \
                           f"endDate from {SUPP_SCHEDULE_TABLE} where status='W'  and if(nextScheduleDue is NULL,now(),nextscheduleDue)<=now() limit 5"
            logger.info(f"requestquery ::{requestquery}")
            mysqlcur.execute("SET time_zone = 'UTC';")
            mysqlcur.execute(requestquery)
            requestList = mysqlcur.fetchall()
        except Exception as e:
            logging.error(f"Error in Getrequests() :: {e}")
            logger.error(traceback.print_exc())
        return requestList

    def requestThreadProcess(self):
        logger.info(f"Thread Process Started :: {datetime.now()}")
        try:
            with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
                try:
                    waitingRequests = self.getrequests()
                    if len(waitingRequests)>0:
                        print(f"Waiting Requests are :: {waitingRequests}")
                        logger.info(f"Waiting Requests are :: {waitingRequests}")
                        for request in waitingRequests:
                            resultList = [False]
                            executor.submit(self.processrequests,request,resultList)
                            #result = futures.result()
                            #print(result)
                    else:
                        logging.info(f"No waiting requests found ....")
                        time.sleep(5)
                    time.sleep(5)
                except Exception as e:
                    logger.error(f"Error in ThreadPoolExecutor() :: {e}")
                    logger.error(traceback.print_exc())
        except Exception as e:
            logger.error(f"Error in requestThreadProcess() :: {e}")
            logger.error(traceback.print_exc())


if __name__ == '__main__':
    obj = RequestPicker()
    obj.requestThreadProcess()
