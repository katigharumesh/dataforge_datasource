from config import config
from concurrent.futures import ThreadPoolExecutor
from sqlconfiguration import mysqlconnection
from serviceconfigurations import *
import time
from datetime import datetime
import traceback
import logging
from datasourceRequestProcessor import *
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
date= str(datetime.now().date())

class RequestPicker:
    def __init__(self):
        self.obj = config()
        self.cpProp = self.obj.config

    def datasourceschedule(self, updateflag, request,requestList):
        logger.info(f"data source schedule process started ....")
        try:
            with mysqlconnection() as mysqlcon:
                mysqlcur = mysqlcon.cursor()
                insertquery = f"Insert into {self.cpProp.schedulestatusTable}(datasourceId,datasourceScheduleId,runnumber,createdDate,createdTimestamp,updatedDate) values " \
                              f"({request[1]},{request[0]},{request[2]}+1,date(now()),now(),now())"
                if updateflag:
                    logger.info(f"insertquery :: {insertquery}")
                    mysqlcur.execute("SET time_zone = 'UTC';")
                    mysqlcur.execute(insertquery)
                    #mysqlcur.commit()
        except Exception as e:
            logger.error(f"Error in datasourceschedule() :: {e}")


    def processrequests(self,request,requestList):
        logger.info(f"Request Process Started .. {datetime.now()}")
        updateflag=None
        id = request[0]
        startDate=str(request[4])
        try:
            with mysqlconnection() as mysqlcon:
                if startDate<=date:
                    mysqlcur = mysqlcon.cursor()
                    updatequery = f"update {self.cpProp.datasourceTable} set status='I',runnumber=runnumber+1 where ID = {id}"
                    logger.info(f"updatequery :: {updatequery}")
                    mysqlcur.execute("SET time_zone = 'UTC';")
                    mysqlcur.execute(updatequery)
                    updateflag=True
                    self.datasourceschedule(updateflag,request,requestList)
                    dataset_obj = Dataset()
                    dataset_obj.dataset_processor(request[1],request[2]+1,request[3],request[4], request[5])
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
            with mysqlconnection() as mysqlcon:
                mysqlcur = mysqlcon.cursor()
                requestquery = f"select a.id,a.datasourceId,a.runnumber, if(a.nextScheduleDue is NULL, now(), a.nextscheduleDue) as nextscheduleDue, a.notificationMails, a.sendNotificationsFor,a.type,if(a.startDate is NULL,date(now()),a.startDate) as startDate,if(a.endDate is NULL,date(now()),a.endDate) as " \
                               f"endDate from {SCHEDULE_TABLE} a join  {DATASET_TABLE} b on a.datasourceId = b.id where a.status='W' and b.isActive = 1  and if(nextScheduleDue is NULL,now(),nextscheduleDue)<=now() limit 5"
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
            with ThreadPoolExecutor(max_workers=self.cpProp.maxThreads) as executor:
                try:
                    waitingRequests = self.getrequests()
                    if len(waitingRequests)>0:
                        print(waitingRequests)
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



if __name__=='__main__':
    obj=RequestPicker()
    obj.requestThreadProcess()
