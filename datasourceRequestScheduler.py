from config import config
from concurrent.futures import ThreadPoolExecutor
from sqlconfiguration import mysqlconnection
from serviceconfigurations import *
import time
from datetime import datetime,timezone
import traceback
import logging
from datasourceRequestProcessor import *
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
date= str(datetime.now(timezone.utc).date())

class RequestPicker:
    def __init__(self):
        self.obj = config()
        self.cpProp = self.obj.config
        self.request_queue = Queue()

    def datasourceschedule(self, updateflag, request):
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
                    mysqlcon.commit()
        except Exception as e:
            logger.error(f"Error in datasourceschedule() :: {e}")


    def processrequests(self):
        while True:
            request = self.request_queue.get()
            if request is None:
                break
            logger.info(f"Request Processing Started .. {datetime.now()}")
            updateflag = None
            id = request[0]
            startDate=str(request[7])
            try:
                with mysqlconnection() as mysqlcon:
                    if startDate<=date:
                        mysqlcur = mysqlcon.cursor()
                        updatequery = f"update {self.cpProp.datasourceTable} set status='I',runnumber=runnumber+1 where ID = {id}"
                        logger.info(f"updatequery :: {updatequery}")
                        mysqlcur.execute("SET time_zone = 'UTC';")
                        mysqlcur.execute(updatequery)
                        updateflag=True
                        self.datasourceschedule(updateflag,request)
                        dataset_obj = Dataset()
                        dataset_obj.dataset_processor(request[1],request[2]+1,request[3],request[4], request[5])
                    else:
                        logger.info(f"Request Scheduled in Future.It will initiate from {startDate} .....")
                    #mysqlcur.commit()
            except Exception as e:
                logger.error(f"Error in processrequests() :: {e}")
                logger.error(traceback.print_exc())
            finally:
                mysqlcon.close()
                self.request_queue.task_done()


    def getrequests(self):
        logger.info(f"Getrequests() :: {datetime.now()}")
        requestList=[]
        try:
            with mysqlconnection() as mysqlcon:
                mysqlcur = mysqlcon.cursor()
                requestquery = f"select a.id,a.datasourceId,a.runnumber, if(a.nextScheduleDue is NULL, now(), a.nextscheduleDue) as nextscheduleDue, a.notificationMails, a.sendNotificationsFor,a.type,if(a.startDate is NULL,date(now()),a.startDate) as startDate,if(a.endDate is NULL,date(now()),a.endDate) as " \
                               f"endDate from {SCHEDULE_TABLE} a join  {DATASET_TABLE} b on a.datasourceId = b.id where a.status='W' and b.isActive = 1  and if(nextScheduleDue is NULL,now(),nextscheduleDue)<=now() limit {THREAD_COUNT}"
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
                futures = []
                while True:
                    try:
                        waitingRequests = self.getrequests()
                        if waitingRequests:
                            logger.info(f"Waiting Requests are :: {waitingRequests}")
                            for request in waitingRequests:
                                self.request_queue.put(request)
                            while not self.request_queue.empty():
                                executor.submit(self.processrequests)
                        else:
                            logger.info(f"No waiting requests found ....")
                        time.sleep(5)
                    except Exception as e:
                        logger.error(f"Error in ThreadPoolExecutor loop() :: {e}")
                        logger.error(traceback.format_exc())
                        time.sleep(5)
        except Exception as e:
            logger.error(f"Error in requestThreadProcess() :: {e}")
            logger.error(traceback.print_exc())



if __name__=='__main__':
    obj=RequestPicker()
    obj.requestThreadProcess()
