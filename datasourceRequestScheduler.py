from config import config
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
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
                    mysqlcur.execute("SET time_zone = '+00:00';")
                    mysqlcur.execute(insertquery)
                    mysqlcon.commit()
        except Exception as e:
            logger.error(f"Error in datasourceschedule() :: {e}")


    def processrequests(self, thread_number):
        request = self.request_queue.get()
        logger.info(f"[Thread-{thread_number}] Request fetched from Queue ::: {request}")
        if request is None:
            logger.info(f"[Thread-{thread_number}] No request found... Closing this Thread...")
            self.request_queue.task_done()
            return
        logger.info(f"[Thread-{thread_number}] Request Processing Started .. {datetime.now(timezone.utc)}")
        updateflag = None
        id = request[0]
        try:
            with mysqlconnection() as mysqlcon:
                mysqlcur = mysqlcon.cursor()
                updatequery = f"update {self.cpProp.datasourceTable} set status='I',runnumber=runnumber+1 where ID = {id}"
                logger.info(f"[Thread-{thread_number}] Update query :: {updatequery}")
                mysqlcur.execute("SET time_zone = '+00:00';")
                mysqlcur.execute(updatequery)
                mysqlcon.commit()
                updateflag=True
                self.datasourceschedule(updateflag,request)
                dataset_obj = Dataset()
                dataset_obj.dataset_processor(request[1],request[2]+1,request[3],request[4], request[5])
                logger.info(f"[Thread-{thread_number}] Request Successfully sent to Processor.... ID :: {id}")
        except Exception as e:
            logger.error(f"Error in processrequests() :: {e}")
            logger.error(traceback.print_exc())
        finally:
            mysqlcon.close()
            self.request_queue.task_done()


    def getrequests(self):
        logger.info(f"Getrequests() :: {datetime.now(timezone.utc)}")
        requestList=[]
        try:
            with mysqlconnection() as mysqlcon:
                mysqlcur = mysqlcon.cursor()
                requestquery = f"select a.id,a.datasourceId,a.runnumber, if(a.nextScheduleDue is NULL, now(), a.nextscheduleDue) as nextscheduleDue, a.notificationMails, a.sendNotificationsFor,a.type,if(a.startDate is NULL,date(now()),a.startDate) as startDate,if(a.endDate is NULL,date(now()),a.endDate) as endDate from {SCHEDULE_TABLE} a join  {DATASET_TABLE} b on a.datasourceId = b.id where a.status='W' and b.isActive = 1  and if(nextScheduleDue is NULL,now(),nextscheduleDue)<=now() limit {THREAD_COUNT}"
                logger.info(f"requestquery ::{requestquery}")
                mysqlcur.execute("SET time_zone = '+00:00';")
                mysqlcur.execute(requestquery)
                requestList = mysqlcur.fetchall()
        except Exception as e:
            logging.error(f"Error in Getrequests() :: {e}")
            logger.error(traceback.print_exc())
        return requestList

    def requestThreadProcess(self):
        logger.info(f"Thread Process Started :: {datetime.now(timezone.utc)}")
        try:
             with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
                futures = {}
                while True:
                    try:
                        waitingRequests = self.getrequests()
                        if waitingRequests:
                            logger.info(f"Waiting Requests are :: {waitingRequests}")
                            for request in waitingRequests:
                                self.request_queue.put(request)
                                logger.info(f" Request Sent to Queue :: request details {request}")
                            while not self.request_queue.empty():
                                if len(futures) < THREAD_COUNT:
                                    thread_number = len(futures) + 1
                                    future = executor.submit(self.processrequests, thread_number)
                                    futures[future] = thread_number
                                    logger.info(f"Request submitted to Thread-{thread_number} from Executor....")
                                else:
                                    logger.info("No thread is free, waiting for a thread to complete...")
                                    completed_futures, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
                                    for completed_future in completed_futures:
                                        thread_number = futures.pop(completed_future)
                                        logger.info(f"Thread-{thread_number} has completed.")
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

