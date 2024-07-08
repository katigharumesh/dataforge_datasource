import mysql.connector
import logging
from datetime import datetime, timezone
import time
import traceback
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from serviceconfigurations import *
from basicudfs import *
from suppressionRequestProcessor import *

logging.basicConfig(level=logging.INFO)
logger = create_logger("scheduler_log", SUPP_LOG_PATH)
date = str(datetime.now(timezone.utc).date())

class RequestPicker:
    def __init__(self):
        self.mysql_config = MYSQL_CONFIGS
        self.request_queue = Queue()

    def create_connection(self):
        while True:
            mysqlcon = mysql.connector.connect(**self.mysql_config)
            if mysqlcon.is_connected():
                return mysqlcon
            logger.info("Trying to get MySQL connection...")

    def suppressionRequestSchedule(self, mysqlcon, updateflag, request):
        logger.info(f"Suppression request schedule process started ....")
        try:
            mysqlcur = mysqlcon.cursor()
            insertquery = f"INSERT INTO {SUPP_SCHEDULE_STATUS_TABLE}(requestID, requestScheduledId, runnumber, createdDate, createdTimestamp, updatedDate) VALUES " \
                          f"({request[1]}, {request[0]}, {request[2]}+1, DATE(NOW()), NOW(), NOW())"
            if updateflag:
                logger.info(f"Insert query :: {insertquery}")
                mysqlcur.execute("SET time_zone = 'UTC';")
                mysqlcur.execute(insertquery)
                mysqlcon.commit()
        except Exception as e:
            logger.error(f"Error in suppressionRequestSchedule() :: {e}")

    def processrequests(self):
        while True:
            request = self.request_queue.get()
            if request is None:
                break
            logger.info(f"Request Processing Started .. {datetime.now()}")
            updateflag = None
            id = request[0]
            startDate = str(request[7])
            mysqlcon = self.create_connection()
            try:
                if startDate <= date:
                    mysqlcur = mysqlcon.cursor()
                    updatequery = f"UPDATE {SUPP_SCHEDULE_TABLE} SET status='I', runnumber=runnumber+1 WHERE ID = {id}"
                    logger.info(f"Update query :: {updatequery}")
                    mysqlcur.execute("SET time_zone = 'UTC';")
                    mysqlcur.execute(updatequery)
                    mysqlcon.commit()
                    updateflag = True
                    self.suppressionRequestSchedule(mysqlcon, updateflag, request)
                    request_obj = Suppression_Request()
                    request_obj.suppression_request_processor(request[1], request[2]+1, request[3], request[4], request[5])
                else:
                    logger.info(f"Request Scheduled in Future. It will initiate from {startDate} .....")
            except Exception as e:
                logger.error(f"Error in processrequests() :: {e}")
                logger.error(traceback.print_exc())
            finally:
                mysqlcon.close()
                self.request_queue.task_done()

    def getrequests(self):
        logger.info(f"Getrequests() :: {datetime.now()}")
        requestList = []
        mysqlcon = self.create_connection()
        try:
            mysqlcur = mysqlcon.cursor()
            requestquery = f"SELECT a.id, a.requestId, a.runnumber, IF(a.nextScheduleDue IS NULL, NOW(), a.nextscheduleDue) AS nextscheduleDue, a.notificationMails, a.sendNotificationsFor, a.type, IF(a.startDate IS NULL, DATE(NOW()), a.startDate) AS startDate, IF(a.endDate IS NULL, DATE(NOW()), a.endDate) AS " \
                           f"endDate FROM {SUPP_SCHEDULE_TABLE} a JOIN {SUPP_REQUEST_TABLE} b ON a.requestId = b.id WHERE a.status = 'W' AND b.isActive = 1 AND IF(a.nextScheduleDue IS NULL, NOW(), a.nextscheduleDue) <= NOW() LIMIT {THREAD_COUNT}"
            logger.info(f"Request query ::{requestquery}")
            mysqlcur.execute("SET time_zone = 'UTC'")
            mysqlcur.execute(requestquery)
            requestList = mysqlcur.fetchall()
        except Exception as e:
            logger.error(f"Error in Getrequests() :: {e}")
            logger.error(traceback.print_exc())
        finally:
            mysqlcon.close()
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
            logger.error(traceback.format_exc())

if __name__ == '__main__':
    obj = RequestPicker()
    obj.requestThreadProcess()

