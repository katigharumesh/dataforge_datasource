import sys

import mysql.connector
import logging
from datetime import datetime, timezone
import time
import traceback
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from serviceconfigurations import *
from basicudfs import *
from suppressionRequestProcessor import *

logging.basicConfig(level=logging.INFO)
logger = create_logger("scheduler_log", SUPP_LOG_PATH)

class RequestPicker:
    def __init__(self):
        self.mysql_config = MYSQL_CONFIGS
        self.request_queue = Queue()

    def create_connection(self):
        retry_limit = 0
        while retry_limit <= 5:
            try:
                if retry_limit == 5:
                    logger.info("Unable to Get MySQL connection... after 5 retries..")
                    send_skype_alert("Unable to Get MySQL connection... after 5 retries..")
                    sys.exit(0)
                logger.info("Trying to get MySQL connection...")
                mysqlcon = mysql.connector.connect(**self.mysql_config)
                retry_limit += 1
                if mysqlcon.is_connected():
                    return mysqlcon
            except Exception as e:
                logger.info(f"Exception caught during acquiring MySQL connection.. Here it is...{str(e)}")
                logger.info("Unable to Get MySQL connection... Retrying again after 10 sec...")
                time.sleep(10)
                retry_limit += 1

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
        mysqlcon = self.create_connection()
        try:
            mysqlcur = mysqlcon.cursor()
            updatequery = f"UPDATE {SUPP_SCHEDULE_TABLE} SET status='I', runnumber=runnumber+1 WHERE ID = {id}"
            logger.info(f"[Thread-{thread_number}] Update query :: {updatequery}")
            mysqlcur.execute("SET time_zone = 'UTC';")
            mysqlcur.execute(updatequery)
            mysqlcon.commit()
            updateflag = True
            self.suppressionRequestSchedule(mysqlcon, updateflag, request)
            request_obj = Suppression_Request()
            request_obj.suppression_request_processor(request[1], request[2]+1, request[3], request[4], request[5])
            logger.info(f"[Thread-{thread_number}] Request Successfully sent to Processor.... ID :: {id}")
        except Exception as e:
            logger.error(f"[Thread-{thread_number}] Error in processrequests() :: {e}")
            logger.error(traceback.print_exc())
        finally:
            mysqlcon.close()
            self.request_queue.task_done()

    def getrequests(self):
        logger.info(f"Getrequests() :: {datetime.now(timezone.utc)}")
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
            logger.error(traceback.format_exc())

if __name__ == '__main__':
    obj = RequestPicker()
    obj.requestThreadProcess()
