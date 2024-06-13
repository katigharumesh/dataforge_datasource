from serviceconfigurations import *
from basicudfs import *
from concurrent.futures import ThreadPoolExecutor
from suppressionRequestProcessor import *
import mysql.connector
import logging
from datetime import datetime
import time
import traceback

logging.basicConfig(level=logging.INFO)
logger = create_logger("scheduler_log", SUPP_LOG_PATH)
date = str(datetime.now().date())

class RequestPicker:
    def __init__(self):
        self.mysql_config = MYSQL_CONFIGS

    def create_connection(self):
        while True:
            mysqlcon = mysql.connector.connect(**self.mysql_config)
            if mysqlcon.is_connected():
                return mysqlcon
            logger.info("Trying to Get mysql connection...")

    def suppressionRequestSchedule(self, mysqlcon, updateflag, request, requestList):
        logger.info(f"Suppression request schedule process started ....")
        try:
            mysqlcur = mysqlcon.cursor()
            insertquery = f"Insert into {SUPP_SCHEDULE_STATUS_TABLE}(requestID, requestScheduledId, runnumber, createdDate, createdTimestamp, updatedDate) values " \
                          f"({request[1]}, {request[0]}, {request[2]}+1, date(now()), now(), now())"
            if updateflag:
                logger.info(f"insertquery :: {insertquery}")
                mysqlcur.execute("SET time_zone = 'UTC';")
                mysqlcur.execute(insertquery)
                mysqlcon.commit()
        except Exception as e:
            logger.error(f"Error in suppressionRequestSchedule() :: {e}")

    def processrequests(self, request, requestList):
        logger.info(f"Request Processing Started .. {datetime.now()}")
        updateflag = None
        id = request[0]
        startDate = str(request[8])
        mysqlcon = self.create_connection()
        try:
            if startDate <= date:
                mysqlcur = mysqlcon.cursor()
                updatequery = f"update {SUPP_SCHEDULE_TABLE} set status='I', runnumber=runnumber+1 where ID = {id}"
                logger.info(f"updatequery :: {updatequery}")
                mysqlcur.execute("SET time_zone = 'UTC';")
                mysqlcur.execute(updatequery)
                mysqlcon.commit()
                updateflag = True
                self.suppressionRequestSchedule(mysqlcon, updateflag, request, requestList)
                request_obj = Suppression_Request()
                request_obj.suppression_request_processor(request[1], request[2]+1, request[3], request[4], request[5], request[6])
            else:
                logger.info(f"Request Scheduled in Future. It will initiate from {startDate} .....")
        except Exception as e:
            logger.error(f"Error in processrequests() :: {e}")
            logger.error(traceback.print_exc())
        finally:
            mysqlcon.close()

    def getrequests(self):
        logger.info(f"Getrequests() :: {datetime.now()}")
        requestList = []
        mysqlcon = self.create_connection()
        try:
            mysqlcur = mysqlcon.cursor()
            requestquery = f"select a.id, a.requestId, a.runnumber, if(a.nextScheduleDue is NULL, now(), a.nextscheduleDue) as nextscheduleDue, a.notificationMails, a.sendNotificationsFor, a.wasInActive, a.type, if(a.startDate is NULL, date(now()), a.startDate) as startDate, if(a.endDate is NULL, date(now()), a.endDate) as " \
                           f"endDate from {SUPP_SCHEDULE_TABLE} a join {SUPP_REQUEST_TABLE} b on a.requestId = b.id where a.status = 'W' and b.isActive = 1 and if(a.nextScheduleDue is NULL, now(), a.nextscheduleDue) <= now() limit 5"
            logger.info(f"requestquery ::{requestquery}")
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
                waitingRequests = self.getrequests()
                if len(waitingRequests) > 0:
                    logger.info(f"Waiting Requests are :: {waitingRequests}")
                    for request in waitingRequests:
                        resultList = [False]
                        executor.submit(self.processrequests, request, resultList)
                else:
                    logger.info(f"No waiting requests found ....")
                    time.sleep(5)
        except Exception as e:
            logger.error(f"Error in requestThreadProcess() :: {e}")
            logger.error(traceback.print_exc())

if __name__ == '__main__':
    obj = RequestPicker()
    obj.requestThreadProcess()