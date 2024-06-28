#import statements

import glob
import json
import ntpath
import os
import string
import random
import sys
import time
import urllib
from datetime import datetime
from pathlib import Path
from threading import Thread
import threading
import mysql.connector
import snowflake.connector
from snowflake.connector import DictCursor
import concurrent.futures
import pytz
import requests
import urllib
import queue
from queue import Queue
import logging
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import subprocess
import paramiko
import ftplib
import traceback
import fileinput
import gzip
import boto3


''' DATA SOURCE CONFIGS '''

# SCRIPT_PATH = r"D:\tmp\data_forge"
# LOG_PATH = r"D:\tmp\data_forge\app_logs"
SCRIPT_PATH = r"/home/zxdev/zxcustom/DATAOPS/DATASET/"

LOG_PATH = SCRIPT_PATH + "app_logs"
FILE_PATH = "/zds-stg-cas/zxcustom/DATAOPS/DATASET/r_logs/"   # local file path - mount to download the temp files
PID_FILE = SCRIPT_PATH + "app_REQUEST_ID.pid"
LOG_FILES_REMOVE_LIMIT = 30

MAIL_HTML_FILE = FILE_PATH + "mail_{}_{}.ftl"

THREAD_COUNT = 4  # thread count

skype_configurations = {
    'url': 'http://zds-prod-ext-greenapp1-vip.bo3.e-dialog.com/sendSkypeAlerts/index.php?key=',
    'file_path': SCRIPT_PATH,
    'default_channel': '19:69777cb1e0d94ef9ba894c5d4d7eb3b6@thread.skype',
    'script_path': SCRIPT_PATH,
    'script_name': 'main_app.py',
    'server': 'zds-prod-job-02.bo3.e-dialog.com',
    'log_path': LOG_PATH
}

# GLOBAL_VARIABLES
'''
MYSQL_CONFIGS = {
    "host": "10.218.18.157",
    "user": "smartuser",
    "password": "smart12#$",
    "database": "CAMPAIGN_TOOL_GMAIL",
    "autocommit": True,
    "allow_local_infile": True
}
'''
MYSQL_CONFIGS = {
    'user': 'pmtauser',
    'password': 'pmta12#$',
    'host': '10.100.6.181',
    'database': 'CAMPAIGN_TOOL_QA',
    'autocommit': True,
    'allow_local_infile': True
}



SCHEDULE_TABLE = 'SUPPRESSION_DATASOURCE_SCHEDULE'
SCHEDULE_STATUS_TABLE = 'SUPPRESSION_DATASOURCE_SCHEDULE_STATUS'
DATASET_TABLE = 'SUPPRESSION_DATASOURCE'
DATASET_MAPPING_TABLE = 'SUPPRESSION_DATASOURCE_MAPPING'
SOURCE_TYPES_TABLE = 'SUPPRESSION_SOURCE_TYPES'
FILE_DETAILS_TABLE = 'SUPPRESSION_DATASOURCE_FILE_DETAILS'

FETCH_MAIN_DATASET_DETAILS = f'select a.id,a.name,a.channelName,a.userGroupId,a.feedType,a.dataProcessingType,' \
                             f'a.FilterMatchFields,a.isps,b.dataSourceScheduleId as ScheduleId,b.runNumber from {DATASET_TABLE} a ' \
                             f'join {SCHEDULE_STATUS_TABLE} b on a.id=b.dataSourceId where a.id=%s and b.runNumber=%s'

FETCH_SOURCE_DETAILS = f'select a.id,a.dataSourceId,a.sourceId,a.inputData,b.name,b.hostname,b.port,b.username,b.password,' \
                       'b.sfAccount,b.sfDatabase,' \
                       'b.sfSchema,b.sfTable,b.sfQuery,b.sourceType,b.sourceSubType from ' \
                       f'{DATASET_MAPPING_TABLE} a join ' \
                       f'{SOURCE_TYPES_TABLE} b on a.sourceId=b.id where a.dataSourceId=%s and a.isDeleted=0'

INSERT_FILE_DETAILS = f'insert into {FILE_DETAILS_TABLE}(dataSourceScheduleId,runNumber,dataSourceMappingId,count,fileName,createdBy,updatedBy,size,last_modified_time,file_status,error_desc)' \
                      f' values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

DELETE_FILE_DETAILS = f'delete from {FILE_DETAILS_TABLE} where dataSourceScheduleId=%s and ' \
                      f'runNumber=%s '

INSERT_SCHEDULE_STATUS = f'insert into {SCHEDULE_STATUS_TABLE}(dataSourceId,dataSourceScheduleId,runNumber,status,createdDate) values (%s,%s,%s,%s,%s)'

UPDATE_SCHEDULE_STATUS = f"update {SCHEDULE_STATUS_TABLE} set status=%s, recordCount=%s, errorReason=%s where dataSourceId=%s and runNumber=%s "

MAKE_SCHEDULE_IN_PROGRESS = f"update {SCHEDULE_TABLE} set status = 'I' where dataSourceId=%s and runNumber=%s "

SNOWFLAKE_CONFIGS = {
    "account": 'zetaglobal.us-east-1',
    "user": "green_lp_service",
    "password": "Jsw44QTLRYYGLGBgfhXQR7webwaxArWx",
    "database": "GREEN",
    "warehouse": "GREEN_ADHOC",
    "schema": 'INFS_LPT_QA'
}

STAGE_TABLE_PREFIX = 'STAGE_DO_DATASET_MAPPING_'
SOURCE_TABLE_PREFIX = 'DO_DATASET_MAPPING_'
MAIN_DATASET_TABLE_PREFIX = 'DO_DATASET_'

# OPERATOR_MAPPING = {'on': '=', 'after': '>', 'before': '<', 'between': 'between', 'greater than': '>', 'less than': '<'
# , 'equals': '=', 'not equals': '!=', 'like': 'like', 'not like': 'not like', 'exists in': 'in',
#                    'not exists in': 'not in', 'predefined daterange': '>='}

FROM_EMAIL = "noreply-notifications@zetaglobal.com"
#RECEPIENT_EMAILS = ["glenka@zetaglobal.com", "ukatighar@zetaglobal.com", "nuggina@zetaglobal.com"]

SF_DELETE_OLD_DETAILS_QUERY = "delete from %s where DO_INPUTSOURCE in (%s)"
FETCH_LAST_ITERATION_FILE_DETAILS_QUERY = f"select filename,last_modified_time,size,count,file_status as status,error_desc as error_msg from {FILE_DETAILS_TABLE} where dataSourceMappingId=%s and runNumber=%s and file_status='C' "  # get last iteration files data
LAST_SUCCESSFUL_RUN_NUMBER_QUERY = f"select max(runNumber) as runNumber from {SCHEDULE_STATUS_TABLE} where dataSourceId=%s and status in ('C','P')"

'''' SUPPRESSION REQUEST CONFIGS '''

SUPP_SCRIPT_PATH = r"/home/zxdev/zxcustom/DATAOPS/SUPPRESSION_REQUEST/"
#SUPP_SCRIPT_PATH = r"D:\tmp\data_forge"
SUPP_LOG_PATH = SUPP_SCRIPT_PATH + "supp_logs"
SUPP_FILE_PATH = "/zds-stg-cas/zxcustom/DATAOPS/SUPPRESSION_REQUEST/r_logs"  # local file path - mount to download the temp files
SUPP_PID_FILE = SUPP_SCRIPT_PATH + "supp_REQUEST_ID.pid"

SUPP_SOURCE_TABLE_PREFIX = "DO_SUPPRESSION_REQUEST_MAPPING_"
MAIN_INPUT_SOURCE_TABLE_PREFIX = "DO_SUPPRESSION_REQUEST_"

SUPP_REQUEST_TABLE = "SUPPRESSION_REQUEST"
SUPP_SCHEDULE_TABLE = "SUPPRESSION_REQUEST_SCHEDULE"
SUPP_MAPPING_TABLE = "SUPPRESSION_REQUEST_MAPPING"
SUPP_SCHEDULE_STATUS_TABLE = "SUPPRESSION_REQUEST_SCHEDULE_STATUS"
SUPPRESSION_MATCH_DETAILED_STATS_TABLE = "SUPPRESSION_MATCH_DETAILED_STATS"
SUPPRESSION_REQUEST_FILTERS_TABLE = "SUPPRESSION_REQUEST_FILTERS"
SUPPRESSION_PRESET_FILTERS_TABLE = "SUPPRESSION_PRESET_FILTERS"
SUPPRESSION_REQUEST_OFFERS_TABLE = "SUPPRESSION_REQUEST_OFFERS"
SUPPRESSION_PROFILE_TABLES_LOOKUP_TABLE = "SUPPRESSION_PROFILE_TABLES_LOOKUP"
SUPPRESSION_REQUEST_INPUT_SOURCES_TABLE = "SUPPRESSION_REQUEST_INPUT_SOURCES"
SUPPRESSION_REQUEST_FILES_INPUT_TABLE = "SUPPRESSION_REQUEST_FILES_INPUT"

SUPP_FILE_DETAILS_TABLE = 'SUPPRESSION_REQUEST_FILE_DETAILS'

SUPP_LAST_SUCCESSFUL_RUN_NUMBER_QUERY = f"select max(runNumber) as runNumber from {SUPP_SCHEDULE_STATUS_TABLE} where requestId=%s and status in ('C','P')"

SUPP_FETCH_LAST_ITERATION_FILE_DETAILS_QUERY = f"select filename,last_modified_time,size,count,file_status as status,error_desc as error_msg from {SUPP_FILE_DETAILS_TABLE} where suppressionRequestMappingId=%s and runNumber=%s and file_status='C' "

SUPP_INSERT_FILE_DETAILS = f'insert into {SUPP_FILE_DETAILS_TABLE}(suppressionRequestScheduleId,runNumber,suppressionRequestMappingId,count,fileName,createdBy,updatedBy,size,last_modified_time,file_status,error_desc) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

SUPP_DELETE_FILE_DETAILS = f'delete from {SUPP_FILE_DETAILS_TABLE} where suppressionRequestScheduleId=%s and runNumber=%s '

FP_LISTIDS_SF_TABLE = "GREEN_LPT.PFM_FLUENT_REGISTRATIONS_LOOKUP_DONOTDROP_RT"
OTEAM_FP_LISTIDS_SF_TABLE = "INFS_LPT.INFS_ORANGE_MAPPING_TABLE"

UPDATE_SUPP_SCHEDULE = f"UPDATE {SUPP_SCHEDULE_TABLE} SET STATUS = %s WHERE requestId= %s AND runNumber =%s "
UPDATE_SUPP_SCHEDULE_STATUS = f"update {SUPP_SCHEDULE_STATUS_TABLE} set status=%s, recordCount=%s, errorReason=%s where requestId=%s and runNumber=%s "

FETCH_SUPP_REQUEST_DETAILS = f'select a.id,a.name,a.channelName,a.userGroupId,a.feedType,a.removeDuplicates,' \
                             f'a.FilterMatchFields,b.requestScheduledId as ScheduleId,b.runNumber,a.isCustomFilter,' \
                             f'a.filterId,a.offerSuppressionIds,a.purdueSuppression,a.groupingColumns,a.autoGenerateFiles' \
                             f',a.ftpIds from {SUPP_REQUEST_TABLE} a ' \
                             f'join {SUPP_SCHEDULE_STATUS_TABLE} b on a.id=b.requestId where a.id=%s and b.runNumber=%s'

FETCH_PROFILE_TABLE_DETAILS = f'select sfTableName, emailField from {SUPPRESSION_PROFILE_TABLES_LOOKUP_TABLE}' \
                              f' where channelName = %s'

FETCH_SUPP_SOURCE_DETAILS = f'select a.id, a.requestId,a.sourceId,a.dataSourceId,a.inputData,b.name,b.hostname,b.port,b.username,b.password,' \
                            'b.sfAccount,b.sfDatabase,' \
                            'b.sfSchema,b.sfTable,b.sfQuery,b.sourceType,b.sourceSubType, a.indexNumber from ' \
                            f'{SUPP_MAPPING_TABLE} a left join ' \
                            f'{SOURCE_TYPES_TABLE} b on a.sourceId=b.id where a.requestId=%s and a.isDeleted=0 order by a.indexNumber'

SUPP_DATASET_MAX_RUN_NUMBER_QUERY = f" SELECT runNumber, status from {SCHEDULE_STATUS_TABLE} WHERE dataSourceId = %s AND status not in ('W','I') order by runNumber desc limit 1"
#SUPP_DATAMATCH_DETAILS_QUERY = f"SELECT filterId, isCustomFilter from {SUPP_REQUEST_TABLE} where id= %s"

DELETE_SUPPRESSION_MATCH_DETAILED_STATS = f" delete from {SUPPRESSION_MATCH_DETAILED_STATS_TABLE} where requestId= %s" \
                                          f" and runNumber = %s "
INSERT_SUPPRESSION_MATCH_DETAILED_STATS = f" insert into {SUPPRESSION_MATCH_DETAILED_STATS_TABLE} " \
                                          f"(requestId,requestScheduledId,runNumber,offerId,filterType,associateOfferId" \
                                          f",filterName,countsBeforeFilter,countsAfterFilter,downloadCount,insertCount )" \
                                          f" values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

FETCH_REQUEST_FILTER_DETAILS = "select id,name,isps,matchedDataSources,suppressionMethod," \
                               "stateSuppression,zipSuppression,filterDataSources," \
                               "applyOfferFileSuppression,applyChannelFileSuppression,applyOfferFileMatch," \
                               "applyChannelFileMatch,appendProfileFields,appendPostalFields,profileFields," \
                               "postalFields,isActive,outputRemainingData from {} where id = {}"

INSERT_INPUT_SOURCES = f"insert into {SUPPRESSION_REQUEST_INPUT_SOURCES_TABLE}(requestId,inputSource) values (%s,%s)"

INSERT_REQUEST_OFFERS = f"insert into {SUPPRESSION_REQUEST_OFFERS_TABLE} (requestId,requestScheduledId,runNumber,offerId) values (%s,%s,%s,%s)"

MAX_OFFER_THREADS_COUNT = 2

OFFER_PROCESSING_SCRIPT = f"/usr/bin/sh -x /home/zxdev/zxcustom/DATAOPS/SUPPRESSION_REQUEST/OFFER_DOWNLOADING_SERVICES/offer_consumer.sh "

CHANNEL_AFFILIATE_TABLE = "CHANNEL_LIST"
CHANNEL_OFFER_FILES_SF_SCHEMA = "LIST_PROCESSING_UAT"
OFFER_SUPP_TABLES_SF_SCHEMA = "LIST_PROCESSING_UAT"

CHANNEL_OFFER_FILES_DB_CONFIG = {
    'user': 'zxdev',
    'password': 'zxdev12#$',
    'host': '10.100.6.181',
    'database': 'GR_TOOL_DB',
    'autocommit': True,
    'allow_local_infile': True
}

FETCH_AFFILIATE_CHANNEL_VALUE = f"select channelvalue,table_prefix from {CHANNEL_AFFILIATE_TABLE} where channel = upper(%s) "

POSTAL_TABLE = "INFS_LPT_QA.POSTAL_DATA"
PROFILE_TABLE = ""
POSTAL_MATCH_FIELDS = "MD5HASH"
PROFILE_MATCH_FIELDS = "EMAIL_ID"

FETCH_FILTER_FILE_SOURCE_INFO = f"select b.hostname,b.port,b.username,b.password,b.sourceType,b.sourceSubType from {SOURCE_TYPES_TABLE} b where id = %s "

# SUPPRESSIONS tables

GREEN_GLOBAL_SUPP_TABLES = (
                            "LIST_PROCESSING_UAT.APT_CUSTOM_Datatonomy_SUPPRESSION_DND",
                            "LIST_PROCESSING_UAT.APT_CUSTDOD_ORANGE_EOS_RETURNS_INAVLID_EMAILS",
                            "LIST_PROCESSING_UAT.PFM_UNIVERSE_UNSUBS",
                            "LIST_PROCESSING_UAT.APT_CUSTDOD_NONUS_DATA_PROFILE",
                            "LIST_PROCESSING_UAT.GREEN_UNSUBS",
                            "LIST_PROCESSING_UAT.APT_CUSTOM_GLOBAL_HARDBOUNCES_DATA",
                            "LIST_PROCESSING_UAT.APT_CUSTOM_GLOBAL_SOFTINACTIVE"
                            )
GREEN_FEED_LEVEL_SUPP_TABLES = {
    'email': (
        "LIST_PROCESSING_UAT.GLOBAL_COMPLAINER_EMAILS",
        "LIST_PROCESSING_UAT.ABUSE_DETAILS",
        "LIST_PROCESSING_UAT.PFM_FLUENT_REGISTRATIONS_CANADA",
        "LIST_PROCESSING_UAT.APT_CUSTOM_GLOBAL_HARDBOUNCES_DATA",
        "LIST_PROCESSING_UAT.APT_CUSTOM_GLOBAL_SOFTINACTIVE"
    ),
    'email_listid': (
        "select email,listid from LIST_PROCESSING_UAT.UNSUB_DETAILS where listid in (select cast(listid as VARCHAR) from  LIST_PROCESSING_UAT.PFM_FLUENT_REGISTRATIONS_LOOKUP_DONOTDROP_RT where RULE in (2,3) ) ",
    )
}

INFS_FEED_LEVEL_SUPP_TABLES = {
    'email': (
        "INFS_LPT.infs_softs",
        "INFS_LPT.infs_hards",
        "INFS_LPT.APT_ADHOC_GLOBAL_SUPP_20210204",
        "INFS_LPT.abuse_details",
        "GREEN_LPT.APT_CUSTOM_GLOBAL_HARDBOUNCES_DATA",
        "GREEN_LPT.APT_CUSTOM_GLOBAL_SOFTINACTIVE"
    ),
    'email_listid': (
        "INFS_LPT.unsub_details_oteam",
        "select email,listid from INFS_LPT.EMAIL_REPLIES_TRANSACTIONAL a join INFS_LPT.GM_SUBID_DOMAIN_DETAILS b on lower(trim(a.domain))=lower(trim(b.domain)) where a.id > 17218326",
        "select email,listid from INFS_LPT.INFS_UNSUBS_ACCOUNT_WISE a join (select distinct listid,account_name from INFS_LPT.GM_SUBID_DOMAIN_DETAILS) b on a.account_name=b.account_name",
        "INFS_LPT.infs_account_level_static_suppression_data"
    ),
    'listid_profileid': (
        "INFS_LPT.APT_CUSTOM_CONVERSIONS_DATA_OTEAM",
    )
}


# email notification configurations
SUPP_MAIL_HTML_FILE = SUPP_FILE_PATH+"/mail_{}_{}.ftl"

RECEPIENT_EMAILS = []
EMAIL_SUBJECT = "Dataops {type_of_request} {request_name} {request_id}"
ERROR_EMAIL_SUBJECT = "Error: " + EMAIL_SUBJECT
MAIL_BODY = '''
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Email Template</title>
  <style>
    body {{ font-family: Arial, sans-serif; color: #333; }}
    table {{ border: 1px solid; width: 100%; border-collapse: collapse; }}
    th, td {{ border: 1px solid ; padding: 8px; text-align: left; }}
    th {{ background-color: #296695; color: white; border-color: black; }}
    tr:hover {{background-color: coral;}}
  </style>
</head>
<body>
<p>Hi Team,<br>
This is a system generated mail, please do not reply.<br>
Below are the request details.</p>
<p>Request Type: {type_of_request}<br>
Request Id: {request_id}<br>
Run Number: {run_number}<br>
Schedule Time: {schedule_time}<br>
Status: {status}<br></p>
<p>{table} </p><!--FOR TABLE INSERTION-->
<p>Thanks,<br></p>
<p>System Admin</p>
'''


PURDUE_SUPP_LOOKUP_TABLE = "PURDUE_SUPP_LOOKUP"
PURDUE_INSERT_QUERY = f"insert into {PURDUE_SUPP_LOOKUP_TABLE}(requestId,runNumber,status) values (%s,%s,%s)"
PURDUE_CHECK_INPROGRESS_QUERY = f"select requestId,runNumber from {PURDUE_SUPP_LOOKUP_TABLE} where status='I'"
PURDUE_CHECK_QUEUE_QUERY = f"select requestId,runNumber from {PURDUE_SUPP_LOOKUP_TABLE} where status='W' LIMIT 1"
PURDUE_UPDATE_STATUS_QUERY = f"update {PURDUE_SUPP_LOOKUP_TABLE} set status=%s where requestId= %s and runNumber= %s"
PURDUE_SUPP_WAITING_TIME = 90


STATS_LIMIT = 5000
SUPPRESSION_REQUEST_DATA_STATS_TABLE = "SUPPRESSION_REQUEST_DATA_STATS"
INSERT_INTO_STATS_TABLE_QUERY = f"insert into {SUPPRESSION_REQUEST_DATA_STATS_TABLE} (requestId,requestScheduledId,runNumber,stats) values (%s,%s,%s,%s)"

INSERT_AUTO_GENERATE_FILE_DETAILS = f"insert into {SUPPRESSION_REQUEST_FILES_INPUT_TABLE} (requestId, offerIds, " \
                                    f"groupingColumns, inputDataSources, ftpIds, createdDate, createdBy, updatedBy) " \
                                    f"values(%s, %s, %s, %s, %s, UTC_TIMESTAMP(), %s, %s)"

STATS_TABLE_OUTFILE_QUERY = f"SELECT requestId,requestScheduledId,runNumber,stats FROM {SUPPRESSION_REQUEST_DATA_STATS_TABLE} WHERE  requestId= %s AND requestScheduledId = %s AND runNumber= %s "


FETCH_ERROR_MSG = f" select group_concat(error_desc) as error_msg from {FILE_DETAILS_TABLE} where dataSourceScheduleId = %s and runNumber = %s and error_desc!=''"
SUPP_FETCH_ERROR_MSG = f" select group_concat(error_desc) as error_msg from {SUPP_FILE_DETAILS_TABLE} where suppressionRequestScheduleId = %s and runNumber = %s and error_desc!=''"

FETCH_FAILED_OFFERS = f"select group_concat(offerId) as failed_offers from {SUPPRESSION_REQUEST_OFFERS_TABLE} where requestid = %s and runNumber = %s and status='F'"

FETCH_SUCCESS_OFFERS = f"select group_concat(offerId) as success_offers from {SUPPRESSION_REQUEST_OFFERS_TABLE} where requestid = %s and runNumber = %s and status='S'"
