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
SCRIPT_PATH = r"/gmservices/DATAOPS/DATASET/"

LOG_PATH = SCRIPT_PATH + "app_logs"
FILE_PATH = "/gmserviceslogs/DATAOPS/DATASET/r_logs/"   # local file path - mount to download the temp files
PID_FILE = SCRIPT_PATH + "app_REQUEST_ID.pid"
LOG_FILES_REMOVE_LIMIT = 30

MAIL_HTML_FILE = FILE_PATH + "mail_{}_{}.ftl"

THREAD_COUNT = 10  # thread count

skype_configurations = {
    'url': 'http://zds-prod-ext-greenapp1-vip.bo3.e-dialog.com/sendSkypeAlerts/index.php?key=',
    'file_path': SCRIPT_PATH,
    'default_channel': '19:69777cb1e0d94ef9ba894c5d4d7eb3b6@thread.skype',
    'script_path': SCRIPT_PATH,
    'script_name': 'Data Ops',
    'server': 'zdl3-mn09.bo3.e-dialog.com',
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
    'host': 'zx-prod-db1-vip.bo3.e-dialog.com',
    'database': 'CAMPAIGN_TOOL',
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
    "warehouse": "GREEN_DF_SUPPREQ",
    "schema": 'DF_SUPPREQ'
}

STAGE_TABLE_PREFIX = 'STAGE_DO_DATASET_MAPPING_'
SOURCE_TABLE_PREFIX = 'DO_DATASET_MAPPING_'
MAIN_DATASET_TABLE_PREFIX = 'DO_DATASET_'

# OPERATOR_MAPPING = {'on': '=', 'after': '>', 'before': '<', 'between': 'between', 'greater than': '>', 'less than': '<'
# , 'equals': '=', 'not equals': '!=', 'like': 'like', 'not like': 'not like', 'exists in': 'in',
#                    'not exists in': 'not in', 'predefined daterange': '>='}


SF_DELETE_OLD_DETAILS_QUERY = "delete from %s where DO_INPUTSOURCE in (%s)"
FETCH_LAST_ITERATION_FILE_DETAILS_QUERY = f"select filename,last_modified_time,size,count,file_status as status,error_desc as error_msg from {FILE_DETAILS_TABLE} where dataSourceMappingId=%s and runNumber=%s and file_status='C' "  # get last iteration files data
LAST_SUCCESSFUL_RUN_NUMBER_QUERY = f"select  if(max(runNumber) is NULL,-1,max(runNumber)) as runNumber from {SCHEDULE_STATUS_TABLE} where dataSourceId=%s and status in ('C','P')"

'''' SUPPRESSION REQUEST CONFIGS '''

SUPP_SCRIPT_PATH = r"/gmservices/DATAOPS/SUPPRESSION_REQUEST/"
#SUPP_SCRIPT_PATH = r"D:\tmp\data_forge"
SUPP_LOG_PATH = SUPP_SCRIPT_PATH + "supp_logs"
SUPP_FILE_PATH = "/gmserviceslogs/DATAOPS/SUPPRESSION_REQUEST/r_logs/"  # local file path - mount to download the temp files
SUPP_PID_FILE = SUPP_SCRIPT_PATH + "supp_REQUEST_ID.pid"

SUPP_SOURCE_TABLE_PREFIX = "DO_SUPPRESSION_REQUEST_MAPPING_"
MAIN_INPUT_SOURCE_TABLE_PREFIX = "DO_SUPPRESSION_REQUEST_"

SUPP_REQUEST_TABLE = "SUPPRESSION_REQUEST"
SUPP_SCHEDULE_TABLE = "SUPPRESSION_REQUEST_SCHEDULE"
SUPP_MAPPING_TABLE = "SUPPRESSION_REQUEST_MAPPING"
SUPP_SCHEDULE_STATUS_TABLE = "SUPPRESSION_REQUEST_SCHEDULE_STATUS"
SUPPRESSION_MATCH_DETAILED_STATS_TABLE = "SUPPRESSION_MATCH_SUPPRESSION_BREAKDOWN_STATS"
SUPPRESSION_REQUEST_FILTERS_TABLE = "SUPPRESSION_REQUEST_FILTERS"
SUPPRESSION_PRESET_FILTERS_TABLE = "SUPPRESSION_PRESET_FILTERS"
SUPPRESSION_REQUEST_OFFERS_TABLE = "SUPPRESSION_REQUEST_OFFERS"
SUPPRESSION_PROFILE_TABLES_LOOKUP_TABLE = "SUPPRESSION_PROFILE_TABLES_LOOKUP"
SUPPRESSION_REQUEST_INPUT_SOURCES_TABLE = "SUPPRESSION_REQUEST_INPUT_SOURCES"
SUPPRESSION_REQUEST_FILES_INPUT_TABLE = "SUPPRESSION_REQUEST_FILES_INPUT"

SUPP_FILE_DETAILS_TABLE = 'SUPPRESSION_REQUEST_FILE_DETAILS'

SUPP_LAST_SUCCESSFUL_RUN_NUMBER_QUERY = f"select if(max(runNumber) is NULL,-1,max(runNumber)) as runNumber from {SUPP_SCHEDULE_STATUS_TABLE} where requestId=%s and status in ('C','P')"

SUPP_FETCH_LAST_ITERATION_FILE_DETAILS_QUERY = f"select filename,last_modified_time,size,count,file_status as status,error_desc as error_msg from {SUPP_FILE_DETAILS_TABLE} where suppressionRequestMappingId=%s and runNumber=%s and file_status='C' "

SUPP_INSERT_FILE_DETAILS = f'insert into {SUPP_FILE_DETAILS_TABLE}(suppressionRequestScheduleId,runNumber,suppressionRequestMappingId,count,fileName,createdBy,updatedBy,size,last_modified_time,file_status,error_desc) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

SUPP_DELETE_FILE_DETAILS = f'delete from {SUPP_FILE_DETAILS_TABLE} where suppressionRequestScheduleId=%s and runNumber=%s '

FP_LISTIDS_SF_TABLE = "GREEN_LPT.PFM_FLUENT_REGISTRATIONS_LOOKUP_DONOTDROP_RT"
OTEAM_FP_LISTIDS_SF_TABLE = "INFS_LPT.INFS_ORANGE_MAPPING_TABLE"

FETCH_GM_CONFIGURED_ISPS = '''select group_concat(concat("'",ispName,"'")) as isps from ISP_DETAILS where isDeleted=0'''
HOTMAIL_ISPS = "'hotmail.com','live.com','msn.com','outlook.com'"

FETCH_DATASET_NAME = f"select name from {DATASET_TABLE} where id = %s"
FETCH_DATASET_COUNT = f"select recordCount from {SCHEDULE_STATUS_TABLE} where dataSourceId = %s and runNumber = %s "

FETCH_SUPP_REQUEST_INITIAL_COUNT = f"select sum(count) as Total_Count from {SUPP_FILE_DETAILS_TABLE} where " \
                                   f"suppressionRequestScheduleId = %s and runNumber = %s"

UPDATE_SUPP_SCHEDULE = f"UPDATE {SUPP_SCHEDULE_TABLE} SET STATUS = %s WHERE requestId= %s AND runNumber =%s "
UPDATE_SUPP_SCHEDULE_STATUS = f"update {SUPP_SCHEDULE_STATUS_TABLE} set status=%s, recordCount=%s, errorReason=%s where requestId=%s and runNumber=%s "

FETCH_SUPP_REQUEST_DETAILS = f'select a.id,a.name,a.channelName,a.userGroupId,a.feedType,a.removeDuplicates,' \
                             f'a.FilterMatchFields,b.requestScheduledId as ScheduleId,b.runNumber,a.isCustomFilter,' \
                             f'a.filterId,a.offerSuppressionIds,a.purdueSuppression,a.groupingColumns,a.autoGenerateFiles' \
                             f',a.ftpIds from {SUPP_REQUEST_TABLE} a ' \
                             f'join {SUPP_SCHEDULE_STATUS_TABLE} b on a.id=b.requestId where a.id=%s and b.runNumber=%s'

FETCH_PROFILE_TABLE_DETAILS = f'select sfTableName, emailField, listIdField from {SUPPRESSION_PROFILE_TABLES_LOOKUP_TABLE}' \
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
                               "applyChannelFileMatch,matchMockingBirdData,matchJornayaData,appendProfileFields," \
                               "appendPostalFields,profileFields," \
                               "postalFields,isActive,outputRemainingData from {} where id = {}"

JORNAYA_TABLE = 'DF_DATASYNC.JORNAYA_DATA'
MOCKINGBIRD_TABLE = 'DF_DATASYNC.MOCKINGBIRD_DATA'

INSERT_INPUT_SOURCES = f"insert into {SUPPRESSION_REQUEST_INPUT_SOURCES_TABLE}(requestId,inputSource) values (%s,%s)"
DELETE_OLD_INPUT_SOURCES = f"delete from {SUPPRESSION_REQUEST_INPUT_SOURCES_TABLE} where requestId = %s"

INSERT_REQUEST_OFFERS = f"insert into {SUPPRESSION_REQUEST_OFFERS_TABLE} (requestId,requestScheduledId,runNumber,offerId) values (%s,%s,%s,%s)"

MAX_OFFER_THREADS_COUNT = 2

OFFER_PROCESSING_SCRIPT = f"/usr/bin/sh -x /gmservices/DATAOPS/SUPPRESSION_REQUEST/OFFER_DOWNLOADING_SERVICES/offer_consumer.sh "

CHANNEL_AFFILIATE_TABLE = "CHANNEL_LIST"
CHANNEL_OFFER_FILES_SF_SCHEMA = "LIST_PROCESSING"
OFFER_SUPP_TABLES_SF_SCHEMA = "LIST_PROCESSING"
CAKE_CONVERTION_TABLES_SF_SCHEMA = "LIST_PROCESSING"

CHANNEL_OFFER_FILES_DB_CONFIG = {
    'user': 'techuser',
    'password': 'tech12#$',
    'host': 'zds-prod-mdb-vip.bo3.e-dialog.com',
    'database': 'GR_TOOL_DB',
    'autocommit': True,
    'allow_local_infile': True
}

FETCH_AFFILIATE_CHANNEL_VALUE = f"select channelvalue,table_prefix from {CHANNEL_AFFILIATE_TABLE} where channel = upper(%s) "

POSTAL_TABLE = "INFS_LPT.POSTAL_DATA"

APPEND_POSTAL_FIELDS_SOURCE = f"SELECT sfDatabase,sfSchema,sfTable,sfQuery FROM {SOURCE_TYPES_TABLE} WHERE sourceType = 'A' and sourceSubType = 'A' and channelName = %s"
APPEND_PROFILE_FIELDS_SOURCE = f"SELECT sfDatabase,sfSchema,sfTable,sfQuery FROM {SOURCE_TYPES_TABLE} WHERE sourceType = 'A' and sourceSubType = 'P' and channelName = %s"
POSTAL_MATCH_FIELDS = "MD5HASH"
PROFILE_MATCH_FIELDS = "EMAIL_ID"

FETCH_FILTER_FILE_SOURCE_INFO = f"select b.hostname,b.port,b.username,b.password,b.sourceType,b.sourceSubType from {SOURCE_TYPES_TABLE} b where id = %s "

# SUPPRESSIONS tables

VALID_EMAILS_FORMAT = '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

GREEN_GLOBAL_SUPP_TABLES = (
    {"Origin data Suppression": ( "GREEN_LPT.ORIGIN_UNIVERSE_UNSUBS", )
     },
    {"Global Unsubs"          : ("GREEN_LPT.PFM_UNIVERSE_UNSUBS",
                                 "GREEN_LPT.GREEN_UNSUBS",
                                 "INFS_LPT.GLOBAL_COMPLAINER_EMAILS",
                                 "GREEN_LPT.APT_CUSTOM_GLOBAL_HARDBOUNCES_DATA",
                                 "INFS_LPT.ABUSE_DETAILS")
     },
    {"Miscilenious Unsubs"   : ("GREEN_LPT.APT_CUSTOM_Datatonomy_SUPPRESSION_DND",
                                "GREEN_LPT.PFM_FLUENT_REGISTRATIONS_CANADA",
                                "GREEN_LPT.APT_CUSTDOD_ORANGE_EOS_RETURNS_INAVLID_EMAILS",
                                "GREEN_LPT.APT_CUSTDOD_NONUS_DATA_PROFILE",
                                "GREEN_LPT.APT_CUSTOM_GLOBAL_SOFTINACTIVE")
    }
   )
GREEN_FEED_LEVEL_SUPP_TABLES = {
    'email': (
        { "Global Complainers"    : ("INFS_LPT.GLOBAL_COMPLAINER_EMAILS",) },
        { "Abuses [Green + INFS]" : ("INFS_LPT.ABUSE_DETAILS",)        },
        { "Canada Suppression"    : ("GREEN_LPT.PFM_FLUENT_REGISTRATIONS_CANADA",) } ,
        { "Hard bounces"          : ("GREEN_LPT.APT_CUSTOM_GLOBAL_HARDBOUNCES_DATA",) },
        { "Soft bounces"          : ("GREEN_LPT.APT_CUSTOM_GLOBAL_SOFTINACTIVE",)  },
    ),
    'email_listid': (
        {"Feed level Unsubs"   : ("select email,listid from GREEN_LPT.UNSUB_DETAILS where listid in (select cast(listid as VARCHAR) from  GREEN_LPT.PFM_FLUENT_REGISTRATIONS_LOOKUP_DONOTDROP_RT where RULE in (2,3) ) ",) },
    )
}

INFS_FEED_LEVEL_SUPP_TABLES = {
    'email': (
        {"Static INFS Unsubs"     :   ("INFS_LPT.APT_ADHOC_GLOBAL_SUPP_20210204",)},
        { "Abuses [Green + INFS]" :   ("INFS_LPT.abuse_details",) },
        { "Hard bounces"          :   ("GREEN_LPT.APT_CUSTOM_GLOBAL_HARDBOUNCES_DATA", "INFS_LPT.infs_hards",) },
        { "Soft bounces"          :   ("GREEN_LPT.APT_CUSTOM_GLOBAL_SOFTINACTIVE", "INFS_LPT.infs_softs",)}
    ),
    'email_listid': (
        { "Account Level Unsubs":  ( "INFS_LPT.unsub_details_oteam",) },
        { "IEP Unsubs" :  ("select email,listid from INFS_LPT.EMAIL_REPLIES_TRANSACTIONAL a join INFS_LPT.GM_SUBID_DOMAIN_DETAILS b on lower(trim(a.domain))=lower(trim(b.domain)) where a.id > 17218326",)},
        { "Orange Account Unsubs" :  ("select email,listid from INFS_LPT.INFS_UNSUBS_ACCOUNT_WISE a join (select distinct listid,account_name from INFS_LPT.GM_SUBID_DOMAIN_DETAILS) b on a.account_name=b.account_name",)},
        {"Static Account level INFS Unsubs": ("INFS_LPT.infs_account_level_static_suppression_data",)}
    )
}


APPTNESS_SUPP_CODES = {
    "GCOMPR": "Global Complainers",
    "CCMP": "Channel Complainers",
    "CABUSE": "Channel Abuses",
    "CUNSUB": "Channel Unsubs",
    "FUNSUB": "Feed Unsubs & Abuses",
    "GDPRSU": "GDPR Filter",
    "EOSUPR": "EO Suppression",
    "CANADA": "Canada Suppression",
    "DUNSUB": "Data Partner Unsubs",
    "ZABUSE": "Zeta Abuses",
    "BOSUPR": "Global Bounces Suppression"
}

# email notification configurations
FROM_EMAIL = "noreply@alerts.zetaglobal.net"
#RECEPIENT_EMAILS = ["glenka@zetaglobal.com", "ukatighar@zetaglobal.com", "nuggina@zetaglobal.com"]
SUPP_MAIL_HTML_FILE = SUPP_FILE_PATH+"/mail_{}_{}.ftl"

RECEPIENT_EMAILS = []
EMAIL_SUBJECT = "[{request_id}] DataOps {type_of_request}: {request_name}"
ERROR_EMAIL_SUBJECT = "Error: " + EMAIL_SUBJECT
MAIL_BODY = '''
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Email Template</title>
  <style>
    body {{ font-family: LatoWeb, sans-serif; color: #212529; }}
    p {{ font-family: LatoWeb, sans-serif; color: #212529; }}
    table {{ border: 1px solid; width: 100%; border-collapse: collapse; }}
    th, td {{ border: 1px solid ; padding: 8px; text-align: left; }}
    th {{ background-color: #296695; color: white; border-color: black; }}
    tr:hover {{background-color: #fde995;}}
  </style>
</head>
<body>
<p>Hi Team,<br>
This is a system generated notification, please do not reply.<br>
Below are the request process details.</p>
<p>Channel: {channel}<br>
Request Type: {type_of_request}<br>
Request Id: {request_id}<br>
Run Number: {run_number}<br>
Schedule Time: {schedule_time} UTC <br>
Status: {status}<br></p>
<p>{table}</p><!--FOR TABLE INSERTION-->
<p>Thanks,<br>
System Admin</p>
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

ERROR_CODES = {
    'DO0': "Error occured due to processing of another instance. Error: PID {pidfile} existence",
    'DO1': "Error occurred while importing Input data sources - Out of '{n}' input sources, '{m}' input sources are unable to process. Below are the source(s) wise details. {error}",
    'DO2': "Service is unable to create master table to import all input sources data. Error: {error}",
    'DO3': "Error occurred while doing {operation} operation. Error: {error}",
    'DO4': "Unknown Exception occurred while processing the input data sources. Error: {error}",
    'DO5': "Unable to process the request as selected {Filter/Dataset} is inactive. Error: {Filter/Dataset} Id {id} is inactive.",
    'DO6': "Unable to process the request as selected dataset was inactivated. Error: Dataset Id {id} was inactivated.",
    'DO7': "Unable to process the request as selected dataset is in error state. Error: Dataset Id {id} is in error state",
    'DO8': "Unknown Exception occurred while validating the active state of Dataset. Error: {error}",
    'DO9': "The service is unable to perform ISP filtration. Error: {error}",
    'DO10': "The service is unable to perform profile non-match suppression. Error: {error}",
    'DO11': "The service is unable to perform match process for the channel match data. Error: {error}",
    'DO12': "The service is unable to perform suppression process for the channel suppression data. Error: {error}",
    'DO13': "The service is unable to perform request level match process. Error: {error}",
    'DO14': "The service is unable to perform request level filters. Error: {error}",
    'DO15': "The service is unable to perform global suppression. Error: {error}",
    'DO16': "The service is unable to perform feed-level suppression. Error: {error}",
    'DO17': "The service is unable to perform zip code suppression. Error: {error}",
    'DO18': "The service is unable to perform state suppression. Error: {error}",
    'DO19': "The service is unable to perform Purdue suppression. Error: {error}",
    'DO20': "The service is unable to perform offer suppression. Error: {error}",
    'DO21': "The service is unable to perform match process for offer match data. Error: {error}",
    'DO22': "The service is unable to perform suppression process for offer suppression data. Error: {error}",
    'DO23': "Unknown Exception occurred while processing the {request_type}. Error: {error}",
    'DO24': "The service is unable to summarize the request metrics. Error: {error}",
    'DO25': "Unknown Exception occurred while summarizing the request metrics. Error: {error}",
    'DO26': "The service is unable to connect to Snowflake DB. Error: {error}",
    'DO27': "The service is unable to connect to MySQL DB. Error: {error}",
    'DO28': "All offers selected for the request are failed.",
}
