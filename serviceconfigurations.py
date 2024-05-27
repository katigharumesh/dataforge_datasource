# import statements

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
import concurrent.futures
import pytz
import requests
import urllib
import queue
from queue import Queue
import logging
from datetime import datetime, timedelta

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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
FILE_PATH = "/zds-stg-cas/zxcustom/DATAOPS/DATASET/r_logs/" + "r_logs"  # local file path - mount to download the temp files
PID_FILE = SCRIPT_PATH + "app_REQUEST_ID.pid"
LOG_FILES_REMOVE_LIMIT = 30

MAIL_HTML_FILE = SCRIPT_PATH + "mail.html"

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
                       f'{SOURCE_TYPES_TABLE} b on a.sourceId=b.id where a.dataSourceId=%s'

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
RECEPIENT_EMAILS = ["glenka@zetaglobal.com", "ukatighar@zetaglobal.com", "nuggina@zetaglobal.com"]
SUBJECT = "PROXY TESTING REPORT"

SF_DELETE_OLD_DETAILS_QUERY = "delete from %s where filename in (%s)"
FETCH_LAST_ITERATION_FILE_DETAILS_QUERY = f"select filename,last_modified_time,size,count,file_status as status,error_desc as error_msg from {FILE_DETAILS_TABLE} where dataSourceMappingId=%s and runNumber=%s and file_status='C' "  # get last iteration files data
LAST_SUCCESSFUL_RUN_NUMBER_QUERY = f"select max(runNumber) as runNumber from {SCHEDULE_STATUS_TABLE} where dataSourceId=%s and status='C'"

'''' SUPPRESSION REQUEST CONFIGS '''

SUPP_SCRIPT_PATH = r"/home/zxdev/zxcustom/DATAOPS/SUPPRESSION_REQUEST/"
#SUPP_SCRIPT_PATH = r"D:\tmp\data_forge"
SUPP_LOG_PATH = SUPP_SCRIPT_PATH + "supp_logs"
SUPP_FILE_PATH = "/zds-stg-cas/zxcustom/DATAOPS/SUPPRESSION_REQUEST/r_logs"  # local file path - mount to download the temp files
SUPP_PID_FILE = SUPP_SCRIPT_PATH + "supp_REQUEST_ID.pid"

SUPP_SOURCE_TABLE_PREFIX = "SUPPRESSION_INPUT_SOURCE_"
MAIN_INPUT_SOURCE_TABLE_PREFIX = "SUPPRESSION_REQUEST_MAIN_INPUT_"

SUPP_REQUEST_TABLE = "SUPPRESSION_REQUEST"
SUPP_SCHEDULE_TABLE = "SUPPRESSION_REQUEST_SCHEDULE"
SUPP_MAPPING_TABLE = "SUPPRESSION_REQUEST_MAPPING"
SUPP_SCHEDULE_STATUS_TABLE = "SUPPRESSION_REQUEST_SCHEDULE_STATUS"
SUPPRESSION_MATCH_DETAILED_STATS_TABLE = "SUPPRESSION_MATCH_DETAILED_STATS"
SUPPRESSION_REQUEST_FILTERS_TABLE = "SUPPRESSION_REQUEST_FILTERS"
SUPPRESSION_PRESET_FILTERS_TABLE = "SUPPRESSION_PRESET_FILTERS"
SUPPRESSION_REQUEST_OFFERS_TABLE = "SUPPRESSION_REQUEST_OFFERS"

FP_LISTIDS_SF_TABLE = "GREEN_LPT.PFM_FLUENT_REGISTRATIONS_LOOKUP_DONOTDROP_RT"

UPDATE_SUPP_SCHEDULE = f"UPDATE {SUPP_SCHEDULE_TABLE} SET STATUS = %s WHERE requestId= %s AND runNumber =%s "
UPDATE_SUPP_SCHEDULE_STATUS = f"update {SUPP_SCHEDULE_STATUS_TABLE} set status=%s, recordCount=%s, errorReason=%s where requestId=%s and runNumber=%s "

FETCH_SUPP_REQUEST_DETAILS = f'select a.id,a.name,a.channelName,a.userGroupId,a.feedType,a.removeDuplicates,' \
                             f'a.FilterMatchFields,b.requestScheduledId as ScheduleId,b.runNumber,a.isCustomFilter,a.filterId from {SUPP_REQUEST_TABLE} a ' \
                             f'join {SUPP_SCHEDULE_STATUS_TABLE} b on a.id=b.requestId where a.id=%s and b.runNumber=%s'

FETCH_SUPP_SOURCE_DETAILS = f'select a.id, a.requestId,a.sourceId,a.dataSourceId,a.inputData,b.name,b.hostname,b.port,b.username,b.password,' \
                            'b.sfAccount,b.sfDatabase,' \
                            'b.sfSchema,b.sfTable,b.sfQuery,b.sourceType,b.sourceSubType from ' \
                            f'{SUPP_MAPPING_TABLE} a left join ' \
                            f'{SOURCE_TYPES_TABLE} b on a.sourceId=b.id where a.requestId=%s '

SUPP_DATASET_MAX_RUN_NUMBER_QUERY = f" SELECT runNumber, status from {SCHEDULE_STATUS_TABLE} WHERE dataSourceId = %s AND status not in ('W','I') order  by runNumber desc limit 1"
SUPP_DATAMATCH_DETAILS_QUERY = f"SELECT filterId, isCustomFilter from {SUPP_REQUEST_TABLE} where id= %s"

INSERT_SUPPRESSION_MATCH_DETAILED_STATS = f" insert into {SUPPRESSION_MATCH_DETAILED_STATS_TABLE} " \
                                          f"(requestId,requestScheduledId,runNumber,offerId,filterType,associateOfferId" \
                                          f",filterName,countsBeforeFilter,countsAfterFilter,downloadCount,insertCount )" \
                                          f" values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

FETCH_REQUEST_FILTER_DETAILS = "select id,name,isps,matchedDataSources,suppressionMethod,offerSuppression," \
                               "purdueSuppression,stateSuppression,zipSuppression,filterDataSources," \
                               "applyOfferFileSuppression,applyChannelFileSuppression,applyOfferFileMatch," \
                               "applyChannelFileMatch,appendProfileFields,appendPostalFields,profileFields," \
                               "postalFields from {} where id = {}"

INSERT_REQUEST_OFFERS = f"insert into {SUPPRESSION_REQUEST_OFFERS_TABLE} (requestId,requestScheduledId,runNumber,offerId) values (%s,%s,%s,%s)"

MAX_OFFER_THREADS_COUNT = 2

OFFER_PROCESSING_SCRIPT = f"/usr/bin/sh -x /home/zxdev/zxcustom/DATAOPS/SUPPRESSION_REQUEST/OFFER_DOWNLOADING_SERVICES/offer_consumer.sh "

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

POSTAL_TABLE = "INFS_LPT.POSTAL_DATA"
PROFILE_TABLE = ""
POSTAL_MATCH_FIELDS = ""
PROFILE_MATCH_FIELDS = ""

FETCH_FILTER_FILE_SOURCE_INFO = f"select b.hostname,b.port,b.username,b.password,b.sourceType,b.sourceSubType from {SOURCE_TYPES_TABLE} b where id = %s "

# SUPPRESSIONS tables

GREEN_GLOBAL_SUPP_TABLES = (
                            "GREEN_LPT.APT_CUSTOM_Datatonomy_SUPPRESSION_DND",
                            "GREEN_LPT.APT_CUSTDOD_ORANGE_EOS_RETURNS_INAVLID_EMAILS",
                            "GREEN_LPT.PFM_UNIVERSE_UNSUBS",
                            "GREEN_LPT.APT_CUSTDOD_NONUS_DATA_PROFILE",
                            "GREEN_LPT.GREEN_UNSUBS",
                            "GREEN_LPT.APT_CUSTOM_GLOBAL_HARDBOUNCES_DATA",
                            "GREEN_LPT.APT_CUSTOM_GLOBAL_SOFTINACTIVE"
                            )
GREEN_FEED_LEVEL_SUPP_TABLES = {
    'email': (
        "INFS_LPT.GLOBAL_COMPLAINER_EMAILS",
        "INFS_LPT.ABUSE_DETAILS",
        "GREEN_LPT.PFM_FLUENT_REGISTRATIONS_CANADA",
        "GREEN_LPT.APT_CUSTOM_GLOBAL_HARDBOUNCES_DATA",
        "GREEN_LPT.APT_CUSTOM_GLOBAL_SOFTINACTIVE"
    ),
    'email_listid': (
        "select email,listid from GREEN_LPT.UNSUB_DETAILS where listid in (select cast(listid as VARCHAR) from  GREEN_LPT.PFM_FLUENT_REGISTRATIONS_LOOKUP_DONOTDROP_RT where RULE in (2,3) ) ",
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

FROM_EMAIL = "noreply-notifications@zetaglobal.com"
#RECEPIENT_EMAILS = []
EMAIL_SUBJECT = "Dataops {} {}"
ERROR_EMAIL_SUBJECT = "Error: " + EMAIL_SUBJECT
MAIL_BODY = '''Hi Team,

This is a system generated mail, please do not reply.

Below are the request details.

Request Type: {}
Request Id: {}
Run Number: {}
Schedule Time: {}
Status: {}


Thanks,
System Admin
'''


