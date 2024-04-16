
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

SCRIPT_PATH = r"/u3/zx_tenant/ganesh/dataforge_datasource"
LOG_PATH = r"/u3/zx_tenant/ganesh/dataforge_datasource/app_logs"
PID_FILE = SCRIPT_PATH + "/app.pid"
LOG_FILES_REMOVE_LIMIT = 30

MAIL_HTML_FILE = SCRIPT_PATH + "mail.html"

THREAD_COUNT = 20  # thread count

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

MYSQL_CONFIGS = {
    "host": "10.218.18.157",
    "user": "smartuser",
    "password": "smart12#$",
    "database": "CAMPAIGN_TOOL_GMAIL",
    "autocommit": True,
    "allow_local_infile": True
}

FETCH_MAIN_DATASOURCE_DETAILS = 'select id,name,channelName,userGroupId,feedType,dataProcessingType,' \
                                'FilterMatchFields,isps from SUPPRESSION_DATASOURCE where id=REQUEST_ID '

FETCH_SOURCE_DETAILS = 'select a.id,a.dataSourceId,a.sourceId,a.inputData,b.hostname,b.port,b.username,b.password,' \
                       'b.sfAccount,b.sfDatabase,' \
                       'b.sfSchema,b.sfTable,b.sfQuery,b.sourceType,b.sourceSubType from ' \
                       'SUPPRESSION_DATASOURCE_MAPPING a join ' \
                       'SUPPRESSION_SOURCE_TYPES b on a.sourceId=b.id where a.dataSourceId=REQUEST_ID '

SNOWFLAKE_CONFIGS = {
    "account": 'zetaglobal.us-east-1',
    "user": "green_lp_service",
    "password": "Jsw44QTLRYYGLGBgfhXQR7webwaxArWx",
    "database": "GREEN",
    "warehouse": "GREEN_ADHOC",
    "schema": 'INFS_LPT'
}

STAGE_TABLE_PREFIX = 'STAGE_SUPPRESSION_DATASOURCE_MAPPING_'
SOURCE_TABLE_PREFIX = 'SUPPRESSION_DATASOURCE_MAPPING_'
MAIN_DATASOURCE_TABLE_PREFIX = 'SUPPRESSION_DATASOURCE_'

OPERATOR_MAPPING = {'on': '=', 'after': '>', 'before': '<', 'between': 'between', 'greater than': '>', 'less than': '<',
                    'equals': '=', 'not equals': '!=', 'like': 'like', 'not like': 'not like', 'exists in': 'in',
                    'not exists in': 'not in', 'predefined daterange': '>='}

FROM_EMAIL = "noreply-notifications@zetaglobal.com"
RECEPIENT_EMAILS = ["glenka@zetaglobal.com", "ukatighar@zetaglobal.com", "nuggina@zetaglobal.com"]
SUBJECT = "PROXY TESTING REPORT"

SF_DELETE_OLD_DETAILS_QUERY = "delete from SOURCE_TABLE where filename in (FILES)"
FETCH_LAST_ITERATION_FILE_DETAILS_QUERY = "select filename,last_modified_time,size,count from x"  # get last iteration files data
LAST_SUCCESSFULL_RUN_NUMBER_QUERY = ""
RUN_NUMBER_QUERY = "select runNumber from SUPPRESSION_DATASOURCE_SCHEDULE where dataSourceId=REQUEST_ID"  # query to fetch run number
file_path = r"/u3/zx_tenant/ganesh/dataforge_datasource/temp_files"  # local file path - mount to download the temp files
