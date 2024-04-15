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

SCRIPT_PATH = r"C:\Users\GaneshKumarLenka\Downloads\suppression_automation"
LOG_PATH = r"C:\Users\GaneshKumarLenka\Downloads\suppression_automation\app_logs"
PID_FILE = SCRIPT_PATH + "/app.pid"
LOG_FILES_REMOVE_LIMIT = 30

MAIL_HTML_FILE = SCRIPT_PATH + "mail.html"

count = 20  # thread count

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

mysql_source_configs = {
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

snowflake_configs = {
    "account": 'zetaglobal.us-east-1',
    "user": "green_lp_service",
    "password": "Jsw44QTLRYYGLGBgfhXQR7webwaxArWx",
    "database": "GREEN",
    "warehouse": "GREEN_ADHOC",
    "schema": 'INFS_LPT'
}


SOURCE_TABLE_PREFIX = 'SUPPRESSION_DATASOURCE_MAPPING_'
MAIN_DATASOURCE_TABLE_PREFIX = 'SUPPRESSION_DATASOURCE_'

operator_mapping = {'on': '=', 'after': '>', 'before': '<', 'between': 'between', 'greater than': '>', 'less than': '<',
                    'equals': '=', 'not equals': '!=', 'like': 'like', 'not like': 'not like', 'exists in': 'in',
                    'not exists in': 'not in', 'predefined daterange': '>='}

FROM_EMAIL = "noreply-notifications@zetaglobal.com"
RECEPIENT_EMAILS = ["glenka@zetaglobal.com", "ukatighar@zetaglobal.com", "nuggina@zetaglobal.com"]
SUBJECT = "PROXY TESTING REPORT"

























































































































































































































TABLE_NAME=""
stage_name=""
fetch_last_iteration_file_details_query = "select filename,last_modified_time,size,count from x" # get last iteration files data
run_number_query = "select runNumber from SUPPRESSION_DATASOURCE_FILE_DETAILS where dataSourceMappingId=REQUEST_ID"  # query to fetch run number
file_path = "/suppression/shyam/"  # local file path - mount to download the temp files
import paramiko
import ftplib


class FileTransfer:
    def __init__(self, hostname, port, username, password):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.connection = None
        self.metadata = {}

    def connect(self):
        if self.port == 22:
            # Connect using SFTP
            self.connection = paramiko.Transport((self.hostname, self.port))
            self.connection.connect(username=self.username, password=self.password)
        else:
            # Connect using FTP
            self.connection = ftplib.FTP()
            self.connection.connect(self.hostname, self.port)
            self.connection.login(self.username, self.password)

    def list_files(self, remote_directory):
        if isinstance(self.connection, paramiko.Transport):
            sftp = paramiko.SFTPClient.from_transport(self.connection)
            return sftp.listdir(remote_directory)
        elif isinstance(self.connection, ftplib.FTP):
            return self.connection.nlst(remote_directory)

    def download_file(self, remote_file, local_file):
        if isinstance(self.connection, paramiko.Transport):
            sftp = paramiko.SFTPClient.from_transport(self.connection)
            sftp.get(remote_file, local_file)
        elif isinstance(self.connection, ftplib.FTP):
            with open(local_file, 'wb') as f:
                self.connection.retrbinary('RETR ' + remote_file, f.write)

    def get_file_metadata(self, file_path):
        if isinstance(self.connection, paramiko.Transport):
            sftp = paramiko.SFTPClient.from_transport(self.connection)
            file_attr = sftp.stat(file_path)
            self.metadata = {
                'size': file_attr.st_size,
                'last_modified': file_attr.st_mtime
            }
            return self.metadata
        elif isinstance(self.connection, ftplib.FTP):
            try:
                self.metadata = {}
                size = self.connection.size(file_path)
                self.metadata['size'] = size if size is not None else 'N/A'
                modified_time = self.connection.sendcmd('MDTM ' + file_path)
                self.metadata['last_modified'] = modified_time[-14:] if modified_time.startswith('213') else 'N/A'
                return self.metadata
            except ftplib.error_perm as e:
                print(f"Error: {e}")
                return None

    def close(self):
        self.connection.close()


def process_sftp_ftp_nfs_request(sourcesubtype , hostname, port, username, password, inputData_dict, mysql_cursor, consumer_logger, id):
    try:
        if sourcesubtype =="S":
            consumer_logger.info("Request initiated to process.. File source: SFTP/FTP ")
            consumer_logger.info("Getting SFTP/FTP connection...")
            ftpObj = FileTransfer(hostname, port, username, password)
            ftpObj.connect()
            consumer_logger.info("SFTP/FTP connection established successfully.")
        elif sourcesubtype == "N":
            consumer_logger.info("Request initiated to process.. File source: NFS ")
            ftpObj = LocalFileTransfer(inputData_dict["filePath"])
        else:
            consumer_logger.info("Wrong method called.. ")
        isFile = False if inputData_dict["filePath"].endswith("/") else True
        isDir = True if inputData_dict["filePath"].endswith("/") else False

        result = {}
        mysql_cursor.execute(run_number_query.replace("REQUEST_ID",id))
        run_number = int(mysql_cursor.fetchone()[0])
        if isFile:
            if run_number == 1:
                file_details_list = []
                consumer_logger.info(f"Processing file started. File : {inputData_dict['filePath']}")
                file_details_dict = process_single_file(run_number, ftpObj, inputData_dict['filePath'], consumer_logger, inputData_dict)
                file_details_list.append(file_details_dict)

            else:
                file_details_list = []
                consumer_logger.info(f"Processing file started. File : {inputData_dict['filePath']}")
                mysql_cursor.execute(fetch_last_iteration_file_details_query)
                last_iteration_files_details = mysql_cursor.fetchall()
                file_details_dict = process_single_file(run_number,ftpObj, inputData_dict['filePath'], consumer_logger, inputData_dict , last_iteration_files_details)
                file_details_list.append(file_details_dict)
                # snowflake process from local file to stage , stage to temp table
            result["files"] = file_details_list
            return result
        elif isDir:
            consumer_logger.info("Given source is a path. List of files need to be considered.")
            files_list = ftpObj.list_files(inputData_dict["filePath"])
            if run_number == 1:
                file_details_list = []
                consumer_logger.info("First time dump processing all the files..")
                for file in files_list :
                    fully_qualified_file = inputData_dict["filePath"] + file
                    file_details_dict = process_single_file(ftpObj,fully_qualified_file)
                    file_details_list.append(file_details_dict)
            else:
                mysql_cursor.execute(fetch_last_iteration_file_details_query)
                last_iteration_files_details = mysql_cursor.fetchall()
                for file in last_iteration_files_details :
                    if file['filename'] not in files_list :
                        pass
                for file in files_list:
                    fully_qualified_file = inputData_dict["filePath"] + file
                    file_details_dict = process_single_file(run_number,ftpObj, inputData_dict['filePath'], consumer_logger, inputData_dict , last_iteration_files_details)


                        # delete from existing table . i.e. file is not present in the path.
        else:
            consumer_logger.info("Wrong Input...raising Exception..")

    except Exception as e:
        print(f"Except occurred. Please look into it. {str(e)}")
        consumer_logger.info(f"Except occurred. Please look into it. {str(e)}")
        raise Exception(str(e))


def process_single_file(run_number,ftpObj, fully_qualified_file, consumer_logger, inputData_dict,last_iteration_files_details=[]):
    try:
        file_details_dict = {}
        consumer_logger.info("Processing file for the first time...")
        file = fully_qualified_file.split("/")[-1]
        meta_data = ftpObj.get_file_metadata(fully_qualified_file)
        ftpObj.download_file(fully_qualified_file, file_path)
        line_count = sum(1 for _ in open(file_path + file, 'r'))
        file_details_dict["name"] = file
        file_details_dict["count"] = line_count
        file_details_dict["size"] = meta_data["size"]
        file_details_dict["last_modified_time"] = meta_data["last_modified"]
        sf_conn = snowflake.connector.connect(**snowflake_configs)
        sf_cursor=sf_conn.cursor()
        field_delimiter = inputData_dict["delimiter"]
        if run_number == 1:
            if inputData_dict['isHeaderExists'] == 0:
                consumer_logger.info("Header not available for file.. Creating stage according to it.")
                sf_create_stage_query = f" CREATE STAGE {stage_name} FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = '{field_delimiter}', FIELD_OPTIONALLY_ENCLOSED_BY = '\"' )  ; PUT file://{file_path}/{file} @{stage_name} ;"
            else:
                consumer_logger.info("Header available for file.. Creating stage according to it.")
                sf_create_stage_query=f" CREATE STAGE {stage_name} FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = '{field_delimiter}', FIELD_OPTIONALLY_ENCLOSED_BY = '\"' , SKIP_HEADER =1)  ; PUT file://{file_path}/{file} @{stage_name} ;"
            consumer_logger.info(f"Executing query: {sf_create_stage_query}")
            sf_cursor.execute(sf_create_stage_query)
            header_list=inputData_dict['headerValue'].split(",")
            sf_create_table_query = f"create transient table if not exists {SOURCE_TABLE_PREFIX+TABLE_NAME}  ( "
            sf_create_table_query += " varchar ,".join(i for i in header_list)
            sf_create_table_query += " varchar , filename varchar )"
            consumer_logger.info(f"Executing query: {sf_create_table_query}")
            sf_cursor.execute(sf_create_table_query)
            stage_columns = ", ".join(f"${i+1}" for i in range(len(header_list)))
            sf_copy_into_query = f"copy into {SOURCE_TABLE_PREFIX+TABLE_NAME} FROM (select {stage_columns}, '{file}' FROM @stage_name ) "
            consumer_logger.info(f"Executing query: {sf_copy_into_query}")
            sf_cursor.execute(sf_copy_into_query)
        else:
            if file in [i['filename'] for i in last_iteration_files_details]:
                file_index = [i['filename'] for i in last_iteration_files_details].index(file)
                if meta_data['size'] == last_iteration_files_details[file_index]['size'] and meta_data['last_modified'] == last_iteration_files_details[file_index]['last_modified_time']:
                    consumer_logger.info("File already processed last time.. So skipping the file.")
                    return file_details_dict
                else:
                    consumer_logger.info("File has changes in it , creating a clone for the table and reprocessing the file.")
                    sf_create_clone_query=f"create or replace {SOURCE_TABLE_PREFIX+TABLE_NAME}_CLONE clone {SOURCE_TABLE_PREFIX+TABLE_NAME}"
                    consumer_logger.info(f"Executing query: {sf_create_clone_query}")
                    sf_cursor.execute(sf_create_clone_query)
                    sf_delete_old_query=f"delete from {SOURCE_TABLE_PREFIX+TABLE_NAME}_CLONE where filename = {file}"
                    consumer_logger.info(f"Executing query: {sf_delete_old_query}")
                    sf_cursor.execute(sf_delete_old_query)
                    process_single_file(1,ftpObj,fully_qualified_file, consumer_logger, inputData_dict)

            else:
                consumer_logger.info("File processing first time..So only dump is required")
                process_single_file(1, ftpObj, fully_qualified_file, consumer_logger, inputData_dict)
        return file_details_dict
    except Exception as e:
        consumer_logger.error(f"Exception occurred. PLease look into this. {str(e)}")
        raise Exception(f"Exception occurred. PLease look into this. {str(e)}")










def process_nfs_request(inputData_dict,mysql_cursor,consumer_logger,id):
    try:
        consumer_logger.info("Processing request : NFS mount path/file")
        result={}
        isFile = False if inputData_dict["filePath"].endswith("/") else True
        isDir = True if inputData_dict["filePath"].endswith("/") else False
        mysql_cursor.execute(run_number_query.replace("REQUEST_ID", id))
        run_number = int(mysql_cursor.fetchone()[0])
        if isFile:
            if run_number == 1:
                process_single_file(run_number,)
            else:
                pass
        if isDir:
            if run_number == 1:
                pass
            else:
                pass
    except Exception as e:
        consumer_logger.error(f"Exception occurred . Please look into this.. {str(e)}")






import os

class LocalFileTransfer:
    def __init__(self, mount_path):
        self.mount_path = mount_path

    def list_files(self, directory):
        try:
            full_path = os.path.join(self.mount_path, directory)
            return os.listdir(full_path)
        except FileNotFoundError:
            print(f"Directory '{directory}' not found.")
            return []

    def download_file(self, remote_file, local_file):
        try:
            remote_path = os.path.join(self.mount_path, remote_file)
            with open(remote_path, 'rb') as src, open(local_file, 'wb') as dst:
                dst.write(src.read())
        except FileNotFoundError:
            print(f"File '{remote_file}' not found.")
        except Exception as e:
            print(f"Error downloading file '{remote_file}': {e}")

    def get_file_metadata(self, file_path):
        try:
            full_path = os.path.join(self.mount_path, file_path)
            file_stat = os.stat(full_path)
            metadata = {
                'size': file_stat.st_size,
                'last_modified': file_stat.st_mtime
            }
            return metadata
        except FileNotFoundError:
            print(f"File '{file_path}' not found.")
            return None

