import json

from serviceconfigurations import *


def load_data_source(source, main_datasource_details, consumer_logger):
    try:
        consumer_logger.info(f"Processing task : {str(source)}")
        data_source_mapping_id = source['id']
        data_source_id = source['dataSourceId']
        source_id = source['sourceId']
        input_data = source['inputData']
        hostname = source['hostname']
        port = source['port']
        username = source['username']
        password = source['password']
        sf_account = source['sfAccount']
        sf_database = source['sfDatabase']
        sf_schema = source['sfSchema']
        sf_table = source['sfTable']
        sf_query = source['sfQuery']
        source_type = source['sourceType']
        source_sub_type = source['sourceSubType']
        input_data_dict = json.loads(input_data)
        consumer_logger.info(f"Acquiring mysql connection...")
        mysql_conn = mysql.connector.connect(**mysql_source_configs)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        consumer_logger.info(f"Acquiring snowflake connection...")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        source_table = SOURCE_TABLE_PREFIX + str(data_source_mapping_id)
        if source_type == "F":
            if source_sub_type == "S":
                pass
            elif source_sub_type == "N":
                pass
            elif source_sub_type == "A":
                pass
            elif source_sub_type == "D":
                pass
            else:
                consumer_logger.info("Unknown source_sub_type selected")
                raise Exception("Unknown source_sub_type selected")
        elif source_type == "D":
            if sf_account != SNOWFLAKE_CONFIGS['account']:
                consumer_logger.info("Snowflake account mismatch. Pending implementation ...")
                raise Exception("Snowflake account mismatch. Pending implementation ...")
            if source_sub_type in ('R', 'D', 'P', 'M', 'J'):
                if sf_table is not None:
                    sf_data_source = sf_table
                else:
                    sf_data_source = "(" + sf_query + ")"
                where_conditions = []
                for filter in input_data_dict:
                    if filter['dataType'] == 'string' and filter['searchType'] in ('like', 'not like'):
                        filter['value'] = f"%{filter['value']}%"
                    if filter['dataType'] != 'number' and filter['searchType'] != 'predefined daterange':
                        filter['value'] = "'" + filter['value'] + "'"
                    if filter['searchType'] in ('exists in', 'not exists in') and filter['dataType'] == 'number':
                        filter['value'] = "(" + filter['value'] + ")"
                    elif filter['searchType'] in ('exists in', 'not exists in') and filter['dataType'] != 'number':
                        filter['value'] = "(" + filter['value'].replace(',', '\',\'') + ")"
                    if filter['searchType'] == 'between' and filter['dataType'] != 'number':
                        filter['value'] = filter['value'].replace(',', '\' and \'')
                    elif filter['searchType'] == 'between' and filter['dataType'] == 'number':
                        filter['value'] = filter['value'].replace(',', ' and ')
                    if filter['searchType'] == 'predefined daterange':
                        filter['value'] = f"current_date() - interval '{filter['value']} days'"
                    if 'touchCount' in filter:
                        if main_datasource_details['feedType'] == 'FirstParty':
                            grouping_fields = 'listid,email'
                        else:
                            grouping_fields = 'email'
                        grouping_clause = f'group by {grouping_fields}'
                        having_clause = f"having count(1) > {filter['touchCount']}"
                    where_conditions.append(
                        f" {filter['fieldName']} {operator_mapping[filter['searchType']]} {filter['value']} ")

                sf_cursor.execute(
                    f"create or replace transient table {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table} "
                    f"as select {main_datasource_details['FilterMatchFields']} from "
                    f"{sf_database}.{sf_schema}.{sf_data_source} {grouping_clause} where"
                    f" {' and '.join(where_conditions)} {having_clause}")
                return source_table
            else:
                consumer_logger.info("Unknown source_sub_type selected")
            raise Exception("Unknown source_sub_type selected")
        else:
            consumer_logger.info("Unknown source_type selected")
            raise Exception("Unknown source_type selected")

    except Exception as e:
        print(f"Exception occurred: Please look into this. {str(e)}")
        raise Exception(f"Exception occurred: Please look into this. {str(e)}")
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.close()
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def create_main_datasource(request_id, main_datasource_details, sources_loaded):
    try:
        datasource_id = main_datasource_details['id']
        channel_name = main_datasource_details['channelName']
        user_group_id = main_datasource_details['userGroupId']
        feed_type = main_datasource_details['feedType']
        data_processing_type = main_datasource_details['dataProcessingType']
        filter_match_fields = main_datasource_details['FilterMatchFields']
        isps = main_datasource_details['isps']

        if data_processing_type == 'K':
            sf_data_source = f"select * from {' intersect select * from '.join(sources_loaded)}"
        elif data_processing_type == 'M':
            sf_data_source = f"select * from {' union select * from '.join(sources_loaded)}"
        else:
            print(f"Unknown data_processing_type - {data_processing_type} . Raising Exception ... ")
            raise Exception(f"Unknown data_processing_type - {data_processing_type} . Raising Exception ... ")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        main_datasource_table = MAIN_DATASOURCE_TABLE_PREFIX + request_id
        temp_datasource_table = MAIN_DATASOURCE_TABLE_PREFIX + request_id + "_TEMP"
        sf_cursor.execute(f"create or replace transient table "
                          f"{SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{temp_datasource_table} as "
                          f"{sf_data_source} ")
        sf_cursor.execute(f"drop table {main_datasource_details}")
        sf_cursor.execute(f"alter table {temp_datasource_table} rename to {main_datasource_table}")
        sf_cursor.execute(f"select count(1) from {main_datasource_details}")
        record_count = sf_cursor.fetchone()[0]
        mysql_conn = mysql.connector.connect(**mysql_source_configs)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        mysql_cursor.execute(f"update SUPPRESSION_DATASOURCE_SCHEDULE_STATUS set status='COMPLETED' , "
                             f"runNumber=runNumber+1 ,recordCount={record_count} where dataSourceId={request_id} "
                             f"order by id desc limit 1")
    except Exception as e:
        print(f"Exception occurred while creating main_datasource. {str(e)} ")
        raise Exception(f"Exception occurred while creating main_datasource. {str(e)} ")
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.mclose()

















































































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






class LocalFileTransfer:
    def __init__(self, mount_path):
        self.mount_path = mount_path

    def list_files(self,mount_path):
        try:
            return os.listdir(mount_path)
        except FileNotFoundError:
            print(f"Directory '{mount_path}' not found.")
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



def process_sftp_ftp_nfs_request(sourcesubtype, hostname, port, username, password, inputData_dict, mysql_cursor, consumer_logger, id):
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
            raise Exception("Wrong method called. This method works only for SFTP/FTP/NFS requests only...")


        isFile = False if inputData_dict["filePath"].endswith("/") else True
        isDir = True if inputData_dict["filePath"].endswith("/") else False

        result = {}
        mysql_cursor.execute(run_number_query.replace("REQUEST_ID",id))
        run_number = int(mysql_cursor.fetchone()[0])
        last_successful_run_number = 0
        #mysql_cursor.execute(last_successful_run_number_query)


        last_iteration_files_details=[]
        if run_number != 1:
            mysql_cursor.execute(last_successfull_run_number_query)
            last_successful_run_number=int(mysql_cursor.fetchone()[0])
            mysql_cursor.execute(fetch_last_iteration_file_details_query)
            last_iteration_files_details = mysql_cursor.fetchall()
            #[filename,size,modified_time,count]


        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        if run_number == 1:
            header_list = inputData_dict['headerValue'].split(",")
            sf_create_table_query = f"create or replace transient table if not exists {SOURCE_TABLE_PREFIX + TABLE_NAME}  ( "
            sf_create_table_query += " varchar ,".join(i for i in header_list)
            sf_create_table_query += " varchar , filename varchar )"
        else:
            sf_create_table_query = f"create or replace transient table if not exists {SOURCE_TABLE_PREFIX + TABLE_NAME}  clone {SOURCE_TABLE_PREFIX + TABLE_NAME+str(last_successfull_run_number)} "
        consumer_logger.info(f"Executing query: {sf_create_table_query}")
        sf_cursor.execute(sf_create_table_query)


        if isFile:
            file_details_list = []
            file_details_dict = process_single_file(run_number, ftpObj, inputData_dict['filePath'], consumer_logger, inputData_dict)
            file_details_list.append(file_details_dict)

        elif isDir:

            consumer_logger.info("Given source is a path. List of files need to be considered.")
            files_list = ftpObj.list_files(inputData_dict["filePath"])
            to_delete=[]
            for file in last_iteration_files_details:
                if file['filename'] not in files_list:
                    to_delete.append(file['filename'])
            to_delete_mysql_formatted=','.join([f"'{item}'" for item in to_delete])

            print(sf_delete_old_details_query)
            sf_delete_old_details_query1 = sf_delete_old_details_query.replace("SOURCE_TABLE",SOURCE_TABLE_PREFIX + TABLE_NAME).replace("FILE",to_delete_mysql_formatted)
            sf_cursor.execute(sf_delete_old_details_query)
            # run delete query to remove old files

            file_details_list = []
            consumer_logger.info("First time dump processing all the files..")
            for file in files_list :
                    fully_qualified_file = inputData_dict["filePath"] + file
                    file_details_dict = process_single_file(ftpObj,fully_qualified_file)
                    file_details_list.append(file_details_dict)

        else:
            consumer_logger.info("Wrong Input...raising Exception..")

    except Exception as e:
        print(f"Except occurred. Please look into it. {str(e)}")
        consumer_logger.info(f"Except occurred. Please look into it. {str(e)}")
        raise Exception(str(e))


def process_single_file(run_number,ftpObj, fully_qualified_file, consumer_logger, inputData_dict,last_iteration_files_details=[]):
    try:
        file_details_dict = {}
        isOldFile = False
        consumer_logger.info("Processing file for the first time...")
        file = fully_qualified_file.split("/")[-1]
        meta_data = ftpObj.get_file_metadata(fully_qualified_file)  #metadata from ftp
        if run_number!=1:
            if file in [i['filename'] for i in last_iteration_files_details]:
                isOldFile=True
                file_index = [i['filename'] for i in last_iteration_files_details].index(file)
                if meta_data['size'] == last_iteration_files_details[file_index]['size'] and meta_data['last_modified'] == \
                    last_iteration_files_details[file_index]['last_modified_time']:
                    consumer_logger.info("File"+file+" already processed last time.. So skipping the file.")
                    file_details_dict=last_iteration_files_details[file_index]
                    return file_details_dict

        ftpObj.download_file(fully_qualified_file, file_path)
        line_count = sum(1 for _ in open(file_path + file, 'r'))
        file_details_dict["name"] = file
        file_details_dict["count"] = line_count
        file_details_dict["size"] = meta_data["size"]
        file_details_dict["last_modified_time"] = meta_data["last_modified"]

        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor=sf_conn.cursor()
        field_delimiter = inputData_dict["delimiter"]

        if inputData_dict['isHeaderExists'] == 0:
            consumer_logger.info("Header not available for file.. Creating stage according to it.")
            sf_create_stage_query = f" CREATE STAGE {stage_name} FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = '{field_delimiter}', FIELD_OPTIONALLY_ENCLOSED_BY = '\"' )  ; PUT file://{file_path}/{file} @{stage_name} ;"
        else:
            consumer_logger.info("Header available for file.. Creating stage according to it.")
            sf_create_stage_query = f" CREATE STAGE {stage_name} FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = '{field_delimiter}', FIELD_OPTIONALLY_ENCLOSED_BY = '\"' , SKIP_HEADER =1)  ; PUT file://{file_path}/{file} @{stage_name} ;"
        consumer_logger.info(f"Executing query: {sf_create_stage_query}")
        sf_cursor.execute(sf_create_stage_query)

        header_list = inputData_dict['headerValue'].split(",")
        stage_columns = ", ".join(f"${i + 1}" for i in range(len(header_list)))


        if isOldFile:
            sf_delete_old_details_query=f"delete from {SOURCE_TABLE_PREFIX + TABLE_NAME} where filename = '{file}'"
            sf_cursor.execute(sf_delete_old_details_query)
        sf_copy_into_query = f"copy into {SOURCE_TABLE_PREFIX + TABLE_NAME} FROM (select {stage_columns}, '{file}' FROM @stage_name ) "
        consumer_logger.info(f"Executing query: {sf_copy_into_query}")
        sf_cursor.execute(sf_copy_into_query)
        return file_details_dict
    except Exception as e:
        consumer_logger.error(f"Exception occurred. PLease look into this. {str(e)}")
        raise Exception(f"Exception occurred. PLease look into this. {str(e)}")



