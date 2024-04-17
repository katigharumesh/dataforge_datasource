
from serviceconfigurations import *


def load_data_source(source, main_datasource_details, consumer_logger):
    try:
        consumer_logger.info(f"Processing task: {str(source)}")
        data_source_mapping_id = source['id']
        data_source_id = source['dataSourceId']
        source_id = source['sourceId']
        input_data = source['inputData']
        sf_source_name = source['name']
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
        data_source_schedule_id = main_datasource_details['dataSourceScheduleId']
        run_number = main_datasource_details['runNumber']
        input_data_dict = json.loads(input_data.strip('"').replace("'", '"'))
        consumer_logger.info(f"Acquiring mysql connection...")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        consumer_logger.info("Mysql Connection established successfully...")
        consumer_logger.info(f"Acquiring snowflake connection...")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        consumer_logger.info("Snowflake connection established Successfully....")

        source_table = SOURCE_TABLE_PREFIX + str(data_source_id) + '_' + str(data_source_mapping_id) + '_' + str(run_number)
        if source_type == "F":
            if source_sub_type == "S":
                source_table = process_sftp_ftp_nfs_request(data_source_id, source_table, run_number,
                                                            data_source_schedule_id, source_sub_type, input_data_dict,
                                                            mysql_cursor, consumer_logger, data_source_mapping_id, hostname,
                                                            int(port), username, password)
                return source_table
            elif source_sub_type == "N":
                source_table = process_sftp_ftp_nfs_request(data_source_id, source_table, run_number,
                                                            data_source_schedule_id, source_sub_type,
                                                            inputData_dict=input_data_dict, mysql_cursor=mysql_cursor,
                                                            consumer_logger=consumer_logger,
                                                            request_id=data_source_mapping_id)
                return source_table
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
                    sf_data_source = f"{sf_database}.{sf_schema}.{sf_table}"
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

                    touch_filter = False
                    if 'touchCount' in filter:
                        touch_filter = True
                        touch_count = filter['touchCount']
                        if main_datasource_details['feedType'] == 'FirstParty':
                            grouping_fields = 'listid,email'
                            join_fields = 'a.listid=b.listid and a.email=b.email'
                        else:
                            grouping_fields = 'email'
                            join_fields = 'a.email=b.email'
                    where_conditions.append(
                        f" {filter['fieldName']} {OPERATOR_MAPPING[filter['searchType']]} {filter['value']} ")
                source_table_preparation_query = f"create or replace transient table " \
                                                 f"{SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table} " \
                                                 f"as select {main_datasource_details['FilterMatchFields']} " \
                                                 f"from {sf_data_source} where {' and '.join(where_conditions)} "
                print("Source table preparation query: " + source_table_preparation_query)
                sf_cursor.execute(source_table_preparation_query)
                if touch_filter:
                    sf_cursor.execute(
                        f"delete from {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table} a "
                        f"using (select {grouping_fields} from {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table}"
                        f" group by {grouping_fields} having count(1)< {touch_count}) b "
                        f"where {join_fields}")
                sf_cursor.execute(
                    f"select count(1) from {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table} ")
                records_count = sf_cursor.fetchone()[0]
                mysql_cursor.execute(DELETE_FILE_DETAILS, (data_source_schedule_id, run_number, data_source_mapping_id))
                mysql_cursor.execute(INSERT_FILE_DETAILS, (
                    data_source_schedule_id, run_number, data_source_mapping_id, records_count, sf_source_name,
                    'DF_DATASOURCE SERVICE', 'DF_DATASOURCE SERVICE', 'NA', 'NA'))
                return source_table
            else:
                consumer_logger.info("Unknown source_sub_type selected")
                raise Exception("Unknown source_sub_type selected")
        else:
            consumer_logger.info("Unknown source_type selected")
            raise Exception("Unknown source_type selected")

    except Exception as e:
        print(f"Exception occurred: Please look into this. {str(e)}" + str(traceback.format_exc()))
        raise Exception(f"Exception occurred: Please look into this. {str(e)}")
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.close()
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def create_main_datasource(sources_loaded, main_datasource_details):
    try:
        data_source_id = main_datasource_details['id']
        channel_name = main_datasource_details['channelName']
        user_group_id = main_datasource_details['userGroupId']
        feed_type = main_datasource_details['feedType']
        data_processing_type = main_datasource_details['dataProcessingType']
        filter_match_fields = main_datasource_details['FilterMatchFields']
        isps = main_datasource_details['isps']
        data_source_schedule_id = main_datasource_details['dataSourceScheduleId']
        run_number = main_datasource_details['runNumber']

        if data_processing_type == 'K':
            sf_data_source = f' intersect select {filter_match_fields} from '.join(sources_loaded)
        elif data_processing_type == 'M':
            sf_data_source = f' union select {filter_match_fields} from '.join(sources_loaded)
        else:
            print(f"Unknown data_processing_type - {data_processing_type} . Raising Exception ... ")
            raise Exception(f"Unknown data_processing_type - {data_processing_type} . Raising Exception ... ")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        main_datasource_table = MAIN_DATASOURCE_TABLE_PREFIX + str(data_source_id) + '_' + str(run_number)
        temp_datasource_table = MAIN_DATASOURCE_TABLE_PREFIX + str(data_source_id) + '_' + str(run_number) + "_TEMP"
        main_datasource_query = f"create or replace transient table {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{temp_datasource_table} as select {filter_match_fields} from {sf_data_source}"
        print(f"Main datasource preparation query: {main_datasource_query}")
        sf_cursor.execute(main_datasource_query)
        sf_cursor.execute(f"drop table if exists {main_datasource_table}")
        sf_cursor.execute(f"alter table {temp_datasource_table} rename to {main_datasource_table}")
        sf_cursor.execute(f"select count(1) from {main_datasource_table}")
        record_count = sf_cursor.fetchone()[0]
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        mysql_cursor.execute(f"update {SCHEDULE_STATUS_TABLE} set status='C', recordCount={record_count}"
                             f" where dataSourceScheduleId={data_source_schedule_id} and runNumber={run_number}")
    except Exception as e:
        print(f"Exception occurred while creating main_datasource. {str(e)} " + str(traceback.format_exc()))
        raise Exception(f"Exception occurred while creating main_datasource. {str(e)} ")
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.mclose()
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()

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

    def list_files(self, mount_path):
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
            #full_path = os.path.join(self.mount_path, file_path)
            file_stat = os.stat(file_path)
            metadata = {
                'size': file_stat.st_size,
                'last_modified': file_stat.st_mtime
            }
            return metadata
        except FileNotFoundError:
            print(f"File '{file_path}' not found.")
            return None


def process_sftp_ftp_nfs_request(data_source_id, source_table, run_number, data_source_schedule_id, sourcesubtype, inputData_dict, mysql_cursor,
                                 consumer_logger, request_id,  hostname = None, port = None, username = None, password = None):
    try:
        if sourcesubtype == "S":
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
        #consumer_logger.info(f"Fetching runNumber from table... ")
        #consumer_logger.info(f"Executing query: {RUN_NUMBER_QUERY.replace('REQUEST_ID', str(request_id))}")
        #mysql_cursor.execute(RUN_NUMBER_QUERY.replace('REQUEST_ID', str(request_id)))

        last_successful_run_number = 0
        # mysql_cursor.execute(last_successful_run_number_query)

        last_iteration_files_details = []
        if run_number != 0:
            mysql_cursor.execute(LAST_SUCCESSFUL_RUN_NUMBER_QUERY.replace('REQUEST_ID',str(data_source_id)))
            last_successful_run_number = int(mysql_cursor.fetchone()['runNumber'])
            mysql_cursor.execute(FETCH_LAST_ITERATION_FILE_DETAILS_QUERY.replace('ID',str(request_id)).replace('RUNNUMBER',str(run_number)))
            last_iteration_files_details = mysql_cursor.fetchall()
            # [filename,size,modified_time,count]
        table_name = source_table
        consumer_logger.info(f"Table name for this DataSource is : {table_name}")
        consumer_logger.info(f"Establishing Snowflake connection...")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        consumer_logger.info("Snowflake connection acquired successfully")
        if run_number == 0:
            field_delimiter = inputData_dict['delimiter']
            header_list = inputData_dict['headerValue'].split(str(field_delimiter))
            sf_create_table_query = f"create or replace transient table  {table_name}  ( "
            sf_create_table_query += " varchar ,".join(i for i in header_list)
            sf_create_table_query += " varchar , filename varchar )"
        else:
            last_run_table_name = table_name[:-1]+str(last_successful_run_number)
            sf_create_table_query = f"create or replace transient table if not exists {table_name}  clone {last_run_table_name} "
        consumer_logger.info(f"Executing query: {sf_create_table_query}")
        sf_cursor.execute(sf_create_table_query)

        if isFile:
            file_details_list = []
            file_details_dict = process_single_file(run_number, ftpObj, inputData_dict['filePath'], consumer_logger,
                                                    inputData_dict, table_name)
            # add logic to insert the file details into table
            fileName = file_details_dict["name"]
            count = file_details_dict["count"]
            size = file_details_dict["size"]
            last_modified_time = file_details_dict["last_modified_time"]
            insert_file_details_query = f"insert into SUPPRESSION_DATASOURCE_FILE_DETAILS (fileName,count,size,last_modified_time,runNumber,dataSourceMappingId,dataSourceScheduleId) values ('{fileName}','{count}','{size}','{last_modified_time}',{run_number},'{request_id}','{data_source_schedule_id}') "
            mysql_cursor.execute(insert_file_details_query)
            file_details_list.append(file_details_dict)

        elif isDir:

            consumer_logger.info("Given source is a path. List of files need to be considered.")
            files_list = ftpObj.list_files(inputData_dict["filePath"])
            to_delete = []
            for file in last_iteration_files_details:
                if file['filename'] not in files_list:
                    to_delete.append(file['filename'])
            to_delete_mysql_formatted = ','.join([f"'{item}'" for item in to_delete])

            print(SF_DELETE_OLD_DETAILS_QUERY)
            SF_DELETE_OLD_DETAILS_QUERY1 = SF_DELETE_OLD_DETAILS_QUERY.replace("SOURCE_TABLE", table_name).replace(
                "FILE", to_delete_mysql_formatted)
            sf_cursor.execute(SF_DELETE_OLD_DETAILS_QUERY1)
            # run delete query to remove old files

            file_details_list = []
            consumer_logger.info("First time dump processing all the files..")
            for file in files_list:
                fully_qualified_file = inputData_dict["filePath"] + file
                file_details_dict = process_single_file(ftpObj, fully_qualified_file, consumer_logger, table_name,
                                                        last_iteration_files_details)
                fileName = file_details_dict["name"]
                count = file_details_dict["count"]
                size = file_details_dict["size"]
                last_modified_time = file_details_dict["last_modified_time"]
                insert_file_details_query = f"insert into SUPPRESSION_DATASOURCE_FILE_DETAILS (fileName,count,size,last_modified_time,runNumber,dataSourceMappingId,dataSourceScheduleId) values ('{fileName}','{count}','{size}','{last_modified_time}',{run_number},'{request_id}','{data_source_schedule_id}')"
                mysql_cursor.execute(insert_file_details_query)
                file_details_list.append(file_details_dict)

        else:
            consumer_logger.info("Wrong Input...raising Exception..")
            raise Exception("Wrong Input...raising Exception..")
        return table_name
    except Exception as e:
        print(f"Except occurred. Please look into it. {str(e)}")
        consumer_logger.info(f"Except occurred. Please look into it. {str(e)}")
        raise Exception(str(e))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def process_single_file(run_number, ftpObj, fully_qualified_file, consumer_logger, inputData_dict,
                        table_name, last_iteration_files_details=[]):
    try:
        file_details_dict = {}
        isOldFile = False
        consumer_logger.info("Processing file for the first time...")
        file = fully_qualified_file.split("/")[-1]
        meta_data = ftpObj.get_file_metadata(fully_qualified_file)  # metadata from ftp
        consumer_logger.info(f"Meta data fetched successfully : {meta_data}")
        if run_number != 0:
            if file in [i['filename'] for i in last_iteration_files_details]:
                isOldFile = True
                file_index = [i['filename'] for i in last_iteration_files_details].index(file)
                if meta_data['size'] == last_iteration_files_details[file_index]['size'] and meta_data[
                    'last_modified'] == \
                        last_iteration_files_details[file_index]['last_modified_time']:
                    consumer_logger.info("File" + file + " already processed last time.. So skipping the file.")
                    file_details_dict = last_iteration_files_details[file_index]
                    return file_details_dict

        ftpObj.download_file(fully_qualified_file, FILE_PATH + file)
        line_count = sum(1 for _ in open(FILE_PATH + file, 'r'))
        file_details_dict["name"] = file
        file_details_dict["count"] = line_count
        file_details_dict["size"] = meta_data["size"]
        file_details_dict["last_modified_time"] = meta_data["last_modified"]

        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        field_delimiter = inputData_dict["delimiter"]
        stage_name = "STAGE_" + table_name
        if inputData_dict['isHeaderExists'] == 'true':
            consumer_logger.info("Header not available for file.. Creating stage according to it.")
            sf_create_stage_query = f" CREATE OR REPLACE  STAGE {stage_name} FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = '{field_delimiter}', FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ) "
        else:
            consumer_logger.info("Header available for file.. Creating stage according to it.")
            sf_create_stage_query = f" CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = '{field_delimiter}', FIELD_OPTIONALLY_ENCLOSED_BY = '\"' , SKIP_HEADER =1) "
        consumer_logger.info(f"Executing query: {sf_create_stage_query}")
        sf_cursor.execute(sf_create_stage_query)
        sf_put_file_stage_query = f" PUT file://{FILE_PATH}/{file} @{stage_name} "
        consumer_logger.info(f"Executing query: {sf_put_file_stage_query}")
        sf_cursor.execute(sf_put_file_stage_query)
        field_delimiter = inputData_dict['delimiter']
        header_list = inputData_dict['headerValue'].split(str(field_delimiter))
        stage_columns = ", ".join(f"${i + 1}" for i in range(len(header_list)))

        if isOldFile:
            sf_delete_old_details_query = f"delete from {table_name} where filename = '{file}'"
            sf_cursor.execute(sf_delete_old_details_query)
        sf_copy_into_query = f"copy into {table_name} FROM (select {stage_columns}, '{file}' FROM @{stage_name} ) "
        consumer_logger.info(f"Executing query: {sf_copy_into_query}")
        sf_cursor.execute(sf_copy_into_query)
        return file_details_dict
    except Exception as e:
        consumer_logger.error(f"Exception occurred. PLease look into this. {str(e)}")
        raise Exception(f"Exception occurred. PLease look into this. {str(e)}")

