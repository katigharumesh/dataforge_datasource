import snowflake.connector.config_manager

from serviceconfigurations import *
from basicudfs import *

def load_input_source(type_of_request, source, main_request_details):
    try:
        if type_of_request == "SUPPRESSION_REQUEST":
            request_id = source['requestId']
        elif type_of_request == "SUPPRESSION_DATASET":
            request_id = source['dataSourceId']
        mapping_id = source['id']
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
        schedule_id = main_request_details['ScheduleId']
        run_number = main_request_details['runNumber']
        input_data_dict = json.loads(input_data.strip('"').replace("'", '"'))
        consumer_logger = create_logger(base_logger_name=f"source_{str(mapping_id)}_{str(request_id)}_{str(run_number)}", log_file_path=f"{LOG_PATH}/{str(request_id)}/{str(run_number)}/", log_to_stdout=True)
        consumer_logger.info(f"Processing task: {str(source)}")
        consumer_logger.info(f"Acquiring mysql connection...")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        consumer_logger.info("Mysql Connection established successfully...")
        if source_id == "0" and data_source_id != "" :
            return tuple(data_source_input("Suppression Request Input Source", data_source_id, mysql_cursor, consumer_logger),mapping_id)
        consumer_logger.info(f"Acquiring snowflake connection...")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        consumer_logger.info("Snowflake connection established Successfully....")

        source_table = SOURCE_TABLE_PREFIX + str(request_id) + '_' + str(mapping_id) + '_' + str(run_number)
        if source_type == "F":
            temp_files_path = f"{FILE_PATH}/{str(request_id)}/{str(run_number)}/{str(mapping_id)}/"
            os.makedirs(temp_files_path,exist_ok=True)
            source_table = process_file_type_request(request_id, source_table, run_number,
                                                            schedule_id, source_sub_type, input_data_dict,
                                                            mysql_cursor, consumer_logger, mapping_id, temp_files_path, hostname,
                                                            port, username, password)
            return tuple(source_table,mapping_id)

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
                    if filter['dataType'] != 'number' and filter['searchType'] != '>=':
                        filter['value'] = "'" + filter['value'] + "'"
                    if filter['searchType'] in ('in', 'not in') and filter['dataType'] == 'number':
                        filter['value'] = "(" + filter['value'] + ")"
                    elif filter['searchType'] in ('in', 'not in') and filter['dataType'] != 'number':
                        filter['value'] = "(" + filter['value'].replace(',', '\',\'') + ")"
                    if filter['searchType'] == 'between' and filter['dataType'] != 'number':
                        filter['value'] = filter['value'].replace(',', '\' and \'')
                    elif filter['searchType'] == 'between' and filter['dataType'] == 'number':
                        filter['value'] = filter['value'].replace(',', ' and ')
                    if filter['searchType'] == '>=':
                        filter['value'] = f"current_date() - interval '{filter['value']} days'"

                    touch_filter = False
                    if 'touchCount' in filter:
                        touch_filter = True
                        touch_count = filter['touchCount']
                        if main_request_details['feedType'] == 'F':
                            grouping_fields = 'list_id,email_id'
                            join_fields = 'a.list_id=b.list_id and a.email_id=b.email_id'
                        else:
                            grouping_fields = 'email_id'
                            join_fields = 'a.email_id=b.email_id'
                    where_conditions.append(
                        f" {filter['fieldName']} {filter['searchType']} {filter['value']} ")
                source_table_preparation_query = f"create or replace transient table " \
                                                 f"{SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table} " \
                                                 f"as select {main_request_details['FilterMatchFields']} " \
                                                 f"from {sf_data_source} where {' and '.join(where_conditions)} "
                print("Source table preparation query: " + source_table_preparation_query)
                sf_cursor.execute(source_table_preparation_query)
                if touch_filter:
                    sf_cursor.execute(
                        f"delete from {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table} a "
                        f"using (select {grouping_fields} from {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table}"
                        f" group by {grouping_fields} having count(1)< {touch_count}) b "
                        f"where {join_fields}")
                sf_cursor.execute(f"alter table {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table}"
                                  f" add column do_inputSource varchar as '{sf_source_name}', add column do_inputSourceMappingId varchar as '{mapping_id}'")
                sf_cursor.execute(
                    f"select count(1) from {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table} ")
                records_count = sf_cursor.fetchone()[0]
                #mysql_cursor.execute(DELETE_FILE_DETAILS, (schedule_id, run_number, mapping_id))
                mysql_cursor.execute(INSERT_FILE_DETAILS, (
                    schedule_id, run_number, mapping_id, records_count, sf_source_name,
                    'DF_DATASET SERVICE', 'DF_DATASET SERVICE', 'NA', 'NA','C',''))
                return tuple(source_table,mapping_id)
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


def create_main_datasource(sources_loaded, main_request_details):
    try:
        data_source_id = main_request_details['id']
        data_source_name = main_request_details['name']
        channel_name = main_request_details['channelName']
        user_group_id = main_request_details['userGroupId']
        feed_type = main_request_details['feedType']
        data_processing_type = main_request_details['dataProcessingType']
        filter_match_fields = main_request_details['FilterMatchFields']
        isps = main_request_details['isps']
        schedule_id = main_request_details['ScheduleId']
        run_number = main_request_details['runNumber']

        if data_processing_type == 'K':
            sf_data_source = f' intersect select {filter_match_fields} from '.join(sources_loaded)
        elif data_processing_type == 'M':
            sf_data_source = f' union select {filter_match_fields} from '.join(sources_loaded)
        else:
            print(f"Unknown data_processing_type - {data_processing_type} . Raising Exception ... ")
            raise Exception(f"Unknown data_processing_type - {data_processing_type} . Raising Exception ... ")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        main_datasource_table = MAIN_DATASET_TABLE_PREFIX + str(data_source_id) + '_' + str(run_number)
        temp_datasource_table = MAIN_DATASET_TABLE_PREFIX + str(data_source_id) + '_' + str(run_number) + "_TEMP"
        main_datasource_query = f"create or replace transient table {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{temp_datasource_table} as select {filter_match_fields} from {sf_data_source}"
        print(f"Main datasource preparation query: {main_datasource_query}")
        sf_cursor.execute(main_datasource_query)
        if 'email_id' in str(filter_match_fields).lower().split(','):
            sf_cursor.execute(f"update {temp_datasource_table} set email_id=lower(trim(email_id))")
            isps_filter = str(isps).replace(",","','")
            sf_cursor.execute(f"delete from {temp_datasource_table} where split_part(email_id,'@',-1) not in ('{isps_filter}')")
            if 'email_md5' not in str(filter_match_fields).lower().split(','):
                sf_cursor.execute(f"alter table {temp_datasource_table} add column email_md5 varchar as md5(email_id)")
        sf_cursor.execute(f"alter table {temp_datasource_table} add column do_inputSource varchar as '{data_source_name}'")
        sf_cursor.execute(f"drop table if exists {main_datasource_table}")
        sf_cursor.execute(f"alter table {temp_datasource_table} rename to {main_datasource_table}")
        sf_cursor.execute(f"select count(1) from {main_datasource_table}")
        record_count = sf_cursor.fetchone()[0]
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        mysql_cursor.execute(UPDATE_SCHEDULE_STATUS,('C', record_count, '', data_source_id, run_number))
    except Exception as e:
        print(f"Exception occurred while creating main_datasource. {str(e)} " + str(traceback.format_exc()))
        raise Exception(f"Exception occurred while creating main_datasource. {str(e)} ")
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.close()
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
            if requires_conversion(local_file):
                perform_conversion(local_file)
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



def requires_conversion(filename):
    """Check if dos2unix conversion is required."""
    with open(filename, 'rb') as f:
        for line in f:
            if b'\r\n' in line:
                return True
    return False


def perform_conversion(filename):
    """Perform dos2unix conversion."""
    # Check if the file is gzipped
    if filename.endswith('.gz'):
        temp_file = filename + '.tmp'
        with gzip.open(filename, 'rt') as f_in, open(temp_file, 'w') as f_out:
            for line in f_in:
                f_out.write(line.replace('\r\n', '\n'))
        os.rename(temp_file, filename)
    else:
        # For regular text files
        with fileinput.FileInput(filename, inplace=True) as f:
            for line in f:
                print(line.replace('\r\n', '\n'), end='')


def validate_header(file, header, delimiter):
    with open(file, 'r') as f:
        first_line = f.readline()
    if len(first_line.split(delimiter)) == len(header.split(delimiter)):
        return True
    return False

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
            if requires_conversion(local_file):
                perform_conversion(local_file)
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

class ProcessS3Files:
    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key
        self.s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    def list_files(self, file_path):
        try:
            bucket_name = file_path.split('/')[2]
            prefix = '/'.join(file_path.split('/')[3:])
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
            s3_files = [str(obj['Key']).split('/')[-1] for obj in response.get('Contents', [])]
            return s3_files
        except Exception as e:
            print(f"Error occurred while listing files in s3 path: {e}")
            return []

    def get_file_metadata(self, file):
        try:
            bucket_name = file.split('/')[2]
            key = '/'.join(file.split('/')[3:])
            response = self.s3_client.head_object(Bucket=bucket_name, Key=key)
            metadata = {
                'size': str(response['ContentLength']),
                'last_modified': str(response['LastModified'])
            }
            return metadata
        except Exception as e:
            print(f"Error occurred while fetching metadata for {file} file. Error: {e}")
            return None

    def header_validation(self, file, header_value, delimiter):
        try:
            bucket_name = file.split('/')[2]
            key = '/'.join(file.split('/')[3:])
            obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            first_line = obj['Body'].readline().decode().strip()
            if len(str(first_line).split(delimiter)) == len(header_value.split(delimiter)):
                return True
            else:
                return False
        except Exception as e:
            print(f"Error occurred during header validation for {file} file. Error: {e}")


def process_file_type_request(data_source_id, source_table, run_number, schedule_id, source_sub_type, input_data_dict, mysql_cursor,
                                 consumer_logger, mapping_id, temp_files_path, hostname = None, port = None, username = None, password = None):
    try:
        if source_sub_type == "S":
            consumer_logger.info("Request initiated to process.. File source: SFTP/FTP ")
            consumer_logger.info("Getting SFTP/FTP connection...")
            source_obj = FileTransfer(hostname, int(port), username, password)
            source_obj.connect()
            consumer_logger.info("SFTP/FTP connection established successfully.")
        elif source_sub_type in ("N", "D"):
            consumer_logger.info("Request initiated to process.. File source: NFS/DESKTOP ")
            source_obj = LocalFileTransfer(input_data_dict["filePath"])
        elif source_sub_type == "A":
            consumer_logger.info("Request initiated to process.. File source: AWS ")
            source_obj = ProcessS3Files(username, password)
        else:
            consumer_logger.info("Wrong method called.. ")
            raise Exception("Wrong method called. This method works only for SFTP/FTP/NFS requests only...")

        isFile = False if input_data_dict["filePath"].endswith("/") else True
        isDir = True if input_data_dict["filePath"].endswith("/") else False

        result = {}
        #consumer_logger.info(f"Fetching runNumber from table... ")
        #consumer_logger.info(f"Executing query: {RUN_NUMBER_QUERY.replace('REQUEST_ID', str(mapping_id))}")
        #mysql_cursor.execute(RUN_NUMBER_QUERY.replace('REQUEST_ID', str(mapping_id)))

        last_successful_run_number = 0
        # mysql_cursor.execute(last_successful_run_number_query)

        last_iteration_files_details = []
        if run_number != 0:
            mysql_cursor.execute(LAST_SUCCESSFUL_RUN_NUMBER_QUERY, (str(data_source_id)))
            last_successful_run_number = int(mysql_cursor.fetchone()['runNumber'])
            mysql_cursor.execute(FETCH_LAST_ITERATION_FILE_DETAILS_QUERY, (str(mapping_id), str(last_successful_run_number)))
            last_iteration_files_details = mysql_cursor.fetchall()
            consumer_logger.info(f"Fetched last iteration_details: {last_iteration_files_details}")
            # [filename,size,modified_time,count]
        table_name = source_table
        consumer_logger.info(f"Table name for this DataSource is: {table_name}")
        consumer_logger.info(f"Establishing Snowflake connection...")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        consumer_logger.info("Snowflake connection acquired successfully")
        if run_number == 0:
            field_delimiter = input_data_dict['delimiter']
            header_list = input_data_dict['headerValue'].split(str(field_delimiter))
            sf_create_table_query = f"create or replace transient table  {table_name}  ( "
            sf_create_table_query += " varchar ,".join(i for i in header_list)
            sf_create_table_query += " varchar , do_inputSource varchar, do_inputSourceMappingId varchar as '{mapping_id}' )"
        else:
            last_run_table_name = table_name[:table_name.rindex('_')+1]+str(last_successful_run_number)
            sf_create_table_query = f"create or replace transient table  {table_name}  clone {last_run_table_name} "
        consumer_logger.info(f"Executing query: {sf_create_table_query}")
        sf_cursor.execute(sf_create_table_query)

        if isFile:
            file_details_list = []
            files_list = input_data_dict['filePath'].split(",")
            consumer_logger.info("File List: "+str(files_list))
            if len(files_list) >= 1 :
                consumer_logger.info("There are many files with comma separated...")
                for file in files_list:
                    file_details_dict = process_single_file(temp_files_path,  run_number , source_obj, file,consumer_logger,input_data_dict, table_name, last_iteration_files_details, source_sub_type, username, password)
                    # add logic to insert the file details into table
                    fileName = file_details_dict["filename"]
                    count = file_details_dict["count"]
                    size = file_details_dict["size"]
                    last_modified_time = file_details_dict["last_modified_time"]
                    file_status = file_details_dict['status']
                    error_desc = file_details_dict['error_msg']
                    mysql_cursor.execute(INSERT_FILE_DETAILS, (schedule_id, run_number, mapping_id, count, fileName, 'DF_DATASET SERVICE', 'DF_DATASET SERVICE', size, last_modified_time, file_status , error_desc))
                    file_details_list.append(file_details_dict)
            else:
                consumer_logger.info("There are no files specified.. Kindly check the request..")
                raise Exception("There are no files specified.. Kindly check the request..")
        elif isDir:
            consumer_logger.info("Given source is a path. List of files need to be considered.")
            files_list = source_obj.list_files(input_data_dict["filePath"])
            consumer_logger.info(f"Fetched files from the path : {files_list}")
            to_delete = []
            for file in last_iteration_files_details:
                if file['filename'] not in files_list:
                    to_delete.append(file['filename'])
            if len(to_delete) != 0:
                to_delete_mysql_formatted = ','.join([f"'{item}'" for item in to_delete])
                print(f"Older files to be deleted: {to_delete_mysql_formatted}")
                sf_cursor.execute(SF_DELETE_OLD_DETAILS_QUERY,(table_name, to_delete_mysql_formatted))
            else:
                print("No older files to delete.")

            file_details_list = []
            consumer_logger.info("First time/existing files processing..")
            for file in files_list:
                fully_qualified_file = input_data_dict["filePath"] + file
                file_details_dict = process_single_file(temp_files_path, run_number, source_obj, fully_qualified_file, consumer_logger, input_data_dict, table_name,
                                                        last_iteration_files_details, source_sub_type, username, password)
                fileName = file_details_dict["filename"]
                count = file_details_dict["count"]
                size = file_details_dict["size"]
                last_modified_time = file_details_dict["last_modified_time"]
                file_status = file_details_dict['status']
                error_desc = file_details_dict['error_msg']
                mysql_cursor.execute(INSERT_FILE_DETAILS, (
                    schedule_id, run_number, mapping_id, count, fileName,
                    'DF_DATASET SERVICE', 'DF_DATASET SERVICE', size, last_modified_time,file_status , error_desc))
                file_details_list.append(file_details_dict)

        else:
            consumer_logger.info("Wrong Input...raising Exception..")
            raise Exception("Wrong Input...raising Exception..")
        return table_name
    except Exception as e:
        print(f"Except occurred. Please look into it. {str(e)} {str(traceback.format_exc())}")
        consumer_logger.info(f"Except occurred. Please look into it. {str(e)} {str(traceback.format_exc())}")
        raise Exception(str(e))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def process_single_file(temp_files_path, run_number, source_obj, fully_qualified_file, consumer_logger, input_data_dict,
                        table_name, last_iteration_files_details, source_sub_type, username = None, password = None):
    try:
        file_details_dict = {}
        is_old_file = False
        consumer_logger.info("Processing file for the first time...")
        file = fully_qualified_file.split("/")[-1]
        if file.split(".")[-1] == file or file.split(".")[-1] == "csv" or file.split(".")[-1] == "txt" or file.split(".")[-1] == "gz":
            consumer_logger.info("The given file is in required extension...")
        else:
            consumer_logger.info("The given file is not in required extension. ")
            file_details_dict['filename'] = file
            file_details_dict['status'] = 'E'
            file_details_dict['error_msg'] = 'The given file is not in required extension.'
            file_details_dict["count"] = '0'
            file_details_dict["size"] = 'NA'
            last_modified_time = file_details_dict["last_modified_time"] = 'NA'
            return file_details_dict
        meta_data = source_obj.get_file_metadata(fully_qualified_file)  # metadata from ftp
        consumer_logger.info(f"Meta data fetched successfully for file:{file} Meta data: {meta_data}")
        if run_number != 0:
            last_iteration_file_names_list = [i["filename"] for i in last_iteration_files_details]
            consumer_logger.info(f"last iteration files are {str(last_iteration_file_names_list)}")
            if file in last_iteration_file_names_list:
                is_old_file = True
                consumer_logger.info("Found filename in last_iteration_file details. Checking for metadata..")
                file_index = last_iteration_file_names_list.index(file)
                if str(meta_data['size']) == last_iteration_files_details[file_index]['size'] and str(meta_data['last_modified']) == last_iteration_files_details[file_index]['last_modified_time']:
                    consumer_logger.info("File " + file + " already processed last time.. So skipping the file.")
                    file_details_dict = last_iteration_files_details[file_index]
                    return file_details_dict
        if source_sub_type != 'A':
            source_obj.download_file(fully_qualified_file, temp_files_path + file)
            line_count = sum(1 for _ in open(temp_files_path + file, 'r'))
            file_details_dict["count"] = line_count
        file_details_dict["filename"] = file
        file_details_dict["size"] = meta_data["size"]
        file_details_dict["last_modified_time"] = meta_data["last_modified"]
        if source_sub_type != 'A':
            if not validate_header(temp_files_path + file , input_data_dict['headerValue'], input_data_dict['delimiter']):
                file_details_dict["count"] = 0
                file_details_dict['status'] = 'E'
                file_details_dict['error_msg'] = 'The header is not matching with the given header. Skipping the file.'
                consumer_logger.info('The header is not matching with the given header. Skipping the file.')
                return file_details_dict
        else:
            if not source_obj.header_validation(fully_qualified_file, input_data_dict['headerValue'], input_data_dict['delimiter']):
                file_details_dict["count"] = 0
                file_details_dict['status'] = 'E'
                file_details_dict['error_msg'] = 'The header is not matching with the given header. Skipping the file.'
                consumer_logger.info('The header is not matching with the given header. Skipping the file.')
                return file_details_dict

        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        field_delimiter = input_data_dict["delimiter"]
        #print(input_data_dict['isHeaderExists'] ,type(input_data_dict['isHeaderExists']))
        if input_data_dict['isHeaderExists']:
            header_exists = ", SKIP_HEADER = 1"
        else:
            header_exists = ""
        if file.split(".")[-1] == "gz":
            compression = " , COMPRESSION = GZIP"
        else:
            compression = ""

        if is_old_file:
            sf_delete_old_details_query = f"delete from {table_name} where do_inputSource = '{file}'"
            sf_cursor.execute(sf_delete_old_details_query)
        if source_sub_type != 'A':
            stage_name = "STAGE_" + table_name
            sf_create_stage_query = f" CREATE OR REPLACE TEMPORARY  STAGE {stage_name} "
            file_format = f"FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = '{field_delimiter}', FIELD_OPTIONALLY_ENCLOSED_BY = '\"'  "

            sf_create_stage_query = sf_create_stage_query + file_format + header_exists + compression + ")"
            consumer_logger.info(f"Executing query: {sf_create_stage_query}")
            sf_cursor.execute(sf_create_stage_query)
            sf_put_file_stage_query = f" PUT file://{temp_files_path}/{file} @{stage_name} "
            consumer_logger.info(f"Executing query: {sf_put_file_stage_query}")
            sf_cursor.execute(sf_put_file_stage_query)
            field_delimiter = input_data_dict['delimiter']
            header_list = input_data_dict['headerValue'].split(str(field_delimiter))
            stage_columns = ", ".join(f"${i + 1}" for i in range(len(header_list)))
            sf_copy_into_query = f"copy into {table_name} FROM (select {stage_columns}, '{file}' FROM @{stage_name} ) "
            consumer_logger.info(f"Executing query: {sf_copy_into_query}")
            sf_cursor.execute(sf_copy_into_query)
            file_details_dict['status'] = 'C'
            file_details_dict['error_msg'] = ''
        else:
            sf_copy_into_query = f"copy into {table_name} FROM {fully_qualified_file} CREDENTIALS=(AWS_KEY_ID='{username}'" \
                                 f" AWS_SECRET_KEY='{password}') FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '{field_delimiter}' " \
                                 f"FIELD_OPTIONALLY_ENCLOSED_BY='\"' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE {header_exists} {compression})"
            consumer_logger.info(f"Executing query: {sf_copy_into_query}")
            sf_cursor.execute(sf_copy_into_query)
            sf_update_query = f"update {table_name} set do_inputSource = '{file}' where do_inputSource is null"
            consumer_logger.info(f"Executing query: {sf_update_query}")
            sf_cursor.execute(sf_update_query)
            file_details_dict["count"] = sf_cursor.rowcount
            file_details_dict['status'] = 'C'
            file_details_dict['error_msg'] = ''
        return file_details_dict
    except Exception as e:
        consumer_logger.error(f"Exception occurred. PLease look into this. {str(e)}")
        raise Exception(f"Exception occurred. PLease look into this. {str(e)}")

def update_next_schedule_due(request_id, run_number, logger, request_status='E'):
    try:
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysqlcur = mysql_conn.cursor()
        mysqlcur.execute("set time_zone='UTC';")
        requestquery = f"select id,datasourceId,runnumber,recurrenceType,startDate,endDate,excludeDates," \
                       f"date(nextscheduleDue) as nextscheduledate from {SCHEDULE_TABLE} where status='I' " \
                       f"and nextScheduleDue<now() and datasourceId={request_id} and runnumber={run_number} "
        logger.info(f"Pulling schedule details for updation of nextScheduleDue, Query ::{requestquery}")
        mysqlcur.execute(requestquery)
        requestList = mysqlcur.fetchall()
        print(requestList)
        for request in requestList:
            recurrenceType = request[3]
            id = request[0]
            startDate = request[4]
            endDate = str(request[5])
            if request[6] is not None:
                try:
                    excludeDates = request[6].split(',')
                except:
                    excludeDates = request[6].split()

            scheduleNextquery = f"update {SCHEDULE_TABLE} set status='W',runnumber=runnumber+1 where id={id}"
            logger.info(f"Updating schedule status and runnumber, query :: {scheduleNextquery}")
            mysqlcur.execute(scheduleNextquery)

            if (recurrenceType is not None and recurrenceType == 'H'):
                nextschedulequery = f"update {SCHEDULE_TABLE} set nextScheduleDue=" \
                                    f"case when date_add(now(),INTERVAL 1 HOUR) < '{endDate}' Then date_add(now(),INTERVAL 1 HOUR)" \
                                    f"else '{endDate}' end where id={id}"
                logger.info(f"Updating nextScheduleDue, query : {nextschedulequery}")
                mysqlcur.execute(nextschedulequery)
            if (recurrenceType is not None and recurrenceType == 'D'):
                if excludeDates is not None:
                    timestamp = str(datetime.now()).split(' ')[1]
                    # print(timestamp)
                    # print(excludeDates)
                    nextscheduledatep = datetime.now().date() + timedelta(days=1)
                    while str(nextscheduledatep) in excludeDates:
                        nextscheduledatep += timedelta(days=1)
                    # print(nextscheduledatep)
                    nextscheduleDuep = str(nextscheduledatep) + ' ' + timestamp
                    # print(nextscheduleDuep)
                else:
                    nextscheduleDuep = datetime.now() + timedelta(days=1)
                    nextscheduledatep = datetime.now().date() + timedelta(days=1)
                nextschedulequery = f"update {SCHEDULE_TABLE} set nextScheduleDue=" \
                                    f"if('%s'<'%s','%s','%s') where id={id}"
                logger.info(f"Updating nextScheduleDue, query : {nextschedulequery , (str(nextscheduledatep), endDate, nextscheduleDuep, endDate)}")
                mysqlcur.execute(nextschedulequery, (str(nextscheduledatep), endDate, nextscheduleDuep, endDate))

            if (recurrenceType is not None and recurrenceType == 'W'):
                if excludeDates is not None:
                    timestamp = str(datetime.now()).split(' ')[1]
                    nextscheduledate = datetime.now().date() + timedelta(days=7)
                    while nextscheduledate in excludeDates:
                        nextscheduledate += timedelta(days=7)

                    nextscheduleDuep = str(nextscheduledate) + ' ' + timestamp
                else:
                    nextscheduleDuep = datetime.now() + timedelta(days=7)

                nextschedulequery = f"update {SCHEDULE_TABLE} set nextScheduleDue=" \
                                    f"if('%s'<'%s','%s','%s') where id={id}"
                # logger.info(nextschedulequery)
                logger.info(f"Updating nextScheduleDue, query : {nextschedulequery, (str(nextscheduledatep), endDate, nextscheduleDuep, endDate)}")
                mysqlcur.execute(nextschedulequery, (str(nextscheduledatep), endDate, nextscheduleDuep, endDate))

            if (recurrenceType is not None and recurrenceType == 'M'):
                if excludeDates is not None:
                    timestamp = str(datetime.now()).split(' ')[1]
                    nextscheduledate = datetime.now().date() + timedelta(months=1)
                    while nextscheduledate in excludeDates:
                        nextscheduledate += timedelta(months=1)

                    nextscheduleDuep = str(nextscheduledate) + ' ' + timestamp
                else:
                    nextscheduleDuep = datetime.now() + timedelta(months=1)
                nextschedulequery = f"update {SCHEDULE_TABLE} set nextScheduleDue=" \
                                    f"if('%s'<'%s','%s','%s') where id={id}"
                logger.info(f"Updating nextScheduleDue, query : {nextschedulequery, (str(nextscheduledatep), endDate, nextscheduleDuep, endDate)}")
                mysqlcur.execute(nextschedulequery, (str(nextscheduledatep), endDate, nextscheduleDuep, endDate))

            if recurrenceType is None and request_status == 'C':
                update_schedule_status = f"update {SCHEDULE_TABLE} set status = 'C' where id={id}"
                logger.info(f"Updating Schedule table status for successful execution of adhoc type request, query : {update_schedule_status}")
                mysqlcur.execute(update_schedule_status)
            logger.info("Successfully updated schedule table details")
    except Exception as e:
        logger.error(F"Error in updatenextscheduledue() :: {e}")
        logger.error(traceback.print_exc())
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysqlcur.close()
            mysql_conn.close()




def data_source_input(type_of_request, datasource_id, mysql_cursor, logger):
    try:
        logger.info(f"Selected Dataset as source for {type_of_request}")
        # fetch latest runNUmber
        logger.info(f" executing query: {SUPP_DATASET_MAX_RUN_NUMBER_QUERY, (datasource_id,)}")
        mysql_cursor.execute(SUPP_DATASET_MAX_RUN_NUMBER_QUERY, (datasource_id,))
        result = mysql_cursor.fetchone()
        max_runNumber = result['runNumber']
        status = result['status']
        if status == "C":
            table_name = f"{MAIN_DATASET_TABLE_PREFIX}{str(datasource_id)}_{str(max_runNumber)}"
            return table_name
        else:
            raise Exception("Given dataSource is not actively working. So making this request error.")

    except Exception as e:
        logger.error("Exception occurred. Please look into this .... {str(e)}")
        raise Exception(str(e))

def create_main_input_source(sources_loaded, main_request_details):
    try:
        request_id = main_request_details['id']
        channel_name = main_request_details['channelName']
        feed_type = main_request_details['feedType']
        remove_duplicates = main_request_details['removeDuplicates']
        filter_match_fields = main_request_details['FilterMatchFields'] + ',do_inputSourceMappingId'
        schedule_id = main_request_details['ScheduleId']
        run_number = main_request_details['runNumber']

        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        main_input_source_table = MAIN_INPUT_SOURCE_TABLE_PREFIX + str(request_id) + '_' + str(run_number)
        temp_input_source_table = MAIN_INPUT_SOURCE_TABLE_PREFIX + str(request_id) + '_' + str(run_number) + "_TEMP"
        generalized_sources = []
        for source in sources_loaded:
            input_source_mapping_table_name = source[0]
            input_source_mapping_id = source[1]
            if SOURCE_TABLE_PREFIX not in input_source_mapping_table_name:
                generalized_sources.append(
                    f"(select *,'{input_source_mapping_id}' as do_inputSourceMappingId from {input_source_mapping_table_name})")
            else:
                generalized_sources.append(input_source_mapping_table_name)

        main_input_source_query = f"create or replace transient table" \
                                  f" {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{temp_input_source_table}" \
                                  f" as select {filter_match_fields} from {f' intersect select {filter_match_fields} from '.join(generalized_sources)}"
        print(f"Main input source preparation query: {main_input_source_query}")
        sf_cursor.execute(main_input_source_query)
        if 'email_id' in str(filter_match_fields).lower().split(','):
            sf_cursor.execute(f"update {temp_input_source_table} set email_id=lower(trim(email_id))")
            if 'email_md5' not in str(filter_match_fields).lower().split(','):
                sf_cursor.execute(f"alter table {temp_input_source_table} add column email_md5 varchar as md5(email_id)")
        sf_cursor.execute(f"drop table if exists {main_input_source_table}")
        sf_cursor.execute(f"alter table {temp_input_source_table} rename to {main_input_source_table}")
        sf_cursor.execute(f"select count(1) from {main_input_source_table}")
        counts_after_filter = sf_cursor.fetchone()[0]
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,(request_id,schedule_id,run_number,'NA','NA','NA'
                                                                      ,'INITIAL COUNT',0,counts_after_filter,0,0))
        counts_before_filter = counts_after_filter
        if feed_type != 'A':
            if feed_type == 'F':
                supp_count = sf_cursor.execute(f"delete from {main_input_source_table} where list_id not in (select listid from {FP_LISTIDS_SF_TABLE})")
                filter_name = 'Third Party listids suppression'
            elif feed_type == 'T':
                supp_count = sf_cursor.execute(f"delete from {main_input_source_table} where list_id in (select listid from {FP_LISTIDS_SF_TABLE})")
                filter_name = 'First Party listids suppression'
            else:
                raise Exception("Unknown feed_type has been configured. Please look into this...")
            counts_after_filter = counts_before_filter - supp_count
            mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,
                                 (request_id, schedule_id, run_number, 'NA', 'Suppression', 'NA'
                                  , filter_name, counts_before_filter, counts_after_filter, 0, 0))
            counts_before_filter = counts_after_filter

        sf_cursor.execute(f"create or replace transient table {temp_input_source_table} like {main_input_source_table}")
        sf_cursor.execute(f"select LISTAGG(COLUMN_NAME,',') WITHIN GROUP (ORDER BY COLUMN_NAME) from information_schema.COLUMNS "
                          f"where table_name='{temp_input_source_table}'")
        insert_fields = sf_cursor.fetchone()[0]
        sf_cursor.execute(f"select LISTAGG(CONCAT('b.',COLUMN_NAME),',') WITHIN GROUP (ORDER BY COLUMN_NAME) from "
                          f"information_schema.COLUMNS where table_name='{temp_input_source_table}'")
        aliased_insert_fields = sf_cursor.fetchone()[0]
        if remove_duplicates == 0:
#Pending Green all feed type
            if feed_type == 'F':
                join_fields = 'a.email_id=b.email_id and a.list_id=b.list_id and a.do_inputSourceMappingId=b.do_inputSourceMappingId'
            if feed_type == 'T':
                join_fields = 'a.email_id=b.email_id and a.do_inputSourceMappingId=b.do_inputSourceMappingId'
            filter_name = 'File level duplicates suppression'
        else:
            if feed_type == 'F':
                join_fields = 'a.email_id=b.email_id and a.do_inputSourceMappingId=b.do_inputSourceMappingId'
            if feed_type == 'T':
                join_fields = 'a.email_id=b.email_id'
            filter_name = 'Across files duplicates suppression'
        for source in sources_loaded:
            input_source_mapping_id = source[1]
            sf_cursor.execute(f"merge into {temp_input_source_table} a using (select * from {main_input_source_table}"
                              f" where do_inputSourceMappingId = '{input_source_mapping_id}') b on {join_fields} when "
                              f"not matched then insert {insert_fields} values {aliased_insert_fields} ")
        sf_cursor.execute(f"drop table {main_input_source_table}")
        # alter table and add column do_suppression_status with default 'clean'  as value
        sf_cursor.execute(f"alter table {temp_input_source_table} add column do_suppressionStatus varchar default 'CLEAN' , add column do_matchStatus varchar default 'NON_MATCH' ")
        sf_cursor.execute(f"alter table {temp_input_source_table} rename to {main_input_source_table}")
        sf_cursor.execute(f"select count(1) from {main_input_source_table}")
        counts_after_filter = sf_cursor.fetchone()[0]
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,
                             (request_id, schedule_id, run_number, 'NA', 'Suppression', 'NA'
                              , filter_name, counts_before_filter, counts_after_filter, 0, 0))
        return counts_after_filter, main_input_source_table

    except Exception as e:
        print(f"Exception occurred while creating main input source table. {str(e)} " + str(traceback.format_exc()))
        raise Exception(f"Exception occurred while creating main input source table. {str(e)} ")
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.close()
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()

def isps_filteration(current_count, main_request_table, isps, logger, mysql_cursor, main_request_details):
    try:
        counts_before_filter = current_count
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        isps_filter = str(isps).replace(",", "','")
        isps_filteration_query = f"delete from {main_request_table} where split_part(email_id,'@',-1) not in ('{isps_filter}')"
        logger.info(f"Deleting non-configured isps records from {main_request_table}. Executing Query: {isps_filteration_query}")
        sf_cursor.execute(isps_filteration_query)
        counts_after_filter = counts_before_filter - sf_cursor.rowcount
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,(main_request_details['id'],main_request_details['ScheduleId'],main_request_details['runNumber'],'NA','Suppression','NA'
                                                                      ,'Configured isps filteration',counts_before_filter,counts_after_filter,0,0))
        return counts_after_filter
    except Exception as e:
        print(f"Exception occurred while performing isps filteration. {str(e)} " + str(traceback.format_exc()))
        raise Exception(f"Exception occurred while performing isps filteration. {str(e)} ")
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def data_append(filter_details, result_table, logger):
    if filter_details['appendPostalFields'] == "1":
        result_table = append_fields(result_table, POSTAL_TABLE, filter_details['postalFields'], POSTAL_MATCH_FIELDS, logger)
    if filter_details['appendProfileFields'] == "1":
        result_table = append_fields(result_table, PROFILE_TABLE, filter_details['profileFields'], PROFILE_MATCH_FIELDS, logger)


def append_fields(result_table, source_table, to_append_columns, match_keys,  logger):
    try:
        logger.info("Executing method append_fields")
        logger.info("Acquiring snowflake connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Snowflake connection acquired successfully...")
        alter_fields_list = to_append_columns.split(",")
        sf_alter_table_query = f"alter table  {result_table}  add column "
        sf_alter_table_query += " varchar ,add column ".join(i for i in alter_fields_list)
        sf_alter_table_query += " varchar"
        logger.info(f"Executing query: {sf_alter_table_query}")
        sf_cursor.execute(sf_alter_table_query)
        logger.info(f"{result_table} altered successfully")
        alter_fields_list = to_append_columns.split(",")
        sf_update_table_query = f"MERGE INTO {result_table}  a using ({source_table}) b ON "
        sf_update_table_query += " AND ".join([f"a.{key} = b.{key}" for key in match_keys.split(",")])
        sf_update_table_query += " WHEN MATCHED THEN  UPDATE SET "
        sf_update_table_query += ", ".join([f"a.{field} = b.{field}" for field in alter_fields_list])
        logger.info(f"Executing query:  {sf_update_table_query}")
        sf_cursor.execute(sf_update_table_query)
        logger.info("Fields appended successfully...")
        return result_table
    except Exception as e:
        logger.error(f"Exception occurred: Please look into it... {str(e)}")
        raise Exception(str(e))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def update_default_values(type_of_request, main_request_table, logger):
    try:
        if type_of_request == "SUPPRESS_MATCH":
            column_to_update = 'do_matchStatus'
            value_to_set = 'MATCH'
        if type_of_request == "SUPPRESS_FILTER":
            column_to_update = 'do_suppressionStatus'
            value_to_set = 'CLEAN'
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Snowflake connection acquired successfully...")
        sf_update_table_query = f"UPDATE {main_request_table}  set {column_to_update} = '{value_to_set}' "
        logger.info(f"Executing query:  {sf_update_table_query}")
        sf_cursor.execute(sf_update_table_query)
        logger.info("Fields appended successfully...")
        return main_request_table
    except Exception as e:
        logger.error(f"Exception occurred: Please look into it... {str(e)}")
        raise Exception(str(e))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()
def load_match_or_filter_file_sources(type_of_request, file_source, file_source_index, main_request_details):
    os.makedirs(f"{LOG_PATH}/{str(main_request_details['id'])}/{type_of_request}/{str(file_source['sourceId'])}/{str(file_source_index)}/", exist_ok=True)
    os.makedirs(f"{FILE_PATH}/{str(main_request_details['id'])}/{type_of_request}/{str(file_source['sourceId'])}/{str(file_source_index)}/", exist_ok=True)
    temp_files_path = f"{FILE_PATH}/{str(main_request_details['id'])}/{type_of_request}/{str(file_source['sourceId'])}/{str(file_source_index)}/"
    source_table = f"SUPPRESSION_{type_of_request}_{str(main_request_details['id'])}_{str(file_source['sourceId'])}_{str(file_source_index)}"
    consumer_logger = create_logger(base_logger_name=f"{type_of_request}_{file_source['sourceId']}_{str(file_source_index)}",
                                    log_file_path=f"{LOG_PATH}/{str(main_request_details['id'])}/{type_of_request}/{str(file_source['sourceId'])}/{str(file_source_index)}/",
                                    log_to_stdout=True)
    file_source_type_id = file_source['sourceId']
    consumer_logger.info(f"Acquiring mysql connection...")
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
    mysql_cursor = mysql_conn.cursor(dictionary=True)
    consumer_logger.info(f"Fetch source_type for the request id: {file_source_type_id}")
    consumer_logger.info(f"Executing query: {FETCH_FILTER_FILE_SOURCE_INFO, (file_source_type_id,)}")
    mysql_cursor.execute(FETCH_FILTER_FILE_SOURCE_INFO, (file_source_type_id,))
    file_source_details = mysql_cursor.fetchone()
    hostname = file_source_details['hostname']
    port = file_source_details['port']
    username = file_source_details['username']
    password = file_source_details['password']
    source_type = file_source_details['sourceType']
    source_sub_type = file_source_details['sourceSubType']
    input_data_dict = {'filePath': file_source['filePath'], 'delimeter': file_source['delimeter'], 'headerValue': file_source['headerValue']}
    request_id = main_request_details['id']
    run_number = main_request_details['runNumber']
    schedule_id = main_request_details['ScheduleId']
    source_table = process_file_type_request(request_id, source_table, run_number, schedule_id, source_sub_type, input_data_dict,
                                             mysql_cursor, consumer_logger, "", temp_files_path, hostname,
                                             port, username, password)

    return tuple([file_source_index, source_table, file_source['columns']])



def perform_filter_or_match(type_of_request, main_request_details, main_request_table, sorted_filter_sources_loaded ,mysql_cursor, logger, current_count):
    try:
        counts_before_filter = current_count
        is_first_match_filter = True
        if type_of_request == "SUPPRESS_MATCH":
            column_to_update = 'do_matchStatus'
            default_value = 'NON_MATCH'
        if type_of_request == "SUPPRESS_FILTER":
            column_to_update = 'do_suppressionStatus'
            default_value = 'CLEAN'
        logger.info(f"perform_filter_or_match method for {type_of_request} invoked..")
        logger.info("Acquiring snowflake connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Snowflake connection acquired successfully...")
        for filter_source in sorted_filter_sources_loaded:
            if filter_source[2] == 'ByField':
                filter = filter_source[1]
                if filter['dataType'] == 'string' and filter['searchType'] in ('like', 'not like'):
                    filter['value'] = f"%{filter['value']}%"
                if filter['dataType'] != 'number' and filter['searchType'] != '>=':
                    filter['value'] = "'" + filter['value'] + "'"
                if filter['searchType'] in ('in', 'not in') and filter['dataType'] == 'number':
                    filter['value'] = "(" + filter['value'] + ")"
                elif filter['searchType'] in ('in', 'not in') and filter['dataType'] != 'number':
                    filter['value'] = "(" + filter['value'].replace(',', '\',\'') + ")"
                if filter['searchType'] == 'between' and filter['dataType'] != 'number':
                    filter['value'] = filter['value'].replace(',', '\' and \'')
                elif filter['searchType'] == 'between' and filter['dataType'] == 'number':
                    filter['value'] = filter['value'].replace(',', ' and ')
                if filter['searchType'] == '>=':
                    filter['value'] = f"current_date() - interval '{filter['value']} days'"
                filter_name = f"ByField: {filter['fieldName']} {filter['searchType']} {filter['value']}"
                sf_update_table_query = f"UPDATE {main_request_table}  a  SET  a.{column_to_update} ='{filter_name}'" \
                                        f" WHERE {filter['fieldName']} {filter['searchType']} {filter['value']} "
            else:
                match_fields = filter_source[1].split(",")
                source_table = filter_source[0]
                filter_name = source_table
                sf_update_table_query = f"UPDATE {main_request_table}  a  SET  a.{column_to_update} = '{source_table}' FROM ({source_table}) b WHERE "
                sf_update_table_query += " AND ".join([f"a.{key} = b.{key}" for key in match_fields])
                sf_update_table_query += f" AND a.{column_to_update} = '{default_value}' "
            if type_of_request == "SUPPRESS_FILTER":
                sf_update_table_query += f" AND a.do_suppressionStatus != 'NON_MATCH' "
            logger.info(f"Executing query:  {sf_update_table_query}")
            sf_cursor.execute(sf_update_table_query)
            if type_of_request == "SUPPRESS_MATCH":
                if is_first_match_filter:
                    counts_before_filter = 0
                    is_first_match_filter = False
                counts_after_filter = counts_before_filter + sf_cursor.rowcount
                filter_type = 'Match'
            elif type_of_request == "SUPPRESS_FILTER":
                counts_after_filter = counts_before_filter - sf_cursor.rowcount
                filter_type = 'Suppression'
            mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,
                                 (main_request_details['id'], main_request_details['ScheduleId'],
                                  main_request_details['runNumber'], 'NA', filter_type, 'NA', filter_name,
                                  counts_before_filter, counts_after_filter, 0, 0))
            counts_before_filter = counts_after_filter
            logger.info(f"perform_filter_or_match method for {type_of_request} executed successfully...")
        return counts_after_filter
    except Exception as e:
        logger.error(f"Exception occurred: Please look into this. {str(e)}" + str(traceback.format_exc()))
        raise Exception(str(e))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()



































# adding code for suppression methods
def apply_green_global_suppression(source_table, result_breakdown_flag , logger):
    try:
        result = {}
        logger.info(f"Applying Green Global Suppression for the given table {source_table}")
        logger.info("Acquiring snowflake Connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Acquired snowflake connection successfully")
        for supp_table in GREEN_GLOBAL_SUPP_TABLES:
            sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{supp_table}' FROM ({supp_table}) b WHERE  lower(trim(a.EMAIL_ID)) = lower(trim(b.email)) AND a.do_suppressionStatus = 'CLEAN' "
            logger.info(f"Executing query:  {sf_update_table_query}")
            sf_cursor.execute(sf_update_table_query)
            rows_effected = sf_cursor.rowcount
            result[supp_table] = int(rows_effected)
            logger.info(f"{supp_table} suppression done successfully...")
        logger.info(f"the result breakdown flag is : {result_breakdown_flag}")
        if result_breakdown_flag:
            return {'Green_Global_Suppression': sum(result.values())}
        else:
            return result
    except Exception as e:
        logger.error(f"Exception occurred: Please look into it... {str(e)}")
        raise Exception(str(e))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def apply_green_feed_level_suppression(source_table, result_breakdown_flag , logger):
    try:
        result = {}
        logger.info(f"Applying Green Feed Level Suppression for the given table {source_table}")
        logger.info("Acquiring snowflake Connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Acquired snowflake connection successfully")
        for supp_table in GREEN_FEED_LEVEL_SUPP_TABLES['email']:
            if "select" in supp_table or "SELECT" in supp_table or "join" in supp_table or "JOIN" in supp_table:
                temp_table_name = supp_table.split()
                try:
                    value_to_set = temp_table_name[temp_table_name.index("from") + 1]
                except:
                    value_to_set = temp_table_name[temp_table_name.index("FROM") + 1]
            sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{value_to_set}' FROM ({supp_table}) b WHERE  lower(trim(a.EMAIL_ID)) = lower(trim(b.email)) AND a.do_suppressionStatus = 'CLEAN' "
            logger.info(f"Executing query:  {sf_update_table_query}")
            sf_cursor.execute(sf_update_table_query)
            rows_effected = sf_cursor.rowcount
            result[value_to_set] = int(rows_effected)
            logger.info(f"{supp_table} suppression done successfully...")
        for supp_table in GREEN_FEED_LEVEL_SUPP_TABLES['email_listid']:
            if "select" in supp_table or "SELECT" in supp_table or "join" in supp_table or "JOIN" in supp_table:
                temp_table_name = supp_table.split()
                try:
                    value_to_set = temp_table_name[temp_table_name.index("from") + 1]
                except:
                    value_to_set = temp_table_name[temp_table_name.index("FROM") + 1]
            sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{value_to_set}' FROM ({supp_table}) b WHERE  lower(trim(a.EMAIL_ID)) = lower(trim(b.email)) AND a.LIST_ID = b.listid AND a.do_suppressionStatus = 'CLEAN' "
            logger.info(f"Executing query:  {sf_update_table_query}")
            sf_cursor.execute(sf_update_table_query)
            rows_effected = sf_cursor.rowcount
            result[value_to_set] = int(rows_effected)
            logger.info(f"{supp_table} suppression done successfully...")
        logger.info(f"the result breakdown flag is : {result_breakdown_flag}")
        if result_breakdown_flag:
            return {'Green_Feed_Level_Suppression': sum(result.values())}
        else:
            return result
    except Exception as e:
        logger.error(f"Exception occurred: Please look into it... {str(e)}")
        raise Exception(str(e))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def apply_infs_feed_level_suppression(source_table, result_breakdown_flag, logger):
    try:
        result = {}
        logger.info(f"Applying INFS Feed Level Suppression for the given table {source_table}")
        logger.info("Acquiring snowflake Connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Acquired snowflake connection successfully")
        for supp_table in GREEN_FEED_LEVEL_SUPP_TABLES['email']:
            if "select" in supp_table or "SELECT" in supp_table or "join" in supp_table or "JOIN" in supp_table:
                temp_table_name = supp_table.split()
                try:
                    value_to_set = temp_table_name[temp_table_name.index("from") + 1]
                except:
                    value_to_set = temp_table_name[temp_table_name.index("FROM") + 1]
            sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{value_to_set}' FROM ({supp_table}) b WHERE  lower(trim(a.EMAIL_ID)) = lower(trim(b.email)) AND a.do_suppressionStatus = 'CLEAN' "
            logger.info(f"Executing query:  {sf_update_table_query}")
            sf_cursor.execute(sf_update_table_query)
            rows_effected = sf_cursor.rowcount
            result[value_to_set] = int(rows_effected)
            logger.info(f"{supp_table} suppression done successfully...")
        for supp_table in GREEN_FEED_LEVEL_SUPP_TABLES['email_listid']:
            if "select" in supp_table or "SELECT" in supp_table or "join" in supp_table or "JOIN" in supp_table:
                temp_table_name = supp_table.split()
                try:
                    value_to_set = temp_table_name[temp_table_name.index("from") + 1]
                except:
                    value_to_set = temp_table_name[temp_table_name.index("FROM") + 1]
            sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{value_to_set}' FROM ({supp_table}) b WHERE  lower(trim(a.EMAIL_ID)) = lower(trim(b.email)) AND a.LIST_ID = b.listid AND a.do_suppressionStatus = 'CLEAN' "
            logger.info(f"Executing query:  {sf_update_table_query}")
            sf_cursor.execute(sf_update_table_query)
            rows_effected = sf_cursor.rowcount
            result[value_to_set] = int(rows_effected)
            logger.info(f"{supp_table} suppression done successfully...")
        for supp_table in GREEN_FEED_LEVEL_SUPP_TABLES['listid_profileid']:
            if "select" in supp_table or "SELECT" in supp_table or "join" in supp_table or "JOIN" in supp_table:
                temp_table_name = supp_table.split()
                try:
                    value_to_set = temp_table_name[temp_table_name.index("from") + 1]
                except:
                    value_to_set = temp_table_name[temp_table_name.index("FROM") + 1]
            sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{value_to_set}' FROM ({supp_table}) b WHERE  a.PROFILE_ID = b.profileid AND a.LIST_ID = b.listid AND a.do_suppressionStatus = 'CLEAN' "
            logger.info(f"Executing query:  {sf_update_table_query}")
            sf_cursor.execute(sf_update_table_query)
            rows_effected = sf_cursor.rowcount
            result[value_to_set] = int(rows_effected)
            logger.info(f"{supp_table} suppression done successfully...")
        logger.info("Applying must and should suppressions...")
        # unsub_details_oteam
        sf_update_table_query = f"UPDATE {source_table} a SET a.do_suppressionStatus = 'unsub_details_oteam' FROM unsub_details_oteam b where iff(a.listid='2','3188',a.listid)=iff(b.listid='2','3188',b.listid) AND a.EMAIL_ID=b.email AND a.do_suppressionStatus = 'CLEAN'"
        logger.info(f"Executing query:  {sf_update_table_query}")
        sf_cursor.execute(sf_update_table_query)
        rows_effected = sf_cursor.rowcount
        if 'unsub_details_oteam' in result.keys():
            result['unsub_details_oteam'] += int(rows_effected)
        else:
            result['unsub_details_oteam'] = int(rows_effected)
        # BLUE_CLIENT_DATA_SUPPRESSION
        sf_update_table_query = f"UPDATE {source_table} a SET a.do_suppressionStatus = 'BLUE_CLIENT_DATA_SUPPRESSION' FROM BLUE_CLIENT_DATA_SUPPRESSION b where a.EMAIL_MD5=b.md5hash and a.listid in (select listid from BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS) AND a.do_suppressionStatus = 'CLEAN'"
        logger.info(f"Executing query:  {sf_update_table_query}")
        sf_cursor.execute(sf_update_table_query)
        rows_effected = sf_cursor.rowcount
        if value_to_set in result.keys():
            result[value_to_set] += int(rows_effected)
        else:
            result[value_to_set] = int(rows_effected)
        # ACCOUNT NAME  SUPPRESSION
        logger.info("Creating a temp table for the source table")
        sf_create_temp_table_query = f" create table {source_table}_temp clone {source_table}"
        logger.info(f"Executing query : {sf_create_temp_table_query}")
        sf_cursor.execute(sf_create_temp_table_query)
        sf_alter_temp_table_query = f"alter table {source_table}_temp add column account_name varchar"
        logger.info(f" Executing query : {sf_alter_temp_table_query}")
        sf_cursor.execute(sf_alter_temp_table_query)
        sf_update_temp_table_query = f"update {source_table}_temp a set a.account_name=b.account_name from INFS_ORANGE_MAPPING_TABLE b where a.LIST_ID=b.listid"
        logger.info(f" Executing query : {sf_update_temp_table_query}")
        sf_cursor.execute(sf_update_temp_table_query)
        sf_update_temp_table_query = f"update {source_table}_temp a set a.do_suppressionStatus = 'unsub_details_oteam' from (select c.email,d.account_name from unsub_details_oteam c join INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) b where a.account_name=b.account_name and a.EMAIL_ID=b.email and a.do_suppressionStatus = 'CLEAN'"
        logger.info(f" Executing query : {sf_update_temp_table_query}")
        sf_cursor.execute(sf_update_temp_table_query)
        if 'unsub_details_oteam' in result.keys():
            result['unsub_details_oteam'] += int(rows_effected)
        else:
            result['unsub_details_oteam'] = int(rows_effected)
        sf_update_temp_table_query = f"update {source_table}_temp a set a.do_suppressionStatus = 'APT_CUSTOM_CONVERSIONS_DATA_OTEAM' from (select c.profileid,d.account_name from APT_CUSTOM_CONVERSIONS_DATA_OTEAM c join INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) b where a.account_name=b.account_name and a.PROFILE_ID=b.profileid and a.do_suppressionStatus = 'CLEAN'"
        logger.info(f" Executing query : {sf_update_temp_table_query}")
        sf_cursor.execute(sf_update_temp_table_query)
        if 'APT_CUSTOM_CONVERSIONS_DATA_OTEAM' in result.keys():
            result['APT_CUSTOM_CONVERSIONS_DATA_OTEAM'] += int(rows_effected)
        else:
            result['APT_CUSTOM_CONVERSIONS_DATA_OTEAM'] = int(rows_effected)
        sf_update_temp_table_query = f"update {source_table}_temp a set a.do_suppressionStatus = 'EMAIL_REPLIES_TRANSACTIONAL' from (select email,listid from EMAIL_REPLIES_TRANSACTIONAL a join GM_SUBID_DOMAIN_DETAILS b on lower(trim(a.domain))=lower(trim(b.domain)) where a.id > 17218326) c join INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) b where a.account_name=b.account_name and a.EMAIL_ID=b.email and a.do_suppressionStatus = 'CLEAN'"
        logger.info(f" Executing query : {sf_update_temp_table_query}")
        sf_cursor.execute(sf_update_temp_table_query)
        if 'EMAIL_REPLIES_TRANSACTIONAL' in result.keys():
            result['EMAIL_REPLIES_TRANSACTIONAL'] += int(rows_effected)
        else:
            result['EMAIL_REPLIES_TRANSACTIONAL'] = int(rows_effected)
        sf_update_temp_table_query = f"update {source_table}_temp a set a.do_suppressionStatus = 'INFS_UNSUBS_ACCOUNT_WISE' from (select c.email,d.account_name from INFS_UNSUBS_ACCOUNT_WISE c join INFS_ORANGE_MAPPING_TABLE d on c.account_name=d.account_name ) b where a.account_name=b.account_name and a.EMAIL_ID=b.email and a.do_suppressionStatus = 'CLEAN'"
        logger.info(f" Executing query : {sf_update_temp_table_query}")
        sf_cursor.execute(sf_update_temp_table_query)
        if 'INFS_UNSUBS_ACCOUNT_WISE' in result.keys():
            result['INFS_UNSUBS_ACCOUNT_WISE'] += int(rows_effected)
        else:
            result['INFS_UNSUBS_ACCOUNT_WISE'] = int(rows_effected)
        sf_update_temp_table_query = f"update {source_table}_temp a set a.do_suppressionStatus = 'infs_account_level_static_suppression_data' from (select c.email,d.account_name from infs_account_level_static_suppression_data c join INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) b where a.account_name=b.account_name and a.EMAIL_ID=b.email and a.do_suppressionStatus = 'CLEAN'"
        logger.info(f" Executing query : {sf_update_temp_table_query}")
        sf_cursor.execute(sf_update_temp_table_query)
        if 'infs_account_level_static_suppression_data' in result.keys():
            result['infs_account_level_static_suppression_data'] += int(rows_effected)
        else:
            result['infs_account_level_static_suppression_data'] = int(rows_effected)
        sf_update_temp_table_query = f"update {source_table}_temp a set a.do_suppressionStatus = 'BLUE_CLIENT_DATA_SUPPRESSION' from BLUE_CLIENT_DATA_SUPPRESSION b where a.EMAIL_MD5=b.md5hash and a.account_name in (select account_name from BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS c join INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) AND  a.do_suppressionStatus = 'CLEAN'"
        logger.info(f" Executing query : {sf_update_temp_table_query}")
        sf_cursor.execute(sf_update_temp_table_query)
        if 'BLUE_CLIENT_DATA_SUPPRESSION' in result.keys():
            result['BLUE_CLIENT_DATA_SUPPRESSION'] += int(rows_effected)
        else:
            result['BLUE_CLIENT_DATA_SUPPRESSION'] = int(rows_effected)

        logger.info(f"the result breakdown flag is : {result_breakdown_flag}")
        if result_breakdown_flag:
            return {'Green_Global_Suppression': sum(result.values())}
        else:
            return result
    except Exception as e:
        logger.error(f"Exception occurred: Please look into it... {str(e)}")
        raise Exception(str(e))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()

