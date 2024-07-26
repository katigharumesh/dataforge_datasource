from serviceconfigurations import *
from basicudfs import *

def load_input_source(type_of_request, source, main_request_details):
    try:
        if type_of_request == "SUPPRESSION_REQUEST":
            request_id = source['requestId']
            log_path = SUPP_LOG_PATH
            file_path = SUPP_FILE_PATH
            source_table_prefix = SUPP_SOURCE_TABLE_PREFIX
            index_number = source['indexNumber']
        elif type_of_request == "SUPPRESSION_DATASET":
            request_id = source['dataSourceId']
            log_path = LOG_PATH
            file_path = FILE_PATH
            source_table_prefix = SOURCE_TABLE_PREFIX
            index_number = source['id']
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
        feed_type = main_request_details['feedType']
        distinct_filter = 'distinct'
        if input_data is not None:
            input_data_dict = json.loads(input_data.strip('"').replace("'", '"'))
        consumer_logger = create_logger(base_logger_name=f"source_{str(mapping_id)}_{str(request_id)}_{str(run_number)}", log_file_path=f"{log_path}/{str(request_id)}/{str(run_number)}/", log_to_stdout=False)
        consumer_logger.info(f"Processing task: {str(source)}")
        consumer_logger.info(f"Acquiring mysql connection...")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        consumer_logger.info("Mysql Connection established successfully...")
        if source_id == 0 and data_source_id is not None:
            dataset_table_name = data_source_input("Suppression Request Input Source", data_source_id, mysql_cursor, consumer_logger)
            mysql_cursor.execute(FETCH_DATASET_COUNT,(data_source_id,int(dataset_table_name.split('_')[-1])))
            dataset_count = mysql_cursor.fetchone()['recordCount']
            mysql_cursor.execute(FETCH_DATASET_NAME,(data_source_id,))
            dataset_name = mysql_cursor.fetchone()['name']
            mysql_cursor.execute(SUPP_INSERT_FILE_DETAILS, (
                schedule_id, run_number, mapping_id, dataset_count, f'Dataset {dataset_name}',
                'DATA OPS SERVICE', 'DATA OPS SERVICE', 'NA', 'NA', 'C', ''))
            return tuple([dataset_table_name, index_number, mapping_id])
        consumer_logger.info(f"Acquiring snowflake connection...")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        consumer_logger.info("Snowflake connection established Successfully....")

        source_table = source_table_prefix + str(request_id) + '_' + str(mapping_id) + '_' + str(run_number)
        temp_source_table = source_table_prefix + str(request_id) + '_' + str(mapping_id) + '_' + str(run_number) + '_TEMP'
        if source_type == "F":
            temp_files_path = f"{file_path}/{str(request_id)}/{str(run_number)}/{str(mapping_id)}/"
            os.makedirs(temp_files_path,exist_ok=True)
            source_table = process_file_type_request(type_of_request,request_id, source_table, run_number, schedule_id,
                                                     source_sub_type, input_data_dict, mysql_cursor, consumer_logger,
                                                     mapping_id, temp_files_path, main_request_details, hostname, port,
                                                     username, password)
            return tuple([source_table, index_number, mapping_id])

        elif source_type == "D":
            if sf_account != SNOWFLAKE_CONFIGS['account']:
                consumer_logger.info("Snowflake account mismatch. Pending implementation.")
                raise Exception('Snowflake account mismatch. Pending implementation.')
            if source_sub_type in ('R', 'D', 'P', 'M', 'J'):
                if sf_table is not None and sf_table!='NULL':
                    sf_data_source = f"{sf_database}.{sf_schema}.{sf_table}"
                else:
                    sf_data_source = "(" + sf_query + ")"
                where_conditions = []
                for filter in input_data_dict:
                    if dict(filter).__len__() != 1:
                        if filter['dataType'] == 'String' and filter['searchType'] in ('like', 'not like'):
                            filter['value'] = f"%{filter['value']}%"
                        if filter['dataType'] != 'Number' and filter['searchType'] != '>=' and filter.get('dateRangeType','NA') != 'R':
                            filter['value'] = "'" + filter['value'] + "'"
                        if filter['searchType'] in ('in', 'not in') and filter['dataType'] == 'Number':
                            filter['value'] = "(" + filter['value'] + ")"
                        elif filter['searchType'] in ('in', 'not in') and filter['dataType'] != 'Number':
                            filter['value'] = "(" + filter['value'].replace(',', '\',\'') + ")"
                        if filter['searchType'] == 'between' and filter['dataType'] == 'Number':
                            filter['value'] = filter['value'].replace(',', ' and ')
                        elif filter['searchType'] == 'between' and filter['dateRangeType'] == 'C':
                            filter['value'] = filter['value'].replace(',', '\' and \'')
                        elif filter['searchType'] == 'between' and filter['dateRangeType'] == 'R':
                            filter['value'] = f"current_date() - interval '{str(filter['value']).split(',')[0]} days'" \
                                              f" and current_date() - interval '{str(filter['value']).split(',')[1]} days'"
                        if filter['searchType'] == '>=':
                            filter['searchType'] = 'between'
                            if filter['value'] == 'T':
                                filter['value'] = f"'2019-01-01' and current_date()"
                            else:
                                filter['value'] = f"current_date() - interval '{filter['value']} days' and current_date()"

                    touch_filter = False

                    if 'touchCount' in filter and source_sub_type in ('R', 'D'):
                        touch_filter = True
                        touch_count = filter['touchCount']
                        if int(touch_count) > 1:
                            distinct_filter = ''

                    if dict(filter).__len__() != 1:
                        where_conditions.append(f" {filter['fieldName']} {filter['searchType']} {filter['value']} ")
                if feed_type == 'F':
                    if main_request_details['channelName'] == 'INFS':
                        grouping_fields = 'ACCOUNT_NAME,email_id'
                        join_fields = 'a.ACCOUNT_NAME=b.ACCOUNT_NAME and a.email_id=b.email_id'
                    else:
                        grouping_fields = 'list_id,email_id'
                        join_fields = 'a.list_id=b.list_id and a.email_id=b.email_id'
                else:
                    grouping_fields = 'email_id'
                    join_fields = 'a.email_id=b.email_id'
                feed_type_cond = ''
                if source_sub_type in ('R', 'D'):
                    if main_request_details['channelName'] == 'INFS':
                        fp_listids_table = OTEAM_FP_LISTIDS_SF_TABLE
                    else:
                        fp_listids_table = FP_LISTIDS_SF_TABLE
                    if feed_type == 'F':
                        feed_type_cond = f'and list_id in (select listid from {fp_listids_table})'
                    elif feed_type == 'T':
                        feed_type_cond = f'and list_id not in (select listid from {fp_listids_table})'

                source_table_preparation_query = f"create or replace transient table " \
                                                 f"{SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{temp_source_table} " \
                                                 f"as select * " \
                                                 f"from {sf_data_source} where {' and '.join(where_conditions)} {feed_type_cond}"
                consumer_logger.info("Source table preparation query: " + source_table_preparation_query)
                sf_cursor.execute(source_table_preparation_query)
                if touch_filter:
                    touch_filter_query = f"delete from {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{temp_source_table} a " \
                                         f"using (select {grouping_fields} from" \
                                         f" {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{temp_source_table} " \
                                         f"group by {grouping_fields} having count(1)< {touch_count}) b where {join_fields}"
                    consumer_logger.info(f"Applying touch filter, executing query: {touch_filter_query}")
                    sf_cursor.execute(touch_filter_query)
                # changes for the Do_InputSource for delivered or resp
                #if source_sub_type in ('D','R'):
                #   sf_source_name = sf_source_name_generator(sf_source_name ,input_data_dict,touch_filter=False)
                sf_cursor.execute(f"create or replace transient table "
                                  f"{SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table} "
                                  f"as select {distinct_filter} {main_request_details['FilterMatchFields']} from {temp_source_table} ")
                sf_cursor.execute(f"drop table if exists {temp_source_table}")
                sf_cursor.execute(f"alter table {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table}" \
                                  f" add column do_inputSource varchar default '{sf_source_name}', do_inputSourceMappingId varchar default '{mapping_id}'")
                sf_cursor.execute(
                    f"select count(1) from {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{source_table} ")
                records_count = sf_cursor.fetchone()[0]
                if type_of_request == "SUPPRESSION_REQUEST":
                    insert_file_details = SUPP_INSERT_FILE_DETAILS
                if type_of_request == "SUPPRESSION_DATASET":
                    insert_file_details = INSERT_FILE_DETAILS
                #mysql_cursor.execute(DELETE_FILE_DETAILS, (schedule_id, run_number, mapping_id))
                mysql_cursor.execute(insert_file_details, (
                    schedule_id, run_number, mapping_id, records_count, sf_source_name,
                    'DATA OPS SERVICE', 'DATA OPS SERVICE', 'NA', 'NA','C',''))
                return tuple([source_table, index_number , mapping_id])
            else:
                consumer_logger.info("Unknown source_sub_type selected")
                raise Exception("Unknown source_sub_type selected")
        else:
            consumer_logger.info("Unknown source_type selected")
            raise Exception("Unknown source_type selected")
    except CustomError as e:
        consumer_logger.error(f"Exception occurred for sourceid:{mapping_id} Error: {str(e)}")
        raise CustomError(f"Sourceid:{mapping_id} Error: {str(e)}")
    except Exception as e:
        consumer_logger.error(f"Exception occurred for sourceid:{mapping_id} Error: {str(e)}" + str(traceback.format_exc()))
        raise CustomError('DO4',{'error': f'Sourceid:{mapping_id} Error: {str(e)}. '})
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.close()
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def create_main_datasource(sources_loaded, main_request_details, logger):
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
            logger.info(f"Unknown data_processing_type - {data_processing_type} . ")
            raise CustomError("DO21","Unknown data_processing_type - {data_processing_type} . ")
        logger.info("Acquiring Snowflake Connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Snowflake connection established Successfully.")
        main_datasource_table = MAIN_DATASET_TABLE_PREFIX + str(data_source_id) + '_' + str(run_number)
        temp_datasource_table = MAIN_DATASET_TABLE_PREFIX + str(data_source_id) + '_' + str(run_number) + "_TEMP"
        main_datasource_query = f"create or replace transient table {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{temp_datasource_table} as select distinct {filter_match_fields} from {sf_data_source}"
        logger.info(f"Main datasource preparation query: {main_datasource_query}")
        sf_cursor.execute(main_datasource_query)
        if 'email_id' in str(filter_match_fields).lower().split(','):
            sf_cursor.execute(f"update {temp_datasource_table} set email_id=lower(trim(email_id))")
            isps_filter = str(isps).replace(",","','")
            logger.info("ISP filtration process initiated.")
            sf_cursor.execute(f"alter table {temp_datasource_table} add column if not exists isp varchar")
            sf_cursor.execute(f"update {temp_datasource_table} set isp=split_part(email_id,'@',-1)")
            mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
            mysql_cursor = mysql_conn.cursor(dictionary=True)
            mysql_cursor.execute(FETCH_GM_CONFIGURED_ISPS)
            gm_configured_isps = mysql_cursor.fetchone()['isps']
            sf_cursor.execute(
                f"update {temp_datasource_table} set isp = 'Others' where isp not in ({gm_configured_isps})")

            sf_cursor.execute(f"delete from {temp_datasource_table} where isp not in ('{isps_filter}')")
            if 'email_md5' not in str(filter_match_fields).lower().split(','):
                logger.info("Adding column email_md5 if not available...")
                sf_cursor.execute(f"alter table {temp_datasource_table} add column email_md5 varchar as md5(email_id)")
        sf_cursor.execute(f"alter table {temp_datasource_table} add column do_inputSource varchar default '{data_source_name}'")
        sf_cursor.execute(f"drop table if exists {main_datasource_table}")
        sf_cursor.execute(f"alter table {temp_datasource_table} rename to {main_datasource_table}")
        sf_cursor.execute(f"select count(1) from {main_datasource_table}")
        record_count = sf_cursor.fetchone()[0]
        logger.info(f"Final table : {main_datasource_table} Count : {record_count}")
        logger.info("Fetching Error desc to find any failed files... ")
        logger.info(f"Executing query: {FETCH_ERROR_MSG, (str(schedule_id), str(run_number))}")
        mysql_cursor.execute(FETCH_ERROR_MSG, (str(schedule_id), str(run_number)))
        error_desc_dict = mysql_cursor.fetchone()
        if error_desc_dict['error_msg'] is not None:
            logger.info(f"fetched Error message is :: {error_desc_dict['error_msg']}")
            schedule_status_value = 'P'
            mysql_cursor.execute(UPDATE_SCHEDULE_STATUS, (schedule_status_value, record_count, '', data_source_id, run_number))
        else:
            schedule_status_value = 'C'
            mysql_cursor.execute(UPDATE_SCHEDULE_STATUS, (schedule_status_value, record_count, '', data_source_id, run_number))
        return schedule_status_value
    except Exception as e:
        logger.error(f"Exception occurred while creating main_datasource. {str(e)}" + str(traceback.format_exc()))
        raise CustomError('DO2',{'error': str(e)})
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
        result_files =[]
        if isinstance(self.connection, paramiko.Transport):
            sftp = paramiko.SFTPClient.from_transport(self.connection)
            files_list = sftp.listdir(remote_directory)
            for file in files_list:
                file_attr = sftp.stat(remote_directory+file)
                result_files.append(tuple([file,file_attr.st_mtime]))
        elif isinstance(self.connection, ftplib.FTP):
            files_list = self.connection.nlst(remote_directory)
            for file in files_list:
                modified_time = self.connection.sendcmd('MDTM ' + remote_directory+file)
                last_modified= modified_time[-14:] if modified_time.startswith('213') else 'N/A'
                result_files.append(tuple([file,last_modified]))
        newfiles_list = [x[0] for x in sorted(result_files, key=lambda x: x[1], reverse=True)]
        return newfiles_list

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



def perform_conversion(local_file):
    if local_file.endswith('.gz'):
        temp_file = local_file + '.tmp'
        with gzip.open(local_file, 'rb') as f_in, open(temp_file, 'wb') as f_out:
            for line in f_in:
                line = line.replace(b'\r\n', b'\n')
                f_out.write(line)
        os.rename(temp_file, local_file)
    else:
        try:
            subprocess.run(['dos2unix', local_file], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error converting file {local_file}: {e}")
        # For regular text files
        # with fileinput.FileInput(local_file, inplace=True, mode='r') as f:
        #     for line in f:
        #         print(line.replace('\r\n', '\n'),end='')
                #print(line.decode('utf-8'), end='')
def requires_conversion(filename):
    """Check if dos2unix conversion is required."""
    if filename.split(".")[-1] == ".gz":
        with gzip.open(filename, 'rb') as f:
            for line in f:
                if b'\r\n' in line:
                    return True
    else:
        with open(filename, 'rb') as f:
            for line in f:
                if b'\r\n' in line:
                    return True
    return False

def is_valid_filename(filename):
    pattern = r'^[a-zA-Z0-9._ -]+$'
    return bool(re.match(pattern, filename))

def validate_header(file, header, delimiter):
    if file.endswith('gz'):
        with gzip.open(file, 'rb') as f:
            first_line = f.readline().decode('utf-8')
    else:
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
            result_list =[]
            files = os.listdir(mount_path)
            for file in files:
                stats = os.stat(mount_path + file)
                result_list.append(tuple([file, stats.st_mtime]))
            newfiles_list = [x[0] for x in sorted(result_list, key=lambda x: x[1], reverse=True)]
            return newfiles_list
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
            result_files =[]
            bucket_name = file_path.split('/')[2]
            prefix = '/'.join(file_path.split('/')[3:])
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
            s3_files = [str(obj['Key']).split('/')[-1] for obj in response.get('Contents', [])]
            for file in s3_files:
                key = '/'.join(str(file_path + file).split('/')[3:])
                response = self.s3_client.head_object(Bucket=bucket_name, Key=key)
                result_files.append(tuple([file, response['LastModified'].isoformat()]))
            newfiles_list = [x[0] for x in sorted(result_files, key=lambda x: x[1], reverse=True)]
            return newfiles_list
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
            streaming_body = obj['Body']

            # Check if the file is gzipped
            if file.endswith('.gz'):
                with gzip.GzipFile(fileobj=streaming_body) as f:
                    first_line = f.readline().decode('utf-8')  # Decode the byte string to a regular string
            else:
                first_line = streaming_body.read().decode('utf-8').split('\n', 1)[0]  # Read and decode the first line

            # Ensure header is processed similarly
            if len(first_line.split(delimiter)) == len(header_value.split(delimiter)):
                return True
            else:
                return False
        except Exception as e:
            print(f"Error occurred during header validation for {file} file. Error: {e}")
            return False

def process_file_type_request(type_of_request,request_id, source_table, run_number,schedule_id, source_sub_type, input_data_dict, mysql_cursor, consumer_logger, mapping_id, temp_files_path, main_request_details, hostname = None, port = None, username = None, password = None):
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
            raise Exception("Wrong method called. This method works only for SFTP/FTP/NFS requests only.")

        isFile = False if input_data_dict["filePath"].endswith("/") else True
        isDir = True if input_data_dict["filePath"].endswith("/") else False

        result = {}
        #consumer_logger.info(f"Fetching runNumber from table... ")
        #consumer_logger.info(f"Executing query: {RUN_NUMBER_QUERY.replace('REQUEST_ID', str(mapping_id))}")
        #mysql_cursor.execute(RUN_NUMBER_QUERY.replace('REQUEST_ID', str(mapping_id)))

        last_successful_run_number = -1
        # mysql_cursor.execute(last_successful_run_number_query)
        if type_of_request == "SUPPRESSION_REQUEST":
            last_successful_run_number_query = SUPP_LAST_SUCCESSFUL_RUN_NUMBER_QUERY
            fetch_last_iteration_file_details_query = SUPP_FETCH_LAST_ITERATION_FILE_DETAILS_QUERY
            insert_file_details = SUPP_INSERT_FILE_DETAILS
        if type_of_request == "SUPPRESSION_DATASET":
            last_successful_run_number_query = LAST_SUCCESSFUL_RUN_NUMBER_QUERY
            fetch_last_iteration_file_details_query = FETCH_LAST_ITERATION_FILE_DETAILS_QUERY
            insert_file_details = INSERT_FILE_DETAILS

        last_iteration_files_details = []
        if run_number != 1:
            mysql_cursor.execute(last_successful_run_number_query, (str(request_id),))
            last_successful_run_number = int(mysql_cursor.fetchone().get('runNumber',-1))
            mysql_cursor.execute(fetch_last_iteration_file_details_query, (str(mapping_id), str(last_successful_run_number)))
            last_iteration_files_details = mysql_cursor.fetchall()
            consumer_logger.info(f"Fetched last iteration_details: {last_iteration_files_details}")
        consumer_logger.info(f"Establishing Snowflake connection...")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        consumer_logger.info("Snowflake connection acquired successfully")
        if run_number == 1 or last_successful_run_number == -1  or last_iteration_files_details == []:
            field_delimiter = input_data_dict['delimiter']
            header_list = input_data_dict['headerValue'].split(str(field_delimiter))
            sf_create_table_query = f"create or replace transient table  {source_table}  ( "
            sf_create_table_query += " varchar ,".join(i for i in header_list)
            sf_create_table_query += f" varchar , do_inputSource varchar, do_inputSourceMappingId varchar default '{mapping_id}' )"
        else:
            last_run_table_name = source_table[:source_table.rindex('_')+1]+str(last_successful_run_number)
            sf_create_table_query = f"create or replace transient table  {source_table}  clone {last_run_table_name} "
        consumer_logger.info(f"Executing query: {sf_create_table_query}")
        sf_cursor.execute(sf_create_table_query)

        if isFile:
            file_details_list = []
            files_list = input_data_dict['filePath'].split(",")
            consumer_logger.info("File List: "+str(files_list))
            if len(files_list) >= 1:
                consumer_logger.info("There are one or more files with comma separated...")
                for file in files_list:
                    file_details_dict = process_single_file(mapping_id, temp_files_path,  run_number , source_obj, file,consumer_logger,input_data_dict, source_table, last_iteration_files_details, source_sub_type, username, password)
                    # add logic to insert the file details into table
                    fileName = file_details_dict["filename"]
                    count = file_details_dict["count"]
                    size = file_details_dict["size"]
                    last_modified_time = file_details_dict["last_modified_time"]
                    file_status = file_details_dict['status']
                    error_desc = file_details_dict['error_msg']
                    if type_of_request == "Match" or type_of_request == "Suppression":
                        if file_status == 'E':
                            raise Exception(f"The file {fileName} is errored due to reason: {error_desc} ")
                        continue
                    mysql_cursor.execute(insert_file_details, (schedule_id, run_number, mapping_id, count, fileName, 'DATA_OPS SERVICE', 'DATA_OPS SERVICE', size, last_modified_time, file_status , error_desc))
                    file_details_list.append(file_details_dict)
                    if len(files_list) == 1:
                        if file_status == 'E':
                            raise CustomError(f"The file {fileName} is errored due to reason: {error_desc} ")
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
                consumer_logger.info(f"Older files to be deleted: {to_delete_mysql_formatted}")
                sf_cursor.execute(SF_DELETE_OLD_DETAILS_QUERY,(source_table, to_delete_mysql_formatted))
            else:
                consumer_logger.info("No older files to delete.")

            file_details_list = []
            consumer_logger.info("First time/existing files processing..")
            for file in files_list:
                fully_qualified_file = input_data_dict["filePath"] + file
                file_details_dict = process_single_file(mapping_id, temp_files_path, run_number, source_obj, fully_qualified_file, consumer_logger, input_data_dict, source_table,
                                                        last_iteration_files_details, source_sub_type, username, password)
                fileName = file_details_dict["filename"]
                count = file_details_dict["count"]
                size = file_details_dict["size"]
                last_modified_time = file_details_dict["last_modified_time"]
                file_status = file_details_dict['status']
                error_desc = file_details_dict['error_msg']
                if type_of_request == "Match" or type_of_request == "Suppression":
                    if file_status == 'E':
                        raise Exception(f"The file {fileName} is errored due to reason: {error_desc} ")
                    continue
                mysql_cursor.execute(insert_file_details, (
                    schedule_id, run_number, mapping_id, count, fileName,
                    'DATA_OPS SERVICE', 'DATA_OPS SERVICE', size, last_modified_time,file_status , error_desc))
                file_details_list.append(file_details_dict)
        else:
            consumer_logger.info("Wrong Input...raising Exception..")
            raise Exception("Wrong Input...raising Exception..")
        if type_of_request == "SUPPRESSION_REQUEST":
            consumer_logger.info("Initiating within source, file level prioritization and feed level de-duplication")
            dedup_source_table = source_table + "_DEDUP"
            sf_query = f"create or replace transient table {dedup_source_table} like {source_table}"
            consumer_logger.info(f"Executing query: {sf_query}")
            sf_cursor.execute(sf_query)
            sf_cursor.execute(f"select LISTAGG(COLUMN_NAME,',') WITHIN GROUP (ORDER BY COLUMN_NAME) from"
                              f" information_schema.COLUMNS where table_name='{source_table}'")
            insert_fields = sf_cursor.fetchone()[0]
            sf_cursor.execute(f"select LISTAGG(CONCAT('b.',COLUMN_NAME),',') WITHIN GROUP (ORDER BY COLUMN_NAME) from "
                              f"information_schema.COLUMNS where table_name='{source_table}'")
            aliased_insert_fields = sf_cursor.fetchone()[0]
            sf_cursor.execute(f"update {source_table} set list_id = '000000'  where list_id is null or "
                              f"cast(list_id as string)='NULL' or cast(list_id as string)='null' or cast(list_id as string)='' ")
            channel_name = main_request_details['channelName']
            if main_request_details['removeDuplicates']:
                source_join_fields = ''
            else:
                source_join_fields = 'and a.do_inputSource = b.do_inputSource'
            file_names_list = []
            for fully_qualified_file in files_list:
                file_names_list.append(fully_qualified_file.split("/")[-1])
            if channel_name == 'GREEN':
                for file_name in file_names_list:
                    tp_sf_query = f"merge into {dedup_source_table} a using (select * from {source_table} " \
                                  f"where do_inputSource = '{file_name}' and list_id not in (select " \
                                  f"cast(listid as varchar) from {FP_LISTIDS_SF_TABLE})) b on a.email_id = b.email_id" \
                                  f" {source_join_fields} when not matched then insert " \
                                  f"({insert_fields}) values ({aliased_insert_fields}) "
                    consumer_logger.info(f"Executing: {tp_sf_query}")
                    sf_cursor.execute(tp_sf_query)
            for file_name in file_names_list:
                if channel_name == 'INFS':
                    fp_listid_table = OTEAM_FP_LISTIDS_SF_TABLE
                else:
                    fp_listid_table = FP_LISTIDS_SF_TABLE
                if channel_name == 'GREEN':
                    fp_listid_validation = f'and list_id in (select cast(listid as varchar) from {fp_listid_table})'
                else:
                    fp_listid_validation = ''
                fp_sf_query = f"merge into {dedup_source_table} a using (select * from {source_table}" \
                              f" where do_inputSource = '{file_name}' {fp_listid_validation}) b on a.email_id = b.email_id" \
                              f" and a.list_id = b.list_id {source_join_fields} when not matched then insert " \
                              f"({insert_fields}) values ({aliased_insert_fields}) "
                consumer_logger.info(f"Executing: {fp_sf_query}")
                sf_cursor.execute(fp_sf_query)
            sf_query = f"drop table if exists {source_table}"
            consumer_logger.info(f"Executing query: {sf_query}")
            sf_cursor.execute(sf_query)
            sf_query = f"alter table {dedup_source_table} rename to {source_table}"
            consumer_logger.info(f"Executing query: {sf_query}")
            sf_cursor.execute(sf_query)
        return source_table
    except CustomError as e:
        consumer_logger.info(f"Exception occurred: {str(e)} {str(traceback.format_exc())}")
        raise CustomError(str(e))
    except Exception as e:
        consumer_logger.info(f"Exception occurred: {str(e)} {str(traceback.format_exc())}")
        raise Exception(str(e))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()




def process_single_file(mapping_id, temp_files_path, run_number, source_obj, fully_qualified_file, consumer_logger, input_data_dict,
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
            file_details_dict['error_msg'] = f'The given file {file} is not in required extension. '
            file_details_dict["count"] = '0'
            file_details_dict["size"] = 'NA'
            file_details_dict["last_modified_time"] = 'NA'
            return file_details_dict
        if not is_valid_filename(file):
            file_details_dict['filename'] = file
            file_details_dict["count"] = 0
            file_details_dict["size"] = 'NA'
            file_details_dict['status'] = 'E'
            file_details_dict["last_modified_time"] = 'NA'
            file_details_dict['error_msg'] = f'The file_name {file} is not in the required format. '
            consumer_logger.info(f'The file_name  {file} is not in the required format. ')
            return file_details_dict
        meta_data = source_obj.get_file_metadata(fully_qualified_file)  # metadata from ftp
        consumer_logger.info(f"Meta data fetched successfully for file:{file} Meta data: {meta_data}")
        if run_number != 1 and last_iteration_files_details != []:
            last_iteration_file_names_list = [i["filename"] for i in last_iteration_files_details]
            consumer_logger.info(f"last iteration files are {str(last_iteration_file_names_list)}")
            if file in last_iteration_file_names_list:
                is_old_file = True
                consumer_logger.info("Found filename in last_iteration_file details. Checking for metadata..")
                file_index = last_iteration_file_names_list.index(file)
                if str(meta_data['size']) == last_iteration_files_details[file_index]['size'] and str(meta_data['last_modified']) == last_iteration_files_details[file_index]['last_modified_time']:
                    consumer_logger.info("File " + file + " already processed last time. So skipping the file.")
                    file_details_dict = last_iteration_files_details[file_index]
                    return file_details_dict
        if source_sub_type != 'A':
            source_obj.download_file(fully_qualified_file, temp_files_path + file)
            if file.split(".")[-1] == "gz":
                if input_data_dict['isHeaderExists']:
                    line_count = sum(1 for _ in gzip.open(temp_files_path + file, 'rb')) - 1
                else:
                    line_count = sum(1 for _ in gzip.open(temp_files_path + file, 'rb'))
            else:
                if input_data_dict['isHeaderExists']:
                    line_count = sum(1 for _ in open(temp_files_path + file, 'r')) - 1
                else:
                    line_count = sum(1 for _ in open(temp_files_path + file, 'r'))

            file_details_dict["count"] = line_count
        file_details_dict["filename"] = file
        file_details_dict["size"] = meta_data["size"]
        file_details_dict["last_modified_time"] = meta_data["last_modified"]
        if source_sub_type != 'A':
            if not validate_header(temp_files_path + file , input_data_dict['headerValue'], input_data_dict['delimiter']):
                file_details_dict["count"] = 0
                file_details_dict['status'] = 'E'
                file_details_dict['error_msg'] = 'The header is not matching with the given header. '
                consumer_logger.info('The header is not matching with the given header. ')
                return file_details_dict
        else:
            if not source_obj.header_validation(fully_qualified_file, input_data_dict['headerValue'], input_data_dict['delimiter']):
                file_details_dict["count"] = 0
                file_details_dict['status'] = 'E'
                file_details_dict['error_msg'] = 'The header is not matching with the given header. '
                consumer_logger.info('The header is not matching with the given header. ')
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
            sf_put_file_stage_query = f" PUT 'file://{temp_files_path}/{file}' @{stage_name} "
            consumer_logger.info(f"Executing query: {sf_put_file_stage_query}")
            sf_cursor.execute(sf_put_file_stage_query)
            field_delimiter = input_data_dict['delimiter']
            header_list = input_data_dict['headerValue'].split(str(field_delimiter))
            stage_columns = ", ".join(f"${i + 1}" for i in range(len(header_list)))
            sf_copy_into_query = f"copy into {table_name} FROM (select {stage_columns}, '{file}', '{mapping_id}' FROM @{stage_name} ) FILE_FORMAT = (ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE {header_exists} ) "
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
            sf_update_query = f"update {table_name} set do_inputSource = '{file}',do_inputSourceMappingId= DEFAULT where do_inputSource is null"
            consumer_logger.info(f"Executing query: {sf_update_query}")
            sf_cursor.execute(sf_update_query)
            file_details_dict["count"] = sf_cursor.rowcount
            file_details_dict['status'] = 'C'
            file_details_dict['error_msg'] = ''
        return file_details_dict
    except Exception as e:
        consumer_logger.error(f"Exception occurred while processing file {fully_qualified_file}. {str(e)}" + + str(traceback.format_exc()))
        raise Exception(e)

def update_next_schedule_due(type_of_request, request_id, run_number, logger, request_status='E'):
    try:
        if type_of_request == "SUPPRESSION_REQUEST":
            schedule_table = SUPP_SCHEDULE_TABLE
            column_to_fetch = "requestId"
        elif type_of_request == "SUPPRESSION_DATASET":
            schedule_table = SCHEDULE_TABLE
            column_to_fetch = "datasourceId"
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysqlcur = mysql_conn.cursor()
        mysqlcur.execute("set time_zone='UTC';")
        requestquery = f"select id,{column_to_fetch},runnumber,recurrenceType,startDate,endDate,excludeDates," \
                f"date(nextscheduleDue) as nextscheduledate,sendAt,timezone,sendon,dayOfMonth from {schedule_table} where status='I' " \
                f"and nextScheduleDue<now() and {column_to_fetch}={request_id} and runnumber={run_number} "
        logger.info(f"Pulling schedule details for updation of nextScheduleDue, Query ::{requestquery}")
        mysqlcur.execute(requestquery)
        requestList = mysqlcur.fetchall()
        logger.info(requestList)
        for request in requestList:
            recurrenceType = request[3]
            id = request[0]
            startDate = str(request[4])
            endDate = str(request[5])
            sendAt = str(request[8])
            timezone = str(request[9])
            sendon=request[10]
            monthDay=request[11]
            if request[6] is not None:
                try:
                    excludeDates = request[6].split(',')
                except:
                    excludeDates = request[6].split()
            else:
                excludeDates = None


            if sendon is not None:
                try:
                    sendon=sendon.split(',')
                except:
                    sendon=sendon.split()
            else:
                sendon=None
            #scheduleNextquery = f"update {schedule_table} set status='W',runnumber=runnumber+1 where id={id}"
            #logger.info(f"Updating schedule status and runnumber, query :: {scheduleNextquery}")
            #mysqlcur.execute(scheduleNextquery)

            if (recurrenceType is not None and recurrenceType == 'H'):
                utcTime = ''
                current_date = datetime.now().date()
                if timezone == 'IST':
                    ist = pytz.timezone('Asia/Kolkata')
                    ist_time_format = "%I:%M %p"
                    istTime = datetime.strptime(sendAt, ist_time_format)
                    istTime = datetime.combine(current_date, istTime.time())
                    istTime = ist.localize(istTime)
                    utcTime = istTime.astimezone(pytz.utc).strftime("%H:%M:%S")
                elif timezone == 'EST':
                    est = pytz.timezone('America/New_York')
                    est_time_format = "%I:%M %p"
                    estTime = datetime.strptime(sendAt, est_time_format)
                    estTime = datetime.combine(current_date, estTime.time())
                    estTime = est.localize(estTime)
                    utcTime = estTime.astimezone(pytz.utc).strftime("%H:%M:%S")

                current_date=str(current_date) + ' ' + utcTime
                dcurrent_date = datetime.strptime(current_date,'%Y-%m-%d %H:%M:%S')
                dendDate=datetime.strptime(endDate,'%Y-%m-%d').date()
                dendDate=dendDate+timedelta(days=1)
                print(dendDate)
                #nextscheduleDatequery=f"select nextScheduleDue from {schedule_table} where id={id}"
                #mysqlcur.execute(nextscheduleDatequery)
                #nextscheduleDate=datetime.strptime(str(mysqlcur.fetchone()[0]),'%Y-%m-%d %H:%M:%S')

                dcurrent_date = datetime.strptime(current_date,'%Y-%m-%d %H:%M:%S') + timedelta(hours=1)
                while dcurrent_date<datetime.utcnow():
                    dcurrent_date=dcurrent_date+timedelta(hours=1)

                nextschedulequery = f"update {schedule_table} set nextScheduleDue=" \
                                    f"case when CONVERT_TZ(date_add(nextScheduleDue,INTERVAL 1 HOUR), 'UTC', 'Asia/Kolkata') <= concat(%s,' 00:00:00') Then %s"\
                                    f"else %s end,status=if(date(nextScheduleDue)>endDate,'C','W') where id={id}"
                logger.info(f"Updating nextScheduleDue, query : {nextschedulequery}")
                mysqlcur.execute(nextschedulequery,(dendDate,dcurrent_date,dendDate))
            if (recurrenceType is not None and recurrenceType == 'D'):
                utcTime = ''
                current_date = datetime.now().date()
                if timezone == 'IST':
                    ist = pytz.timezone('Asia/Kolkata')
                    ist_time_format = "%I:%M %p"
                    istTime = datetime.strptime(sendAt, ist_time_format)
                    istTime = datetime.combine(current_date, istTime.time())
                    istTime = ist.localize(istTime)
                    utcTime = istTime.astimezone(pytz.utc).strftime("%H:%M:%S")
                elif timezone == 'EST':
                    est = pytz.timezone('America/New_York')
                    est_time_format = "%I:%M %p"
                    estTime = datetime.strptime(sendAt, est_time_format)
                    estTime = datetime.combine(current_date, estTime.time())
                    estTime = est.localize(estTime)
                    utcTime = estTime.astimezone(pytz.utc).strftime("%H:%M:%S")

                dendDate=datetime.strptime(endDate,'%Y-%m-%d').date()
                dendDate=dendDate+timedelta(days=1)

                if excludeDates is not None:

                    #timestamp = str(datetime.utcnow()).split(' ')[1]

                    nextscheduledatep=datetime.utcnow().date() + timedelta(days=1)
                    while str(nextscheduledatep) in excludeDates:
                        nextscheduledatep += timedelta(days=1)
                    # print(nextscheduledatep)
                    nextscheduleDuep = str(nextscheduledatep) + ' ' + utcTime
                    # print(nextscheduleDuep)

                    nextschedulequery=f"update {schedule_table} set nextScheduleDue = if(%s<=endDate,%s,%s),status=if(date(nextScheduleDue)>endDate,'C','W') where id={id}"
                    logger.info(f"nextschedulequery :: Daily :: {nextschedulequery}")
                    mysqlcur.execute(nextschedulequery,(nextscheduledatep,nextscheduleDuep,dendDate))
                else:
                    nextscheduledatep = datetime.utcnow().date() + timedelta(days=1)
                    nextscheduleDuep = str(nextscheduledatep)+ ' ' + utcTime

                    #nextscheduledatep = datetime.now().date() + timedelta(days=1)
                    nextschedulequery = f"update {schedule_table} set nextScheduleDue=" \
                                        f"if(%s<=endDate,%s,%s),status=if(date(nextScheduleDue)>endDate,'C','W') where id={id}"
                    logger.info(f"Updating nextScheduleDue, query : {nextschedulequery}")
                    mysqlcur.execute(nextschedulequery, (nextscheduledatep,nextscheduleDuep,dendDate))

            if (recurrenceType is not None and recurrenceType == 'W'):
                utcTime = ''
                current_date = datetime.now().date()
                if timezone == 'IST':
                    ist = pytz.timezone('Asia/Kolkata')
                    ist_time_format = "%I:%M %p"
                    istTime = datetime.strptime(sendAt, ist_time_format)
                    istTime = datetime.combine(current_date, istTime.time())
                    istTime = ist.localize(istTime)
                    utcTime = istTime.astimezone(pytz.utc).strftime("%H:%M:%S")
                elif timezone == 'EST':
                    est = pytz.timezone('America/New_York')
                    est_time_format = "%I:%M %p"
                    estTime = datetime.strptime(sendAt, est_time_format)
                    estTime = datetime.combine(current_date, estTime.time())
                    estTime = est.localize(estTime)
                    utcTime = estTime.astimezone(pytz.utc).strftime("%H:%M:%S")


                if sendon is not None:
                    weekDaysdict = {
                        'Sunday': 'SU', 'Monday': 'M', 'Tuesday': 'T', 'Wednesday': 'W',
                        'Thursday': 'TH', 'Friday': 'F', 'Saturday': 'S'
                    }
                    if excludeDates is None:
                        excludeDates=[]

                    utcdate = datetime.utcnow().date() + timedelta(days=1)
                    date_format = '%Y-%m-%d'
                    dstartDate = datetime.strptime(startDate, date_format).date()
                    dendDate = datetime.strptime(endDate, date_format).date()
                    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                    if utcdate <= dendDate:
                        while utcdate <= dendDate:
                            if utcdate >= dstartDate and utcdate not in excludeDates:
                                day_week = utcdate.weekday()
                                day = days[day_week]
                                if day in weekDaysdict and weekDaysdict[day] in sendon:
                                    if utcTime != '':
                                        nextscheduleDuep = str(utcdate) + ' ' + utcTime
                                        #print(nextscheduleDuep)
                                        nextschedulequery = f"update {schedule_table} set nextScheduleDue=" \
                                            f"if(%s<=endDate,%s,endDate),status=if(date(nextScheduleDue)>endDate,'C','W') where id={id}"
                                        logger.info(f"nextschedulequery ::: {nextschedulequery}")
                                        mysqlcur.execute(nextschedulequery, (utcdate,nextscheduleDuep))

                                        break
                                utcdate += timedelta(days=1)
                            else:
                                utcdate += timedelta(days=1)
                    else:

                        updatequery=f"update {schedule_table} set status='C',nextscheduleDue=endDate where id={id}"
                        logger.info(f"updatequery:: {updatequery}")
                        mysqlcur.execute(updatequery)
                else:
                    logger.info("Week days should be selected")
                '''
                if excludeDates is not None:
                    #timestamp = str(datetime.utcnow()).split(' ')[1]
                    nextscheduledate = datetime.utcnow().date() + timedelta(days=7)
                    while nextscheduledate in excludeDates:
                        nextscheduledate += timedelta(days=7)

                    nextscheduleDuep = str(nextscheduledate) + ' ' + utcTime
                    nextschedulequery = f"update {SCHEDULE_TABLE} set nextScheduleDue = if(%s<=endDate,%s,endDate),status=if(date(nextScheduleDue)>endDate,'C','W') where id={id}"
                    logger.info(f"nextschedulequery :: Daily :: {nextschedulequery}")
                    mysqlcur.execute(nextschedulequery, (nextscheduledatep, nextscheduleDuep))
                else:
                    nextscheduledatep = datetime.utcnow().date() + timedelta(days=7)
                    nextscheduleDuep = str(nextscheduledatep)+ ' ' + utcTime

                    nextschedulequery = f"update {SCHEDULE_TABLE} set nextScheduleDue=" \
                                        f"if(%s<=endDate,%s,endDate),status=if(date(nextScheduleDue)>endDate,'C','W') where id={id}"

                    # logger.info(nextschedulequery)
                    logger.info(f"Updating nextScheduleDue, query : {nextschedulequery}")
                    mysqlcur.execute(nextschedulequery, (nextscheduledatep,nextscheduleDuep))'''

            if (recurrenceType is not None and recurrenceType == 'M'):
                utcTime = ''
                current_date = datetime.now().date()
                if timezone == 'IST':
                    ist = pytz.timezone('Asia/Kolkata')
                    ist_time_format = "%I:%M %p"
                    istTime = datetime.strptime(sendAt, ist_time_format)
                    istTime = datetime.combine(current_date, istTime.time())
                    istTime = ist.localize(istTime)
                    utcTime = istTime.astimezone(pytz.utc).strftime("%H:%M:%S")
                if timezone == 'EST':
                    est = pytz.timezone('America/New_York')
                    est_time_format = "%I:%M %p"
                    estTime = datetime.strptime(sendAt, est_time_format)
                    estTime = datetime.combine(current_date, estTime.time())
                    estTime = est.localize(estTime)
                    utcTime = estTime.astimezone(pytz.utc).strftime("%H:%M:%S")
                dendDate=datetime.strptime(endDate,'%Y-%m-%d').date()
                dendDate=dendDate+timedelta(days=1)

                if excludeDates is not None:
                    timestamp = str(datetime.utcnow()).split(' ')[1]
                    nextscheduledate = datetime.utcnow().date() + relativedelta(months=1)
                    while nextscheduledate in excludeDates:
                        nextscheduledate += relativedelta(months=1)

                    nextscheduleDuep = str(nextscheduledate) + ' ' + utcTime
                    nextschedulequery = f"update {schedule_table} set nextScheduleDue = if(%s<=endDate,%s,%s),status=if(date(nextScheduleDue)>endDate,'C','W') where id={id}"
                    logger.info(f"nextschedulequery :: Daily :: {nextschedulequery}")
                    mysqlcur.execute(nextschedulequery, (nextscheduledatep,nextscheduleDuep,dendDate))
                else:
                    nextscheduledatep = datetime.utcnow().date() + relativedelta(months=1)
                    nextscheduleDuep=str(nextscheduledatep) + ' ' + utcTime
                    nextschedulequery = f"update {schedule_table} set nextScheduleDue=" \
                                    f"if(%s<=endDate,%s,%s),status=if(date(nextScheduleDue)>endDate,'C','W') where id={id}"
                    logger.info(f"Updating nextScheduleDue, query : {nextschedulequery}")
                    mysqlcur.execute(nextschedulequery, (nextscheduledatep,nextscheduleDuep,dendDate))
            if request_status == "C" or request_status == "P":
                update_wasInactive_status_query = f"update {schedule_table} set wasInActive=0 where {column_to_fetch}={request_id}"
            if recurrenceType is None :
                update_schedule_status = f"update {schedule_table} set status = '{request_status}' where id={id}"
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
        logger.info("Checking for Dataset active status.. ")
        status_query = f"select isActive from {DATASET_TABLE} where id = %s limit 1"
        logger.info(f" executing query: {status_query, (datasource_id,)}")
        mysql_cursor.execute(status_query, (datasource_id,))
        result1 = mysql_cursor.fetchone()
        if not result1['isActive']:
            raise CustomError('DO5', {'Filter/Dataset': 'Dataset', 'id': str(datasource_id)})
        was_inactive_query = f"select wasInActive from {SCHEDULE_TABLE} where dataSourceId = %s order by id desc limit 1"
        logger.info(f" executing query: {was_inactive_query, (datasource_id,)}")
        mysql_cursor.execute(was_inactive_query, (datasource_id,))
        result2 = mysql_cursor.fetchone()
        if result2['wasInActive']:
            raise CustomError('DO6', {'id': str(datasource_id)})
        logger.info(f" executing query: {SUPP_DATASET_MAX_RUN_NUMBER_QUERY, (datasource_id,)}")
        mysql_cursor.execute(SUPP_DATASET_MAX_RUN_NUMBER_QUERY, (datasource_id,))
        result = mysql_cursor.fetchone()
        max_runNumber = result['runNumber']
        status = result['status']
        if status == "C" or status == "P":
            table_name = f"{MAIN_DATASET_TABLE_PREFIX}{str(datasource_id)}_{str(max_runNumber)}"
            return table_name
        else:
            raise CustomError('DO7', {'id': str(datasource_id)})
    except CustomError as e:
        logger.error("Exception occurred: {str(e)}")
        raise CustomError(e)
    except Exception as e:
        logger.error("Exception occurred: {str(e)}" + str(traceback.format_exc()))
        raise CustomError('DO8',{'error': str(e)})

def create_main_input_source(sources_loaded, main_request_details, filter_details, logger):
    try:
        request_id = main_request_details['id']
        channel_name = main_request_details['channelName']
        feed_type = main_request_details['feedType']
        remove_duplicates = main_request_details['removeDuplicates']
        filter_match_fields = main_request_details['FilterMatchFields'] + ',do_inputSource,do_inputSourceMappingId'
        schedule_id = main_request_details['ScheduleId']
        run_number = main_request_details['runNumber']

        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        main_input_source_table = MAIN_INPUT_SOURCE_TABLE_PREFIX + str(request_id) + '_' + str(run_number)
        temp_input_source_table = main_input_source_table + "_TEMP"
        feed_dedup_temp_table = main_input_source_table + "_FEED_DEDUP_TEMP"
        feed_prioritize_temp_table = main_input_source_table + "_FEED_PRIORITIZE_TEMP"
        dedup_temp_table = main_input_source_table + "_DEDUP_TEMP"

        generalized_sources = []
        for source in sources_loaded:
            input_source_mapping_table_name = source[0]
            input_source_mapping_id = source[2]
            if SUPP_SOURCE_TABLE_PREFIX not in input_source_mapping_table_name:
                generalized_sources.append(
                    f"(select {main_request_details['FilterMatchFields']},do_inputSource,'{input_source_mapping_id}' as do_inputSourceMappingId from {input_source_mapping_table_name}) ")
            else:
                generalized_sources.append(input_source_mapping_table_name)

        main_input_source_query = f"create or replace temporary table" \
                                  f" {SNOWFLAKE_CONFIGS['database']}.{SNOWFLAKE_CONFIGS['schema']}.{temp_input_source_table}" \
                                  f" as select distinct {filter_match_fields} from {f' union select {filter_match_fields} from '.join(generalized_sources)}"
        logger.info(f"Main input source preparation query: {main_input_source_query}")
        sf_cursor.execute(main_input_source_query)
        sf_cursor.execute(f"update {temp_input_source_table} set email_id=lower(trim(email_id))")
        if 'email_md5' not in str(filter_match_fields).lower().split(','):
            sf_cursor.execute(f"alter table {temp_input_source_table} add column email_md5 varchar")
            sf_cursor.execute(f"update {temp_input_source_table} set email_md5 = md5(email_id)")

        sf_cursor.execute(f"alter table {temp_input_source_table} add column if not exists isp varchar")
        sf_cursor.execute(f"update {temp_input_source_table} set isp=split_part(email_id,'@',-1)")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        mysql_cursor.execute(FETCH_GM_CONFIGURED_ISPS)
        gm_configured_isps = mysql_cursor.fetchone()['isps']
        sf_cursor.execute(f"update {temp_input_source_table} set isp = 'Others' where isp not in ({gm_configured_isps})")

        mysql_cursor.execute(FETCH_SUPP_REQUEST_INITIAL_COUNT,(schedule_id,run_number))
        counts_before_filter = mysql_cursor.fetchone()['Total_Count']
        logger.info(f"Deleting old detailed stats, if existent for request_id: {request_id} , run_number: {run_number} . "
                    f"Executing query: {DELETE_SUPPRESSION_MATCH_DETAILED_STATS},({request_id},{run_number}) ")
        mysql_cursor.execute(DELETE_SUPPRESSION_MATCH_DETAILED_STATS, (request_id, run_number))
        logger.info(f"{INSERT_SUPPRESSION_MATCH_DETAILED_STATS,(request_id,schedule_id,run_number,'NA','NA','NA','Initial Count',0,counts_before_filter,0,0)}")
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,(request_id,schedule_id,run_number,'NA','NA','NA','Initial Count',0,counts_before_filter,0,0))

#Removing duplicates based on feed type
        logger.info(f"Performing deduplication based on feed level and request filter level configured "
                    f"removeDuplicates value: {remove_duplicates}")
        sf_cursor.execute(f"create or replace temporary table {feed_dedup_temp_table} like {temp_input_source_table}")
        sf_cursor.execute(f"select LISTAGG(COLUMN_NAME,',') WITHIN GROUP (ORDER BY COLUMN_NAME) from"
                          f" information_schema.COLUMNS where table_name='{temp_input_source_table}'")
        insert_fields = sf_cursor.fetchone()[0]
        sf_cursor.execute(f"select LISTAGG(CONCAT('b.',COLUMN_NAME),',') WITHIN GROUP (ORDER BY COLUMN_NAME) from "
                          f"information_schema.COLUMNS where table_name='{temp_input_source_table}'")
        aliased_insert_fields = sf_cursor.fetchone()[0]
        sf_cursor.execute(f"update {temp_input_source_table} set list_id = '000000'  where list_id is null or "
                          f"cast(list_id as string)='NULL' or cast(list_id as string)='null' or cast(list_id as string)='' ")
        if remove_duplicates == 0:
            source_join_fields = 'and a.do_inputSourceMappingId = b.do_inputSourceMappingId'
            dedup_field = ',do_inputSource'
        else:
            source_join_fields = ''
            dedup_field = ''
        if channel_name == 'GREEN':
            for source in sources_loaded:
                input_source_mapping_id = source[2]
                tp_sf_query = f"merge into {feed_dedup_temp_table} a using (select * from {temp_input_source_table} " \
                              f"where do_inputSourceMappingId = '{input_source_mapping_id}' and list_id not in (select " \
                              f"cast(listid as varchar) from {FP_LISTIDS_SF_TABLE})) b on a.email_id = b.email_id" \
                              f" {source_join_fields} when not matched then insert " \
                              f"({insert_fields}) values ({aliased_insert_fields}) "
                logger.info(f"Executing: {tp_sf_query}")
                sf_cursor.execute(tp_sf_query)
        for source in sources_loaded:
            input_source_mapping_id = source[2]
            if channel_name == 'INFS':
                fp_listid_table = OTEAM_FP_LISTIDS_SF_TABLE
            else:
                fp_listid_table = FP_LISTIDS_SF_TABLE
            fp_sf_query = f"merge into {feed_dedup_temp_table} a using (select * from {temp_input_source_table}" \
                          f" where do_inputSourceMappingId = '{input_source_mapping_id}' and list_id in (select " \
                          f"cast(listid as varchar) from {fp_listid_table})) b on a.email_id = b.email_id" \
                          f" and a.list_id = b.list_id {source_join_fields} when not matched then insert " \
                          f"({insert_fields}) values ({aliased_insert_fields}) "
            logger.info(f"Executing: {fp_sf_query}")
            sf_cursor.execute(fp_sf_query)
            current_table = feed_dedup_temp_table
        if channel_name == 'GREEN' and filter_details['suppressionMethod'] != 'N':
            sf_query = f"create or replace temporary table {feed_prioritize_temp_table} like {temp_input_source_table}"
            logger.info(f"Executing: {sf_query}")
            sf_cursor.execute(sf_query)
            if filter_details['suppressionMethod'] == 'F':
                sf_query = f"insert into {feed_prioritize_temp_table}({insert_fields}) select {insert_fields} from " \
                           f"{current_table} where list_id in (select cast(listid as varchar) from {FP_LISTIDS_SF_TABLE})"
                logger.info(f"Executing: {sf_query}")
                sf_cursor.execute(sf_query)
                sf_query = f"merge into {feed_prioritize_temp_table} a using (select * from {current_table} where " \
                           f"list_id not in (select cast(listid as varchar) from {FP_LISTIDS_SF_TABLE})) b on " \
                           f"a.email_id = b.email_id {source_join_fields} when not matched then insert ({insert_fields})" \
                           f" values ({aliased_insert_fields})"
                logger.info(f"Executing: {sf_query}")
                sf_cursor.execute(sf_query)
            elif filter_details['suppressionMethod'] == 'G':
                sf_query = f"insert into {feed_prioritize_temp_table}({insert_fields}) select {insert_fields} from " \
                           f"{current_table} where list_id not in (select cast(listid as varchar) from {FP_LISTIDS_SF_TABLE})"
                logger.info(f"Executing: {sf_query}")
                sf_cursor.execute(sf_query)
                sf_query = f"merge into {feed_prioritize_temp_table} a using (select * from {current_table} where " \
                           f"list_id in (select cast(listid as varchar) from {FP_LISTIDS_SF_TABLE})) b on " \
                           f"a.email_id = b.email_id {source_join_fields} when not matched then insert ({insert_fields})" \
                           f" values ({aliased_insert_fields})"
                logger.info(f"Executing: {sf_query}")
                sf_cursor.execute(sf_query)
            else:
                logger.info(f"Unknown suppressionMethod : '{filter_details['suppressionMethod']}' .  ")
                raise Exception(f"Unknown suppressionMethod : '{filter_details['suppressionMethod']}' . ")
            current_table = feed_prioritize_temp_table
        sf_query = f"create or replace transient table {dedup_temp_table} like {temp_input_source_table}"
        logger.info(f"Executing: {sf_query}")
        sf_cursor.execute(sf_query)
        sf_query = f"insert into {dedup_temp_table}({insert_fields}) select {insert_fields} from (select {insert_fields}," \
                   f"row_number() over (partition by email_id,list_id{dedup_field} order by profile_id desc) as row_num from {current_table}) where row_num = 1"
        logger.info(f"Executing: {sf_query}")
        sf_cursor.execute(sf_query)
        sf_cursor.execute(f"drop table if exists {main_input_source_table}")
        sf_query = f"alter table {dedup_temp_table} rename to {main_input_source_table} "
        logger.info(f"Executing: {sf_query}")
        sf_cursor.execute(sf_query)

        sf_cursor.execute(f"alter table {main_input_source_table} add column do_suppressionStatus varchar default "
                          f"'CLEAN', do_matchStatus varchar default 'NON_MATCH', "
                          f"do_feedname varchar default 'Third_Party', do_originalInputSource varchar")
        if channel_name == 'INFS':
            sf_cursor.execute(f"alter table {main_input_source_table} add column if not exists account_name varchar")
            sf_cursor.execute(f"UPDATE {main_input_source_table} A SET do_feedname = CONCAT(B.ACCOUNT_NAME,'_',B.LISTID),"
                              f" account_name = B.ACCOUNT_NAME FROM (select ACCOUNT_NAME,cast(LISTID as varchar) as LISTID from "
                              f"{OTEAM_FP_LISTIDS_SF_TABLE}) B WHERE A.LIST_ID=B.LISTID")
        else:
            sf_cursor.execute(f"UPDATE {main_input_source_table} A SET do_feedname = CONCAT(B.CLIENT_NAME,'_',B.LISTID) "
                              f"FROM (select CLIENT_NAME,cast(LISTID as varchar) AS LISTID from {FP_LISTIDS_SF_TABLE}) B"
                              f" WHERE A.LIST_ID=B.LISTID")
        sf_query = f"update {main_input_source_table} set do_originalInputSource=do_inputSource ,do_inputSource = REPLACE(do_inputSource,' ','')"
        logger.info(f"Removing spaces in do_inputSource column values. Executing query: {sf_query}")
        sf_cursor.execute(sf_query)
        sf_cursor.execute(f"select count(1) from {main_input_source_table}")
        counts_after_filter = sf_cursor.fetchone()[0]
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,
                             (request_id, schedule_id, run_number, 'NA', 'Suppression', 'NA'
                              , 'De-dupe Filter', counts_before_filter, counts_after_filter, 0, 0))
        return counts_after_filter, main_input_source_table
    except Exception as e:
        logger.error(f"Exception occurred while creating main input source table. {str(e)} " + str(traceback.format_exc()))
        raise CustomError('DO2',{'error': str(e)})
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.close()
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()

def isps_filtration(current_count, main_request_table, isps, logger, mysql_cursor, main_request_details):
    try:
        counts_before_filter = current_count
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        isps_filter = str(isps).replace(",", "','")
        isps_filtration_query = f"update {main_request_table} set do_suppressionStatus = 'ISP Filter' where " \
                                f"isp not in ('{isps_filter}') and do_suppressionStatus='CLEAN'"
        logger.info(f"Suppressing non-configured isps records in {main_request_table}. Executing Query: {isps_filtration_query}")
        sf_cursor.execute(isps_filtration_query)
        counts_after_filter = counts_before_filter - sf_cursor.rowcount
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,(main_request_details['id'],main_request_details['ScheduleId'],main_request_details['runNumber'],'NA','Suppression','NA'
                                                                      ,'ISP Filter',counts_before_filter,counts_after_filter,0,0))
        return counts_after_filter
    except Exception as e:
        logger.info(f"Exception Occurred: {str(e)}" + str(traceback.format_exc()))
        raise CustomError('DO9', {'error': str(e)})
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()

def profile_non_match_filtration(current_count, main_request_table, logger, mysql_cursor, main_request_details):
    try:
        counts_before_filter = current_count
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        mysql_cursor.execute(FETCH_PROFILE_TABLE_DETAILS,(main_request_details['channelName'],))
        profile_table_details = mysql_cursor.fetchone()
        profile_table = profile_table_details['sfTableName']
        email_field = profile_table_details['emailField']
        listid_field = profile_table_details['listIdField']
        if str(main_request_details['channelName']).upper() != 'INFS':
            listid_cond = ''
            listid_null_cond = ''
        else:
            listid_cond = ''
            listid_null_cond = ''
        profile_non_match_filtration_query = f"update {main_request_table} a set a.do_suppressionStatus = 'Profile Non-match Filter'" \
                                             f" from (select distinct a.email_id from {main_request_table} a left join" \
                                             f" {profile_table} b on a.email_id = lower(trim(b.{email_field})) {listid_cond} where " \
                                             f"b.{email_field} is null {listid_null_cond}) b where a.email_id = b.email_id and do_suppressionStatus = 'CLEAN'"
        logger.info(f"Suppressing profile non-match records in {main_request_table}. Executing"
                    f" Query: {profile_non_match_filtration_query}")
        sf_cursor.execute(profile_non_match_filtration_query)
        counts_after_filter = get_clean_record_count(main_request_table, sf_cursor)
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS, (main_request_details['id'], main_request_details['ScheduleId'],
                                                                       main_request_details['runNumber'], 'NA','Suppression', 'NA',
                                                                       'Profile Non-match Filter', counts_before_filter,
                                                                       counts_after_filter, 0, 0))
        return counts_after_filter
    except Exception as e:
        logger.error(f"Exception occurred while performing profile non-match filtration. {str(e)} " + str(traceback.format_exc()))
        raise CustomError('D1O', {'error': str(e)})
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()

def data_append(main_request_details , filter_details, main_request_table, logger, mysql_cursor):
    if filter_details['appendPostalFields']:
        append_fields('Postal', main_request_details, main_request_table, APPEND_POSTAL_FIELDS_SOURCE, filter_details['postalFields'], POSTAL_MATCH_FIELDS, logger, mysql_cursor)
    if filter_details['appendProfileFields']:
        append_fields('Profile', main_request_details, main_request_table, APPEND_PROFILE_FIELDS_SOURCE, filter_details['profileFields'], PROFILE_MATCH_FIELDS, logger, mysql_cursor)


def append_fields(append_category, main_request_details, main_request_table, fetch_source_query, to_append_columns, match_keys,  logger, mysql_cursor):
    try:
        if to_append_columns == '':
            logger.info("Due to empty list of appending_columns has been returned, skipping append_fields execution")
            return 0
        mysql_cursor.execute(fetch_source_query,(main_request_details['channelName'],))
        source_details = mysql_cursor.fetchone()
        sf_database = source_details['sfDatabase']
        sf_schema = source_details['sfSchema']
        sf_table = source_details['sfTable']
        sf_query = source_details['sfQuery']
        if sf_table is not None and sf_table != 'NULL':
            source_table = f"{sf_database}.{sf_schema}.{sf_table}"
        else:
            source_table = "(" + sf_query + ")"
        logger.info("Acquiring snowflake connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Snowflake connection acquired successfully...")
        logger.info(f"Initiated {append_category} fields appending process")
        alter_fields_list = to_append_columns.split(",")
        sf_alter_table_query = f"alter table {main_request_table} add column if not exists "
        sf_alter_table_query += " varchar , if not exists ".join(i for i in alter_fields_list)
        sf_alter_table_query += " varchar"
        logger.info(f"Executing query: {sf_alter_table_query}")
        sf_cursor.execute(sf_alter_table_query)
        logger.info(f"{main_request_table} altered successfully")
        alter_fields_list = to_append_columns.split(",")
        sf_update_table_query = f'''UPDATE {main_request_table} a set {", ".join([f"a.{field} = b.{field}" for field in alter_fields_list])} from {source_table} b where {" AND ".join([f"a.{'EMAIL_ID' if key == 'EMAIL' else 'EMAIL_MD5' if key == 'MD5HASH' else key} = b.{key}" for key in match_keys.split(",")])} and a.do_suppressionStatus = 'CLEAN'  and a.do_matchStatus != 'NON_MATCH' '''
        logger.info(f"Executing query:  {sf_update_table_query}")
        sf_cursor.execute(sf_update_table_query)
        logger.info(f"{append_category} fields appended successfully...")
    except Exception as e:
        logger.error(f"Exception occurred: Please look into it... {str(e)}"+ str(traceback.format_exc()))
        raise Exception(str(e) + str(traceback.format_exc()))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def channel_adhoc_files_match_and_suppress(type_of_request,filter_details, main_request_details, main_request_table,
                                           mysql_cursor, main_logger, current_count):
    try:
        counts_before_filter = current_count
        if type_of_request == "Match":
            channel_file_type = 'M'
            filter_type = 'Channel_File_Match'
            column_to_update = 'do_matchStatus'
            default_value = 'NON_MATCH'
            is_first_match_filter = True
            error_code = 'DO11'
        if type_of_request == "Suppress":
            channel_file_type = 'S'
            filter_type = 'Channel_File_Suppression'
            column_to_update = 'do_suppressionStatus'
            default_value = 'CLEAN'
            error_code = 'DO12'
        main_logger.info(f"Processing channel level adhoc {type_of_request} files.")
        main_logger.info(f"Acquiring Channel/Offer static files DB mysql connection")
        channel_files_db_conn = mysql.connector.connect(**CHANNEL_OFFER_FILES_DB_CONFIG)
        channel_files_db_cursor = channel_files_db_conn.cursor(dictionary=True)
        main_logger.info(f"Channel/Offer static files DB mysql connection acquired successfully...")
        fetch_channel_adhoc_files = f"select concat('{CHANNEL_OFFER_FILES_SF_SCHEMA}.',TABLE_NAME) as TABLE_NAME," \
                                    f"FILENAME,DOWNLOAD_COUNT,INSERT_COUNT from SUPPRESSION_MATCH_FILES where " \
                                    f"FILE_TYPE='{channel_file_type}' and STATUS='A' and ID in (select FILE_ID " \
                                    f"from OFFER_CHANNEL_SUPPRESSION_MATCH_FILES where " \
                                    f"CHANNEL='{main_request_details['channelName']}' and " \
                                    f"PROCESS_TYPE='C' and STATUS='A')"
        main_logger.info(f"Fetching channel {type_of_request} adhoc files. Executing query: {fetch_channel_adhoc_files} ")
        channel_files_db_cursor.execute(fetch_channel_adhoc_files)
        channel_file_details = channel_files_db_cursor.fetchall()
        channel_files_db_cursor.close()
        channel_files_db_conn.close()
        if channel_file_details is None or len(channel_file_details) == 0:
            main_logger.info(f"No channel {type_of_request} adhoc files were configured. ")
            return counts_before_filter
        main_logger.info(f"Channel {type_of_request} adhoc files retrieved. Files details - {channel_file_details} ")
        main_logger.info("Acquiring snowflake connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        main_logger.info("Snowflake connection acquired successfully...")
        for filter_source in channel_file_details:
            source_table = filter_source['TABLE_NAME']
            filter_name = filter_source['FILENAME']
            download_count = filter_source['DOWNLOAD_COUNT']
            insert_count = filter_source['INSERT_COUNT']
            sf_update_table_query = f"UPDATE {main_request_table} a set a.{column_to_update} = '{filter_name}'" \
                                    f" from {source_table} b where a.EMAIL_MD5=b.md5hash AND a.{column_to_update} = '{default_value}' "
            if type_of_request == "Match":
                sf_update_table_query += " AND a.do_suppressionStatus = 'CLEAN' "
            main_logger.info(f"Executing query: {sf_update_table_query}")
            sf_cursor.execute(sf_update_table_query)
            if type_of_request == "Match":
                if is_first_match_filter:
                    counts_before_filter = 0
                    is_first_match_filter = False
                counts_after_filter = get_record_count(main_request_table, sf_cursor)
            if type_of_request == "Suppress":
                counts_after_filter = get_clean_record_count(main_request_table, sf_cursor)
            mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,
                                 (main_request_details['id'], main_request_details['ScheduleId'],
                                  main_request_details['runNumber'], 'NA', filter_type, 'NA', filter_name,
                                  counts_before_filter, counts_after_filter, 0, 0))
            counts_before_filter = counts_after_filter
        if type_of_request == "Match":
            sf_update_table_query = f"UPDATE {main_request_table} set do_suppressionStatus = 'Channel_file_match_filtered'" \
                                    f" where {column_to_update} = '{default_value}' AND do_suppressionStatus = 'CLEAN' "
            main_logger.info(f"Updating channel level non-matching records as 'Channel_file_match_filtered' in "
                             f"do_suppressionStatus column. Executing query: {sf_update_table_query} ")
            sf_cursor.execute(sf_update_table_query)
            sf_update_table_query = f" UPDATE {main_request_table} set do_matchStatus='NON_MATCH'"
            main_logger.info(f"Updating do_matchStatus again as NON_MATCH in request table {main_request_table}. Executing query: {sf_update_table_query} ")
            sf_cursor.execute(sf_update_table_query)
        main_logger.info(f"Channel level {type_of_request} adhoc files are processed successfully...")
        return counts_after_filter
    except Exception as e:
        main_logger.error(f"Exception occurred while processing channel level {type_of_request} adhoc files. Please "
                          f"look into this. {str(e)}" + str(traceback.format_exc()))
        raise CustomError(error_code, {'error': str(e)})
    finally:
        if 'connection' in locals() and channel_files_db_conn.is_connected():
            channel_files_db_cursor.close()
            channel_files_db_conn.close()
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()

def jornaya_and_mockingbird_match(category, current_count, main_request_table, logger, mysql_cursor, main_request_details, category_match_details):
    try:
        counts_before_filter = current_count
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        if category == 'Jornaya':
            source_table = JORNAYA_TABLE
            filter_name = 'Jornaya Match'
            match_field = 'do_jornayaMatch'
            match_value = 'JORNAYA_MATCH'
            non_match_value = 'JORNAYA_NON_MATCH'
        elif category == 'Mockingbird':
            source_table = MOCKINGBIRD_TABLE
            filter_name = 'Mockingbird Match'
            match_field = 'do_mockingbirdMatch'
            match_value = 'MB_MATCH'
            non_match_value = 'MB_NON_MATCH'

        if category_match_details['searchType'] == 'between':
            if category_match_details['dateRangeType'] == 'C':
                category_match_details['value'] = "'" + category_match_details['value'] + "'"
                category_match_details['value'] = category_match_details['value'].replace(',', '\' and \'')
            elif category_match_details['dateRangeType'] == 'R':
                category_match_details['value'] = f"current_date() - interval '{str(category_match_details['value']).split(',')[0]} days'" \
                                                  f" and current_date() - interval '{str(category_match_details['value']).split(',')[1]} days'"
        elif category_match_details['searchType'] == '>=':
            category_match_details['searchType'] = 'between'
            if category_match_details['value'] == 'T':
                category_match_details['value'] = f"'2019-01-01' and current_date()"
            else:
                category_match_details['value'] = f"current_date() - interval '{category_match_details['value']} days' and current_date()"

        alter_query = f"alter table {main_request_table} add column {match_field} varchar default '{non_match_value}'"
        logger.info(f"Executing query: {alter_query}")
        sf_cursor.execute(alter_query)
        sf_query = f"update {main_request_table} a set {match_field} = '{match_value}' from {source_table} b where " \
                   f"a.EMAIL_MD5 = b.EMAIL_MD5 and a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH' and" \
                   f"b.LAST_ACTION_DATE {category_match_details['searchType']} {category_match_details['value']}"
        logger.info(f"Executing query: {sf_query}")
        sf_cursor.execute(sf_query)

        counts_after_filter = get_clean_record_count(main_request_table, sf_cursor)
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS, (main_request_details['id'], main_request_details['ScheduleId'],
                                                                       main_request_details['runNumber'], 'NA','Match', 'NA',
                                                                       filter_name, counts_before_filter,
                                                                       counts_after_filter, 0, 0))


        if category_match_details['nonMatchData']:
            logger.info(f"Opted to consider {category} non-matched data as well")
            return counts_before_filter

        sf_query = f"update {main_request_table} set do_suppressionStatus = '{filter_name}' " \
                   f"where {match_field} = '{non_match_value}' and do_suppressionStatus = 'CLEAN'"
        logger.info(f"Suppressing {category} non-match records in {main_request_table}. Executing"
                    f" Query: {sf_query}")
        sf_cursor.execute(sf_query)

        counts_after_filter = get_clean_record_count(main_request_table, sf_cursor)
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS, (main_request_details['id'], main_request_details['ScheduleId'],
                                                                       main_request_details['runNumber'], 'NA','Match', 'NA',
                                                                       filter_name, counts_before_filter,
                                                                       counts_after_filter, 0, 0))
        return counts_after_filter
    except Exception as e:
        logger.error(f"Exception occurred while performing {category} non-match filtration. {str(e)} " + str(traceback.format_exc()))
        raise Exception(
            f"Exception occurred while performing {category} non-match filtration. {str(e)} " + str(traceback.format_exc()))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def update_default_values(type_of_request, main_request_table, logger):
    try:
        if type_of_request == "Match":
            column_to_update = 'do_matchStatus'
            value_to_set = 'MATCH'
            where_cond_column = 'do_suppressionStatus'
            where_cond_column_value = 'CLEAN'
        if type_of_request == "Suppression":
            column_to_update = 'do_suppressionStatus'
            value_to_set = 'CLEAN'
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Snowflake connection acquired successfully...")
        sf_update_table_query = f"UPDATE {main_request_table} set {column_to_update} = '{value_to_set}' where " \
                                f"{where_cond_column} = '{where_cond_column_value}' "
        logger.info(f"Since, no {type_of_request} sources are choosen, updating {column_to_update} column values. "
                    f"Executing query:  {sf_update_table_query}")
        sf_cursor.execute(sf_update_table_query)
        return main_request_table
    except Exception as e:
        logger.error(f"Exception occurred: Please look into it... {str(e)}"+ str(traceback.format_exc()))
        raise Exception(str(e) + str(traceback.format_exc()))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def perform_match_or_filter_selection(type_of_request,filter_details, main_request_details,
                                      main_request_table, mysql_cursor, main_logger, current_count):
    if type_of_request == "Match":
        key_to_fetch = 'matchedDataSources'
    if type_of_request == "Suppression":
        key_to_fetch = 'filterDataSources'
    if filter_details[key_to_fetch] is not None:
        match_or_filter_source_details = json.loads(str(filter_details[key_to_fetch]).strip('"').replace("'", '"'))
    else:
        match_or_filter_source_details = {}
    match_or_filter_sources = []
    if len(match_or_filter_source_details) == 0:
    #if match_or_filter_source_details['DataSource'] == [] and match_or_filter_source_details['ByField'] == []:
        main_logger.info(f"No {type_of_request} filter sources are chosen.")
        if type_of_request == "Match":
            update_default_values(type_of_request, main_request_table, main_logger)
        return current_count
    else:
        if 'FileSource' in match_or_filter_source_details.keys():
            file_source_filter_list = list(match_or_filter_source_details['FileSource'])
            match_or_filter_sources = load_match_or_filter_file_source(type_of_request,file_source_filter_list,main_request_details,main_logger)

        if 'DataSource' in match_or_filter_source_details.keys():
            data_source_filter_list = list(match_or_filter_source_details['DataSource'])
            for i in data_source_filter_list:
                data_source_details_dict = json.loads(str(i).strip('"').replace("'", '"'))
                data_source_table_name = data_source_input(type_of_request, data_source_details_dict['dataSourceId'], mysql_cursor, main_logger)
                columns = data_source_details_dict['columns']
                mysql_cursor.execute(FETCH_DATASET_NAME, (data_source_details_dict['dataSourceId'],))
                data_source_name = 'Dataset ' + str(mysql_cursor.fetchone()['name'])
                match_or_filter_sources.append(tuple([data_source_table_name, columns, data_source_name, 'DataSource']))
        if 'ByField' in match_or_filter_source_details.keys():
            for filter in list(match_or_filter_source_details['ByField']):
                by_field_desc = f"By Attribute: {filter['fieldName']} {filter['searchType']} {filter['value']} ".replace("'",'"')
                match_or_filter_sources.append(tuple(['', filter, by_field_desc, 'ByField']))
    current_count = perform_filter_or_match(type_of_request, main_request_details, main_request_table,
                                            match_or_filter_sources, mysql_cursor, main_logger, current_count)
    main_logger.info(f"All {type_of_request} filter sources are successfully processed.")
    return current_count


def perform_filter_or_match(type_of_request, main_request_details, main_request_table, sorted_filter_sources_loaded ,
                            mysql_cursor, logger, current_count):
    try:
        logger.info(f"Function perform_filter_or_match invoked for {type_of_request}: Sorted Sources Loaded are: {sorted_filter_sources_loaded} ")
        counts_before_filter = current_count
        is_first_match_filter = True
        if type_of_request == "Match":
            column_to_update = 'do_matchStatus'
            default_value = 'NON_MATCH'
            error_code = 'DO13'
        if type_of_request == "Suppression":
            column_to_update = 'do_suppressionStatus'
            default_value = 'CLEAN'
            error_code = 'DO14'
        logger.info(f"perform_filter_or_match method for {type_of_request} invoked..")
        logger.info("Acquiring snowflake connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Snowflake connection acquired successfully...")
        for filter_source in sorted_filter_sources_loaded:
            filter_name = filter_source[2]
            if filter_source[3] == 'ByField':
                filter = filter_source[1]
                if filter['dataType'] == 'String' and filter['searchType'] in ('like', 'not like'):
                    filter['value'] = f"%{filter['value']}%"
                if filter['dataType'] != 'Number' and filter['searchType'] != '>=':
                    filter['value'] = "'" + filter['value'] + "'"
                if filter['searchType'] in ('in', 'not in') and filter['dataType'] == 'Number':
                    filter['value'] = "(" + filter['value'] + ")"
                elif filter['searchType'] in ('in', 'not in') and filter['dataType'] != 'Number':
                    filter['value'] = "(" + filter['value'].replace(',', '\',\'') + ")"
                if filter['searchType'] == 'between' and filter['dataType'] != 'Number':
                    filter['value'] = filter['value'].replace(',', '\' and \'')
                elif filter['searchType'] == 'between' and filter['dataType'] == 'Number':
                    filter['value'] = filter['value'].replace(',', ' and ')
                if filter['searchType'] == '>=':
                    filter['searchType'] = 'between'
                    if filter['value'] == 'T':
                        filter['value'] = f"'2019-01-01' and current_date()"
                    else:
                        filter['value'] = f"current_date() - interval '{filter['value']} days' and current_date()"

                sf_update_table_query = f"UPDATE {main_request_table}  a  SET  a.{column_to_update} ='{filter_name}'" \
                                        f" WHERE {filter['fieldName']} {filter['searchType']} {filter['value']} "
            else:
                match_fields = filter_source[1].split(",")
                source_table = filter_source[0]
                sf_update_table_query = f"UPDATE {main_request_table}  a  SET  a.{column_to_update} = '{filter_name}: {source_table}' FROM {source_table} b WHERE "
                sf_update_table_query += " AND ".join([f"a.{key} = b.{key}" for key in match_fields])
            sf_update_table_query += f" AND a.{column_to_update} = '{default_value}' "
            if type_of_request == "Match":
                sf_update_table_query += f" AND a.do_suppressionStatus = 'CLEAN' "
            if type_of_request == "Suppress":
                sf_update_table_query += f" AND a.do_suppressionStatus != 'NON_MATCH' "
            logger.info(f"Executing query: {sf_update_table_query}")
            sf_cursor.execute(sf_update_table_query)
            if type_of_request == "Match":
                if is_first_match_filter:
                    counts_before_filter = 0
                    is_first_match_filter = False
            counts_after_filter = get_record_count(main_request_table, sf_cursor)
            mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,
                                 (main_request_details['id'], main_request_details['ScheduleId'],
                                  main_request_details['runNumber'], 'NA', type_of_request, 'NA', filter_name,
                                  counts_before_filter, counts_after_filter, 0, 0))
            counts_before_filter = counts_after_filter
            logger.info(f"perform_filter_or_match method for {type_of_request} executed successfully...")
        return counts_after_filter
    except Exception as e:
        logger.error(f"Exception occurred at perform_filter_or_match method for {type_of_request}: Please look into this. {str(e)}" + str(traceback.format_exc()))
        raise CustomError(error_code, {'error': str(e)})
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()

def load_match_or_filter_file_source(type_of_request,file_source_filter_list,main_request_details,logger):
    logger.info("load_match_or_filter_file_source method invoked....")
    result = []
    for i in range(len(file_source_filter_list)):
        file_source_index = i
        file_source = file_source_filter_list[i]
        file_source_type_id = file_source['sourceId']
        os.makedirs(f"{SUPP_LOG_PATH}/{str(main_request_details['id'])}/{type_of_request}/{str(file_source['sourceId'])}_{str(file_source_index)}/",exist_ok=True)
        os.makedirs(f"{SUPP_FILE_PATH}/{str(main_request_details['id'])}/{type_of_request}/{str(file_source['sourceId'])}_{str(file_source_index)}/",exist_ok=True)
        temp_files_path = f"{SUPP_FILE_PATH}/{str(main_request_details['id'])}/{type_of_request}/{str(file_source['sourceId'])}_{str(file_source_index)}/"
        source_table = f"{MAIN_INPUT_SOURCE_TABLE_PREFIX}{type_of_request}_{str(main_request_details['id'])}_{str(file_source['sourceId'])}_{str(file_source_index)}"
        consumer_logger = create_logger(base_logger_name=f"{type_of_request}_{file_source['sourceId']}_{str(file_source_index)}",log_file_path=f"{SUPP_LOG_PATH}/{str(main_request_details['id'])}/{type_of_request}/{str(file_source['sourceId'])}_{str(file_source_index)}/",log_to_stdout=False)
        logger.info(f"Acquiring mysql connection...")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        logger.info(f"Fetch source_type for the request id: {file_source_type_id}")
        logger.info(f"Executing query: {FETCH_FILTER_FILE_SOURCE_INFO, (file_source_type_id,)}")
        mysql_cursor.execute(FETCH_FILTER_FILE_SOURCE_INFO, (file_source_type_id,))
        file_source_details = mysql_cursor.fetchone()
        hostname = file_source_details['hostname']
        port = file_source_details['port']
        username = file_source_details['username']
        password = file_source_details['password']
        source_type = file_source_details['sourceType']
        source_sub_type = file_source_details['sourceSubType']
        input_data_dict = {'filePath': file_source['filePath'], 'delimiter': file_source['delimiter'],
                           'headerValue': file_source['headerValue'], 'isHeaderExists': file_source['isHeaderExists']}
        request_id = main_request_details['id']
        #run_number = main_request_details['runNumber']
        schedule_id = main_request_details['ScheduleId']
        source_table = process_file_type_request(type_of_request,request_id, source_table, 1, schedule_id, source_sub_type,input_data_dict,mysql_cursor, consumer_logger, "", temp_files_path, main_request_details, hostname,port, username, password)
        result.append(tuple([source_table, file_source['columns'], f"File {file_source['filePath']}", 'FileSource']))
    return result

def validate_remaining_data(main_request_details, main_request_table, mysql_cursor, logger, current_count):
    try:
        counts_before_filter = current_count
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        sf_query = f"update {main_request_table} set do_matchStatus = 'Remaining_Data' where do_matchStatus='NON_MATCH'" \
                   f" and do_suppressionStatus='CLEAN'"
        logger.info(f"Validating remaining non-matched data. Executing query: {sf_query}")
        sf_cursor.execute(sf_query)
        counts_after_filter = get_record_count(main_request_table, sf_cursor)
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,
                             (main_request_details['id'], main_request_details['ScheduleId'],
                              main_request_details['runNumber'], 'NA', 'Match', 'NA', 'Non-Matched data',
                              counts_before_filter, counts_after_filter, 0, 0))
        logger.info(f"Successfully validated remaining non-matched data.")
        return counts_after_filter
    except Exception as e:
        logger.error(f"Exception occurred while validating remaining data. Please look into this. {str(e)}" + str(traceback.format_exc()))
        raise Exception(f"Exception occurred while validating remaining data. Please look into this. {str(e)}" + str(traceback.format_exc()))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()

def offer_download_and_suppression(offer_id, main_request_details, filter_details, main_request_table, current_count,
                                   affiliate_channel, offer_table_prefix):
    try:
        request_id = main_request_details['id']
        schedule_id = main_request_details['ScheduleId']
        run_number = main_request_details['runNumber']
        channel = main_request_details['channelName']
        request_offer_log_path = f"{SUPP_LOG_PATH}/{str(request_id)}/{str(run_number)}"
        os.makedirs(f"{request_offer_log_path}", exist_ok=True)
        offer_logger = create_logger(f"offer_{str(request_id)}_{str(run_number)}_{str(offer_id)}",
                                    log_file_path=f"{request_offer_log_path}/",
                                    log_to_stdout=False)
        offer_logger.info(f"Processing started for offerid: {offer_id}")
        offer_logger.info(f"Acquiring mysql connection")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        offer_logger.info(f"Mysql connection acquired successfully...")
        offer_logger.info(f"Inserting offer: {offer_id} into {SUPPRESSION_REQUEST_OFFERS_TABLE} Table. ")
        mysql_cursor.execute(INSERT_REQUEST_OFFERS,(request_id, schedule_id, run_number, offer_id))
        offer_script_exe = f'{OFFER_PROCESSING_SCRIPT} "{request_id}" "{offer_id}" "{affiliate_channel}" "pid" "DATAOPS" "{schedule_id}" "{run_number}">>{request_offer_log_path}/{offer_id}.log 2>>{request_offer_log_path}/{offer_id}.log'
        exit_code = os.system(offer_script_exe)
        if exit_code == 0:
            offer_logger.info(f"Offer downloading process got completed for offerid: {offer_id}")
        else:
            offer_logger.info(f"Error occurred during offer downloading process for offerid: {offer_id}")
            return -1
        offer_logger.info(f"Suppression process started for offerid: {offer_id}")
        offer_logger.info("Acquiring snowflake connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        offer_logger.info("Snowflake connection acquired successfully...")
        sf_cursor.execute(f"alter table {main_request_table} add column do_matchStatus_{offer_id} varchar default "
                          f"'NON_MATCH', do_suppressionStatus_{offer_id} varchar default 'CLEAN' ")
        offer_logger.info(f"Acquiring Channel/Offer static files DB mysql connection")
        offer_files_db_conn = mysql.connector.connect(**CHANNEL_OFFER_FILES_DB_CONFIG)
        offer_files_db_cursor = offer_files_db_conn.cursor(dictionary=True)
        offer_logger.info(f"Channel/Offer static files DB mysql connection acquired successfully...")
        offer_files_db_cursor.execute(f"select group_concat(SUB_OFFER_ID) as sub_offers_list from OFFER_SUBOFFERS "
                                      f"where CHANNEL='{affiliate_channel}' and OFFER_ID={offer_id} and STATUS='A'")
        sub_offers_list = offer_files_db_cursor.fetchone()['sub_offers_list']
        if sub_offers_list is not None:
            offers_list = f'{offer_id},{sub_offers_list}'
        else:
            offers_list = offer_id

        # Offer file match or suppression
        def file_match_or_supp(type, tables_list, current_count):
            counts_before_filter = current_count
            if type == 'Match':
                is_first_file = True
                column_to_update = f'do_matchStatus_{offer_id}'
                default_value = 'NON_MATCH'
            elif type == 'Suppression':
                column_to_update = f'do_suppressionStatus_{offer_id}'
                default_value = 'CLEAN'
            for table in tables_list:
                associate_offer_id = table['OFFER_ID']
                static_file_table = table['TABLE_NAME']
                static_file_name = table['FILENAME']
                download_count = table['DOWNLOAD_COUNT']
                insert_count = table['INSERT_COUNT']

                sf_update_table_query = f"update {main_request_table} a set {column_to_update} = '{static_file_table}' " \
                                        f"from {CHANNEL_OFFER_FILES_SF_SCHEMA}.{static_file_table} b where" \
                                        f" a.EMAIL_MD5 = b.md5hash and do_matchStatus != 'NON_MATCH' and " \
                                        f"do_suppressionStatus = 'CLEAN' and {column_to_update} = '{default_value}'"
                if type == "Suppression":
                    sf_update_table_query += f" AND a.do_matchStatus_{offer_id} != 'NON_MATCH' "
                offer_logger.info(f"Executing query:  {sf_update_table_query}")
                sf_cursor.execute(sf_update_table_query)
                if type == 'Match':
                    if is_first_file:
                        counts_before_filter = 0
                        is_first_file = False
                counts_after_filter = get_offer_record_count(main_request_table, sf_cursor, offer_id)
                if str(offer_id) == str(associate_offer_id):
                    filter_type = f"MainOffer_File_{type}"
                else:
                    filter_type = f"SubOffer_File_{type}"
                offer_logger.info(f"Executing: {INSERT_SUPPRESSION_MATCH_DETAILED_STATS,(request_id, schedule_id, run_number, offer_id, f'{filter_type}', associate_offer_id,f'{static_file_name}', counts_before_filter, counts_after_filter, download_count,insert_count)}")
                mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,
                                     (request_id, schedule_id, run_number, offer_id, f'{filter_type}', associate_offer_id,
                                      f'{static_file_name}', counts_before_filter, counts_after_filter, download_count,
                                      insert_count))
                counts_before_filter = counts_after_filter
            return counts_after_filter

        if filter_details['applyOfferFileMatch']:
            offer_files_db_cursor.execute(f"select B.OFFER_ID,A.TABLE_NAME,A.FILENAME,A.DOWNLOAD_COUNT,A.INSERT_COUNT from "
                                          f"SUPPRESSION_MATCH_FILES A INNER JOIN OFFER_CHANNEL_SUPPRESSION_MATCH_FILES B ON"
                                          f" A.ID=B.FILE_ID where B.CHANNEL='{channel}' and B.OFFER_ID in ({offers_list}) "
                                          f"and B.PROCESS_TYPE='O' and B.STATUS='A' AND A.FILE_TYPE='M' and  A.STATUS='A'")
            match_tables_list = offer_files_db_cursor.fetchall()
            if match_tables_list is not None and len(match_tables_list) != 0:
                current_count = file_match_or_supp('Match', match_tables_list, current_count)
            else:
                sf_cursor.execute(f"update {main_request_table} set do_matchStatus_{offer_id} = 'MATCH' where"
                                  f" do_matchStatus != 'NON_MATCH' and do_suppressionStatus = 'CLEAN'")
                offer_logger.info(f"Offer adhoc match files are not configured")
        else:
            sf_cursor.execute(f"update {main_request_table} set do_matchStatus_{offer_id} = 'MATCH' where"
                              f" do_matchStatus != 'NON_MATCH' and do_suppressionStatus = 'CLEAN'")

        if filter_details['applyOfferFileSuppression']:
            offer_files_db_cursor.execute(f"select B.OFFER_ID,A.TABLE_NAME,A.FILENAME,A.DOWNLOAD_COUNT,A.INSERT_COUNT from"
                                          f" SUPPRESSION_MATCH_FILES A INNER JOIN OFFER_CHANNEL_SUPPRESSION_MATCH_FILES B "
                                          f"ON A.ID=B.FILE_ID where B.CHANNEL='{channel}' and B.OFFER_ID in ({offers_list})"
                                          f" and B.PROCESS_TYPE='O' and B.STATUS='A' AND A.FILE_TYPE='S' and  A.STATUS='A'")
            supp_tables_list = offer_files_db_cursor.fetchall()
            if supp_tables_list is not None and len(supp_tables_list) != 0:
                current_count = file_match_or_supp('Suppression', supp_tables_list, current_count)
            else:
                offer_logger.info(f"Offer adhoc supp files are not configured")

        # Cake suppression
        def cake_supp(filter_type, associate_offer_id, supp_table, current_count):
            counts_before_filter = current_count
            sf_update_table_query = f"update {main_request_table} a set do_suppressionStatus_{offer_id} = '{filter_type}' " \
                                    f"from {OFFER_SUPP_TABLES_SF_SCHEMA}.{supp_table} b where a.EMAIL_MD5 = b.md5hash " \
                                    f"and do_matchStatus != 'NON_MATCH' and do_suppressionStatus = 'CLEAN' and " \
                                    f"a.do_matchStatus_{offer_id} != 'NON_MATCH' and do_suppressionStatus_{offer_id} = 'CLEAN'"
            offer_logger.info(f"Executing query:  {sf_update_table_query}")
            sf_cursor.execute(sf_update_table_query)
            counts_after_filter = get_offer_record_count(main_request_table, sf_cursor, offer_id)
            mysql_cursor.execute(f"update {SUPPRESSION_MATCH_DETAILED_STATS_TABLE} set filterType='{filter_type}',"
                                 f"filterName='{associate_offer_id}',countsBeforeFilter={counts_before_filter}"
                                 f",countsAfterFilter={counts_after_filter} where requestId={request_id} and "
                                 f"offerId={offer_id} and associateOfferId={associate_offer_id} and "
                                 f"filterType='TEMPORARY' and runNumber = {run_number}")
            return counts_after_filter

        current_count = cake_supp("MainOffer_Cake_Suppression", offer_id, f"{affiliate_channel}_{offer_table_prefix}_{offer_id}", current_count)

        #Conversions suppression

        if str(channel).upper() != 'INFS':
            def conversions_supp(filter_type, associate_offer_id, current_count):
                counts_before_filter = current_count
                if str(channel).upper() == 'GREEN':
                    sf_update_table_query = f"update {main_request_table} a set do_suppressionStatus_{offer_id} = '{filter_type}' " \
                                            f"from (select profileid from {CAKE_CONVERTION_TABLES_SF_SCHEMA}.BUYER_CONVERSIONS_SF where " \
                                            f"offer_id='{associate_offer_id}' and CONVERSIONDATE>=current_date() - interval '6 months') " \
                                            f"b where a.profile_id=b.profileid and do_matchStatus != 'NON_MATCH' and " \
                                            f"do_suppressionStatus = 'CLEAN' and a.do_matchStatus_{offer_id} != 'NON_MATCH' and" \
                                            f" do_suppressionStatus_{offer_id} = 'CLEAN'"
                elif str(channel).upper() == 'APPTNESS':
                    sf_update_table_query = f"update {main_request_table} a set do_suppressionStatus_{offer_id} = '{filter_type}' " \
                                            f"from (select profileid from {CAKE_CONVERTION_TABLES_SF_SCHEMA}.BUYER_CONVERSIONS_SF where " \
                                            f"offer_id='{associate_offer_id}' and upper(channel)='APPTNESS' and CONVERSIONDATE>=current_date() - interval '6 months') " \
                                            f"b where cast(a.profile_id as varchar)=b.profileid and do_matchStatus != 'NON_MATCH' and " \
                                            f"do_suppressionStatus = 'CLEAN' and a.do_matchStatus_{offer_id} != 'NON_MATCH' and" \
                                            f" do_suppressionStatus_{offer_id} = 'CLEAN'"
                offer_logger.info(f"Executing query:  {sf_update_table_query}")
                sf_cursor.execute(sf_update_table_query)
                counts_after_filter = get_offer_record_count(main_request_table, sf_cursor, offer_id)
                offer_logger.info(f"Executing: {INSERT_SUPPRESSION_MATCH_DETAILED_STATS,(request_id, schedule_id, run_number, offer_id, f'{filter_type}', associate_offer_id,f'{associate_offer_id}', counts_before_filter, counts_after_filter, 0, 0)}")
                mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,
                                     (request_id, schedule_id, run_number, offer_id, f'{filter_type}', associate_offer_id,
                                      f'{associate_offer_id}', counts_before_filter, counts_after_filter, 0, 0))
                return counts_after_filter

            current_count = conversions_supp("MainOffer_Cake_Converters", offer_id, current_count)

        #Sub offers suppression

        if sub_offers_list is not None:
            for sub_offer_id in sub_offers_list.split(','):
                current_count = cake_supp("SubOffer_Cake_Suppression",sub_offer_id, f"{affiliate_channel}_{offer_table_prefix}_{sub_offer_id}", current_count)
                if str(channel).upper() != 'INFS':
                    current_count = conversions_supp("SubOffer_Cake_Converters", sub_offer_id, current_count)

        offer_logger.info(f"Suppression process ended for offerid: {offer_id}")
    except Exception as e:
        offer_logger.info(f"Exception occurred: At offer_download_and_suppression for requestid: {request_id}, "
                         f"runNumber: {run_number}, offerid: {offer_id}. Please look into this. {str(e)}" + str(traceback.format_exc()))
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        sf_cursor.execute(f"alter table {main_request_table} drop column do_matchStatus_{offer_id} , do_suppressionStatus_{offer_id} ")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        mysql_cursor.execute(f"UPDATE {SUPPRESSION_REQUEST_OFFERS_TABLE} SET STATUS='E' where requestId={request_id} and requestScheduledId={schedule_id} and runNumber={run_number} and offerId={offer_id} ")
    finally:
        if 'connection' in locals() and offer_files_db_conn.is_connected():
            offer_files_db_cursor.close()
            offer_files_db_conn.close()
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.close()


# adding code for suppression methods
def get_record_count(table, sf_cursor):
    sf_cursor.execute(f"select count(1) from {table} where do_suppressionStatus = 'CLEAN'  and do_matchStatus != 'NON_MATCH'")
    return sf_cursor.fetchone()[0]

def get_clean_record_count(table, sf_cursor):
    sf_cursor.execute(f"select count(1) from {table} where do_suppressionStatus = 'CLEAN' ")
    return sf_cursor.fetchone()[0]
def get_offer_record_count(table, sf_cursor, offerid):
    sf_cursor.execute(f"select count(1) from {table} where do_suppressionStatus = 'CLEAN'  and do_matchStatus != "
                      f"'NON_MATCH' and do_suppressionStatus_{offerid} = 'CLEAN'  and do_matchStatus_{offerid} != 'NON_MATCH'")
    return sf_cursor.fetchone()[0]

def apply_green_global_suppression(source_table, result_breakdown_flag, logger):
    try:
        result = []
        logger.info(f"Applying Green Global Suppression for the given table {source_table}")
        logger.info("Acquiring snowflake Connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Acquired snowflake connection successfully")
        logger.info("Invalid Emails Suppression starts... ")
        res = {}
        res['offerId'], res['filterType'], res['associateOfferId'], res['downloadCount'], res['insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
        res['filterName'] = "Invalid Email addresses"
        res['countsBeforeFilter'] = get_record_count(source_table, sf_cursor)
        sf_update_table_query = f"UPDATE {source_table} a SET a.do_suppressionStatus =  CASE WHEN REGEXP_LIKE(a.EMAIL_ID, '{VALID_EMAILS_FORMAT}') THEN 'CLEAN' else 'Invalid Email addresses' END WHERE a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH' "
        logger.info(f"Executing query:  {sf_update_table_query}")
        sf_cursor.execute(sf_update_table_query)
        res['countsAfterFilter'] = get_record_count(source_table, sf_cursor)
        result.append(res)
        for supp_dict in GREEN_GLOBAL_SUPP_TABLES:
            supp_key = tuple(supp_dict.keys())[0]
            supp_tables = supp_dict[supp_key]
            res = {}
            res['offerId'], res['filterType'], res['associateOfferId'], res['downloadCount'], res[
                'insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
            res['filterName'] = supp_key
            res['countsBeforeFilter'] = get_record_count(source_table, sf_cursor)
            for supp_table in supp_tables:
                sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{supp_table}' FROM ({supp_table}) b WHERE  lower(trim(a.EMAIL_ID)) = lower(trim(b.email)) AND a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH' "
                logger.info(f"Executing query:  {sf_update_table_query}")
                sf_cursor.execute(sf_update_table_query)
                logger.info(f"{supp_table} suppression done successfully...")
            res['countsAfterFilter'] = get_record_count(source_table, sf_cursor)
            result.append(res)
            logger.info(f"{supp_key} suppression done successfully...")
        current_count = get_record_count(f"{source_table}", sf_cursor)
        logger.info(f"the result breakdown flag is : {result_breakdown_flag}")
        if not result_breakdown_flag:
            single_res = {}
            single_res['offerId'], single_res['filterType'], single_res['associateOfferId'], single_res[
                'downloadCount'], single_res['insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
            single_res['filterName'] = 'Green Global Suppression'
            single_res['countsBeforeFilter'] = result[0]['countsBeforeFilter']
            single_res['countsAfterFilter'] = result[-1]['countsAfterFilter']
            return True, [single_res], current_count
        else:
            return True, result, current_count
    except Exception as e:
        logger.error(f"Exception occurred: Please look into it... {str(e)}")
        return False, str(e), 0
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def apply_green_feed_level_suppression(source_table, result_breakdown_flag, logger):
    try:
        result = []
        logger.info(f"Applying Green Feed Level Suppression for the given table {source_table}")
        logger.info("Acquiring snowflake Connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Acquired snowflake connection successfully")
        for supp_dict in GREEN_FEED_LEVEL_SUPP_TABLES['email']:
            supp_key = list(supp_dict.keys())[0]
            supp_tables = supp_dict[supp_key]
            res = {}
            res['offerId'], res['filterType'], res['associateOfferId'], res['downloadCount'], res[
                'insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
            res['filterName'] = supp_key
            res['countsBeforeFilter'] = get_record_count(source_table, sf_cursor)
            for supp_table in supp_tables:
                sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{supp_key}' FROM ({supp_table}) b WHERE  lower(trim(a.EMAIL_ID)) = lower(trim(b.email)) AND a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH' "
                logger.info(f"Executing query:  {sf_update_table_query}")
                sf_cursor.execute(sf_update_table_query)
                logger.info(f"{supp_table} suppression done successfully...")
            res['countsAfterFilter'] = get_record_count(source_table, sf_cursor)
            result.append(res)
            logger.info(f"{supp_key} suppression done successfully...")
        # GREEN GLOBAL SUPPRESSION
        res = {}
        res['offerId'], res['filterType'], res['associateOfferId'], res['downloadCount'], res[
            'insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
        res['countsBeforeFilter'] = get_record_count(source_table, sf_cursor)
        res['filterName'] = "Green Global Suppression"
        sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = 'Green Global Suppression' FROM (select email from GREEN_LPT.ORIGIN_UNIVERSE_UNSUBS union all select email from GREEN_LPT.APT_CUSTOM_Datatonomy_SUPPRESSION_DND union all select email from GREEN_LPT.APT_CUSTDOD_ORANGE_EOS_RETURNS_INAVLID_EMAILS union all select email from GREEN_LPT.PFM_UNIVERSE_UNSUBS union all select email from GREEN_LPT.APT_CUSTDOD_NONUS_DATA_PROFILE union all select email from GREEN_LPT.GREEN_UNSUBS) b WHERE  lower(trim(a.EMAIL_ID)) = lower(trim(b.email)) AND a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH' and a.LIST_ID not in (select listid from GREEN_LPT.PFM_FLUENT_REGISTRATIONS_LOOKUP_DONOTDROP_RT where RULE in (2,3))"
        logger.info(f"Executing query: {sf_update_table_query}")
        sf_cursor.execute(sf_update_table_query)
        logger.info(f"Green Global Suppression done successfully...")
        res['countsAfterFilter'] = get_record_count(source_table, sf_cursor)
        result.append(res)
        current_count = get_record_count(f"{source_table}", sf_cursor)
        for supp_dict in GREEN_FEED_LEVEL_SUPP_TABLES['email_listid']:
            supp_key = list(supp_dict.keys())[0]
            supp_tables = supp_dict[supp_key]
            res = {}
            res['offerId'], res['filterType'], res['associateOfferId'], res['downloadCount'], res[
                'insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
            res['countsBeforeFilter'] = get_record_count(source_table, sf_cursor)
            res['filterName'] = supp_key
            for supp_table in supp_tables:
                sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{supp_key}' FROM ({supp_table}) b WHERE  lower(trim(a.EMAIL_ID)) = lower(trim(b.email)) AND a.LIST_ID = b.listid AND a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'"
                logger.info(f"Executing query:  {sf_update_table_query}")
                sf_cursor.execute(sf_update_table_query)
                logger.info(f"{supp_table} suppression done successfully...")
            res['countsAfterFilter'] = get_record_count(source_table, sf_cursor)
            result.append(res)
            logger.info(f"{supp_key} suppression done successfully...")
        current_count = get_record_count(f"{source_table}", sf_cursor)
        logger.info(f"the result breakdown flag is : {result_breakdown_flag}")
        if not result_breakdown_flag:
            single_res = {}
            single_res['offerId'], single_res['filterType'], single_res['associateOfferId'], single_res[
                'downloadCount'], single_res['insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
            single_res['filterName'] = 'Green FeedLevel Suppression'
            single_res['countsBeforeFilter'] = result[0]['countsBeforeFilter']
            single_res['countsAfterFilter'] = result[-1]['countsAfterFilter']
            return True, [single_res], current_count
        else:
            return True, result, current_count
    except Exception as e:
        logger.error(f"Exception occurred: Please look into it... {str(e)}"+ str(traceback.format_exc()))
        return False, str(e), 0
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def apply_infs_feed_level_suppression(source_table, result_breakdown_flag, logger):
    try:
        result = []
        logger.info(f"Applying INFS Feed Level Suppression for the given table {source_table}")
        logger.info("Acquiring snowflake Connection")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        logger.info("Acquired snowflake connection successfully")
        sf_alter_temp_table_query = f"alter table {source_table} add column if not exists account_name varchar"
        logger.info(f" Executing query : {sf_alter_temp_table_query}")
        sf_cursor.execute(sf_alter_temp_table_query)
        sf_update_temp_table_query = f"update {source_table} a set a.account_name=b.account_name from INFS_LPT.INFS_ORANGE_MAPPING_TABLE b where a.LIST_ID=b.listid"
        logger.info(f" Executing query : {sf_update_temp_table_query}")
        sf_cursor.execute(sf_update_temp_table_query)
        # email level
        for supp_dict in INFS_FEED_LEVEL_SUPP_TABLES['email']:
            supp_key = list(supp_dict.keys())[0]
            supp_tables = supp_dict[supp_key]
            res = {}
            res['offerId'], res['filterType'], res['associateOfferId'], res['downloadCount'], res[
                'insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
            res['filterName'] = supp_key
            res['countsBeforeFilter'] = get_record_count(source_table, sf_cursor)
            for supp_table in supp_tables:
                sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{supp_key}' FROM ({supp_table}) b WHERE  a.EMAIL_ID = b.email AND a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'"
                logger.info(f"Executing query:  {sf_update_table_query}")
                sf_cursor.execute(sf_update_table_query)
                logger.info(f"{supp_table} suppression done successfully...")
            res['countsAfterFilter'] = get_record_count(source_table, sf_cursor)
            result.append(res)
            logger.info(f"{supp_key} suppression done successfully...")
        #email and listid ,includes account name suppressions also
        for supp_dict in INFS_FEED_LEVEL_SUPP_TABLES['email_listid']:
            supp_key = list(supp_dict.keys())[0]
            supp_tables = supp_dict[supp_key]
            res = {}
            res['offerId'], res['filterType'], res['associateOfferId'], res['downloadCount'], res[
                'insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
            res['countsBeforeFilter'] = get_record_count(source_table, sf_cursor)
            res['filterName'] = supp_key
            for supp_table in supp_tables:
                sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{supp_key}' FROM ({supp_table}) b WHERE  a.EMAIL_ID = b.email AND a.LIST_ID = b.listid AND a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH' "
                logger.info(f"Executing query:  {sf_update_table_query}")
                sf_cursor.execute(sf_update_table_query)
                logger.info(f"{supp_table} suppression done successfully...")
                if supp_key == "Account Level Unsubs":
                    sf_update_table_query = f"UPDATE {source_table} a SET a.do_suppressionStatus = '{supp_key}' FROM INFS_LPT.unsub_details_oteam b where iff(a.list_id='2','3188',a.list_id)=iff(b.listid='2','3188',b.listid) AND a.EMAIL_ID=b.email AND a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'"
                    logger.info(f"Executing query:  {sf_update_table_query}")
                    sf_cursor.execute(sf_update_table_query)
                    sf_update_temp_table_query = f"update {source_table} a set a.do_suppressionStatus = '{supp_key}' from (select c.email,d.account_name from INFS_LPT.unsub_details_oteam c join INFS_LPT.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) b where a.account_name=b.account_name and a.EMAIL_ID=b.email and a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'"
                    logger.info(f" Executing query : {sf_update_temp_table_query}")
                    sf_cursor.execute(sf_update_temp_table_query)
                    logger.info(f"{supp_table} account level suppression done successfully...")
                elif supp_key == "IEP Unsubs":
                    sf_update_temp_table_query = f"update {source_table} a set a.do_suppressionStatus = '{supp_key}' from (select c.email,d.account_name from (select email,listid from INFS_LPT.EMAIL_REPLIES_TRANSACTIONAL a join INFS_LPT.GM_SUBID_DOMAIN_DETAILS b on lower(trim(a.domain))=lower(trim(b.domain)) where a.id > 17218326 ) c join INFS_LPT.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid ) b where a.account_name=b.account_name and a.EMAIL_ID=b.email and a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'"
                    logger.info(f" Executing query : {sf_update_temp_table_query}")
                    sf_cursor.execute(sf_update_temp_table_query)
                    logger.info(f"{supp_table} account level suppression done successfully...")
                elif supp_key == "Orange Account Unsubs":
                    sf_update_temp_table_query = f"update {source_table} a set a.do_suppressionStatus = '{supp_key}' from (select c.email,d.account_name from INFS_LPT.INFS_UNSUBS_ACCOUNT_WISE c join INFS_LPT.INFS_ORANGE_MAPPING_TABLE d on c.account_name=d.account_name ) b where a.account_name=b.account_name and a.EMAIL_ID=b.email and a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'"
                    logger.info(f" Executing query : {sf_update_temp_table_query}")
                    sf_cursor.execute(sf_update_temp_table_query)
                    logger.info(f"{supp_table}  account level suppression done successfully...")
                elif supp_key == "Static Account level INFS Unsubs":
                    sf_update_temp_table_query = f"update {source_table} a set a.do_suppressionStatus = '{supp_key}' from (select c.email,d.account_name from INFS_LPT.infs_account_level_static_suppression_data c join INFS_LPT.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) b where a.account_name=b.account_name and a.EMAIL_ID=b.email and a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'"
                    logger.info(f" Executing query : {sf_update_temp_table_query}")
                    sf_cursor.execute(sf_update_temp_table_query)
                    logger.info(f"{supp_table} account level suppression done successfully...")
                else:
                    pass
            res['countsAfterFilter'] = get_record_count(source_table, sf_cursor)
            result.append(res)
            logger.info(f"{supp_key} suppression done successfully...")
        # BLUE_CLIENT_DATA_SUPPRESSION
        res = {}
        res['offerId'], res['filterType'], res['associateOfferId'], res['downloadCount'], res[
            'insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
        res['filterName'] = 'INFS Optizmo Unsubs'
        res['countsBeforeFilter'] = get_record_count(source_table, sf_cursor)
        sf_update_table_query = f"UPDATE {source_table} a SET a.do_suppressionStatus = 'INFS Optizmo Unsubs' FROM INFS_LPT.BLUE_CLIENT_DATA_SUPPRESSION b where a.EMAIL_MD5=b.md5hash and a.list_id in (select listid from INFS_LPT.BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS) AND a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'"
        logger.info(f"Executing query:  {sf_update_table_query}")
        sf_cursor.execute(sf_update_table_query)
        sf_update_temp_table_query = f"update {source_table} a set a.do_suppressionStatus = 'INFS Optizmo Unsubs'  from INFS_LPT.BLUE_CLIENT_DATA_SUPPRESSION b where a.EMAIL_MD5=b.md5hash and a.account_name in (select account_name from INFS_LPT.BLUE_CLIENT_DATA_SUPPRESSION_LISTIDS c join INFS_LPT.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) AND  a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'"
        logger.info(f" Executing query : {sf_update_temp_table_query}")
        sf_cursor.execute(sf_update_temp_table_query)
        res['countsAfterFilter'] = get_record_count(source_table, sf_cursor)
        result.append(res)
        for supp_dict in INFS_FEED_LEVEL_SUPP_TABLES['listid_profileid']:
            supp_key = list(supp_dict.keys())[0]
            supp_tables = supp_dict[supp_key]
            res = {}
            res['offerId'], res['filterType'], res['associateOfferId'], res['downloadCount'], res[
                'insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
            res['countsBeforeFilter'] = get_record_count(source_table, sf_cursor)
            res['filterName'] = supp_key
            for supp_table in supp_tables:
                sf_update_table_query = f"UPDATE {source_table}  a  SET  a.do_suppressionStatus = '{supp_key}' FROM ({supp_table}) b WHERE  a.PROFILE_ID = b.profileid AND a.LIST_ID = b.listid AND a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH' "
                logger.info(f"Executing query:  {sf_update_table_query}")
                sf_cursor.execute(sf_update_table_query)
                logger.info(f"{supp_table} suppression done successfully...")
                if supp_key == "Account level Conversions":
                    sf_update_temp_table_query = f"update {source_table} a set a.do_suppressionStatus = '{supp_key}' from (select c.profileid,d.account_name from INFS_LPT.APT_CUSTOM_CONVERSIONS_DATA_OTEAM c join INFS_LPT.INFS_ORANGE_MAPPING_TABLE d on c.listid=d.listid) b where a.account_name=b.account_name and a.PROFILE_ID=b.profileid and a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'"
                    logger.info(f" Executing query : {sf_update_temp_table_query}")
                    sf_cursor.execute(sf_update_temp_table_query)
                    logger.info(f"{supp_table} account level suppression done successfully...")
            res['countsAfterFilter'] = get_record_count(source_table, sf_cursor)
            result.append(res)
            logger.info(f"{supp_key} suppression done successfully...")

        current_count = get_record_count(f"{source_table}", sf_cursor)
        logger.info(f"the result breakdown flag is : {result_breakdown_flag}")
        if not result_breakdown_flag:
            single_res = {}
            single_res['offerId'], single_res['filterType'], single_res['associateOfferId'], single_res[
                'downloadCount'], single_res['insertCount'] = 'NA', 'Suppression', 'NA', '0', '0'
            single_res['filterName'] = 'INFS FeedLevel Suppression'
            single_res['countsBeforeFilter'] = result[0]['countsBeforeFilter']
            single_res['countsAfterFilter'] = result[-1]['countsAfterFilter']
            return True, [single_res], current_count
        else:
            return True, result, current_count
    except Exception as e:
        logger.error(f"Exception occurred: Please look into it... {str(e)}"+ str(traceback.format_exc()))
        return False, str(e), 0
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def channel_suppression(main_request_details, filter_details, source_table, logger, mysql_cursor):
    logger.info("channel_suppression execution started.")
    channel = main_request_details['channelName']
    suppression_method = filter_details['suppressionMethod']
    result_breakdown_flag = True
    if channel == 'GREEN':
        if suppression_method == 'F':
            status, results, current_count = apply_green_feed_level_suppression(source_table, result_breakdown_flag, logger)
        elif suppression_method == 'G':
            status, results, current_count = apply_green_global_suppression(source_table, result_breakdown_flag, logger)
    elif channel == 'INFS':
        status, results, current_count = apply_infs_feed_level_suppression(source_table, result_breakdown_flag, logger)
    elif "Apptness" in channel:
        status, results, current_count = apply_global_fp_feed_level_suppression(source_table,result_breakdown_flag, logger)

    if not status:
        raise Exception(f'Exception occurred while performing channel_suppression. Please look into it. {results}')
    logger.info(f"Inserting channel suppression stats:  {results}")
    for result in results:
        result['requestId'], result['requestScheduledId'], result['runNumber'] = main_request_details['id'],\
            main_request_details['ScheduleId'], main_request_details['runNumber']
        columns = ', '.join(result.keys())
        values_formatter = ', '.join(['%s'] * len(result))
        stats_insert_query = f"INSERT INTO {SUPPRESSION_MATCH_DETAILED_STATS_TABLE} ({columns}) VALUES ({values_formatter})"
        mysql_cursor.execute(stats_insert_query, tuple(result.values()))
    logger.info("channel_suppression execution ended.")
    return current_count


def state_and_zip_suppression(filter_type, current_count, main_request_table, filter_values, main_logger, mysql_cursor, main_request_details):
    try:
        counts_before_filter = current_count
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        if filter_type == "Zipcode Suppression":
            filter = "ZIP"
            error_code = 'DO17'
        elif filter_type == "State Suppression":
            filter = "STATE"
            error_code = 'DO18'
        filter_values = str(filter_values).replace(",", "','")
        sf_update_query = f"update {main_request_table} a set a.do_suppressionStatus = '{filter_type}' from " \
                          f"{POSTAL_TABLE} b where a.EMAIL_MD5 = b.md5hash and b.{filter} in ('{filter_values}') and a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'"
        main_logger.info(f"Performing {filter_type}, Executing Query: {sf_update_query}")
        sf_cursor.execute(sf_update_query)
        counts_after_filter = get_record_count(main_request_table, sf_cursor)
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,(main_request_details['id'],main_request_details['ScheduleId'],
                                                                      main_request_details['runNumber'],'NA','Suppression','NA'
                                                                      ,filter_type,counts_before_filter,counts_after_filter,0,0))
        return counts_after_filter
    except Exception as e:
        raise CustomError(error_code, {'error': str(e)})
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()

class SnowflakeContextManager:
    def __init__(self, sfcon):
        self.sfcon = sfcon

    def __enter__(self):
        self.cur = self.sfcon.cursor()
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()


class MysqlContextManager:

    def __init__(self, mysqlcon):
        self.mysqlcon = mysqlcon

    def __enter__(self):
        self.cur = self.mysqlcon.cursor(dictionary = True)
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()

class SnowflakeContextManager:
    def __init__(self, sfcon):
        self.sfcon = sfcon

    def __enter__(self):
        self.cur = self.sfcon.cursor()
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()


class MysqlContextManager:

    def __init__(self, mysqlcon):
        self.mysqlcon = mysqlcon

    def __enter__(self):
        self.cur = self.mysqlcon.cursor()
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()

class LiveFeed:

    def __init__(self, id, feedName, listId, channelId, suppressionRuleIds, dataPartnerId):
        self.id = id
        self.feedName = feedName
        self.listId = listId
        self.channelId = channelId
        self.suppressionRuleIds = suppressionRuleIds
        self.dataPartnerId = dataPartnerId

class FeedLevelSuppression():

    def __init__(self, sfcon, mysqlcon, main_request_table, result_breakdown,logger):
        self.sfcon = sfcon
        self.main_request_table = main_request_table
        self.mysqlcon = mysqlcon
        self.result_breakdown = result_breakdown
        self.logger = logger

    def getSuppressionCode(self):
        liveFeedTbl = {}
        json_data=None
        try:
            query = f" select id ,code from LIVE_FEED_SUPPRESSION_RULE order by 1 "
            with MysqlContextManager(self.mysqlcon) as mysqlcur:
                mysqlcur.execute(query)
                rows = mysqlcur.fetchall()
                for r in rows:
                    liveFeedTbl[str(r[0])] = r[1]
        except Exception as e:
            self.logger.info("ERROR :: in getSuppressionCode()", e)
        return liveFeedTbl

    def updateGlobalTable(self, liveFeed):
        method = f"{self.__class__.__name__} ::getLiveFeedDetails() :: ListId:{liveFeed.listId} :: channelId:{liveFeed.channelId} :: "
        self.logger.info(f"{method} has started")
        supCode = self.getSuppressionCode()
        supIds = liveFeed.suppressionRuleIds.split(",")


        self.logger.info(f"{method} {supIds}")

        self.logger.info(f"{method} {supCode}")

        # self.log.logMsg(f"{method} {liveFeed}", "I")
        json_data = None
        for i in supIds:
            isChannelUnsub = False
            isChannelAbuse = False
            cunsubCode = ''
            cabuseCode = ''
            cjoinCnd = ''
            wccond = ''

            funsubCode = ''
            fjoinCnd = ''
            wfcond = ''

            zunsubCode = ''
            zjoinCnd = ''
            zhcond = ''

            dpunsubCode = ''
            dpjoinCnd = ''
            dpcond = ''

            # cdunsubCode = ''
            cdjoinCnd = ''
            cdcond = ''

            gunsubCode = ''
            gjoinCnd = ''
            gcond = ''

            bounjoinCnd = ''
            bouncond = ''

            ccpajoinCnd = ''
            ccpacond = ''
            runQue = False

            if f"{i}" in supCode.keys():
                if supCode[i] == 'CUNSUB':
                    cunsubCode = f"{cunsubCode},'A'"
                    cjoinCnd = f' left outer join LIST_PROCESSING.GLOBALFP_UNSUBS_SF cunsub on lower(a.EMAIL_ID)= lower(cunsub.email)' \
                               f' and ( cunsub.channelid={liveFeed.channelId} or cunsub.channelid = 0)'
                    wccond = ' and cunsub.email is not null '
                    isChannelUnsub = True
                    runQue = True

                if supCode[i] == 'CABUSE':
                    cabuseCode = f"{cabuseCode},'A'"
                    cjoinCnd = f' left outer join LIST_PROCESSING.GLOBALFP_UNSUBS_SF cunsub on lower(a.EMAIL_ID)= lower(cunsub.email)' \
                               f' and ( cunsub.channelid={liveFeed.channelId} or cunsub.channelid = 0 ) '
                    wccond = ' and cunsub.email is not null '
                    isChannelAbuse = True
                    runQue = True

                if supCode[i] == 'FUNSUB':
                    funsubCode = f"{funsubCode},'A'"
                    fjoinCnd = f' left outer join LIST_PROCESSING.GLOBALFP_UNSUBS_SF funsub on lower(a.EMAIL_ID)= lower(funsub.email)' \
                               f' and ((funsub.channelid={liveFeed.channelId} and a.LIST_ID=funsub.listid)  or funsub.channelid=0 ) '
                    wfcond = ' and funsub.email is not null '
                    runQue = True

                if supCode[i] == 'ZABUSE':
                    zunsubCode = f"{zunsubCode},'A'"
                    zjoinCnd = f' left outer join LIST_PROCESSING.GLOBALFP_UNSUBS_SF zunsub on lower(a.EMAIL_ID)= lower(zunsub.email)' \
                               f' and zunsub.channelid={liveFeed.channelId} and a.LIST_ID=zunsub.listid and zunsub.source=\'zh\''
                    zhcond = ' and zunsub.email is not null '
                    runQue = True

                if supCode[i] == 'DUNSUB':
                    dpunsubCode = f"{dpunsubCode},'A'"
                    dpjoinCnd = f' left outer join LIST_PROCESSING.GLOBALFP_UNSUBS_SF dpunsub on lower(a.EMAIL_ID)= lower(dpunsub.email)' \
                                f'  and dpunsub.datapartnerid={liveFeed.dataPartnerId} '
                    dpcond = ' and dpunsub.email is not null '
                    runQue = True

                if supCode[i] == 'GCOMPR':
                    gjoinCnd = ' left outer join LIST_PROCESSING.APT_CUSTDOD_GLOBAL_COMPLAINER_EMAILS_SF gunsub on lower(a.EMAIL_ID)= lower(gunsub.email)'
                    gcond = ' and gunsub.email is not null '
                    runQue = True

                if supCode[i] == 'CANADA':
                    cdjoinCnd = ' left outer join LIST_PROCESSING.PFM_FLUENT_REGISTRATIONS_CANADA_SF cdunsub on lower( a.EMAIL_ID)= lower(cdunsub.EMAIL)' \
                                ' '
                    cdcond = ' and cdunsub.EMAIL is not null '
                    runQue = True

                if supCode[i] == 'BOSUPR':
                    bounjoinCnd = f' left outer join GREEN_LPT.APT_CUSTOM_GLOBAL_HARDBOUNCES_DATA hrdboun on lower(a.EMAIL_ID)= lower(hrdboun.EMAIL) left outer join GREEN_LPT.APT_CUSTOM_GLOBAL_SOFTINACTIVE sftboun on lower(a.EMAIL_ID)= lower(sftboun.EMAIL) '
                    bouncond = ' and (hrdboun.EMAIL is not null or sftboun.EMAIL is not null)'
                    runQue = True

                if supCode[i] == 'CCMP':
                    ccpajoinCnd = f' left outer join LIST_PROCESSING.GLOBALFP_CHANNEL_COMPLAINERS ccmp on lower(a.EMAIL_ID)= lower(ccmp.EMAIL) '
                    ccpacond = f' and ccmp.channelid={liveFeed.channelId} and a.LIST_ID={liveFeed.listId} and ccmp.EMAIL is not null '
                    runQue = True

                if funsubCode.startswith(","):
                    funsubCode = funsubCode[1:]

                if cabuseCode.startswith(","):
                    cabuseCode = cabuseCode[1:]

                if cunsubCode.startswith(","):
                    cunsubCode = cunsubCode[1:]

                if zunsubCode.startswith(","):
                    zunsubCode = zunsubCode[1:]

                if dpunsubCode.startswith(","):
                    dpunsubCode = dpunsubCode[1:]

                if len(funsubCode) > 0:
                    fjoinCnd = f'{fjoinCnd}'

                if len(cunsubCode) > 0 or len(cabuseCode) > 0:
                    if isChannelUnsub and not isChannelAbuse:
                        cjoinCnd = f' {cjoinCnd} and cunsub.type not in ({cunsubCode}) '
                    if isChannelAbuse and not isChannelUnsub:
                        cjoinCnd = f' {cjoinCnd} and cunsub.type in ({cabuseCode}) '
                    if isChannelAbuse and isChannelUnsub:
                        cjoinCnd = f' {cjoinCnd} and (cunsub.type not in ({cunsubCode})  or cunsub.type in ({cabuseCode}) )'

                if len(zunsubCode) > 0:
                    zjoinCnd = f' {zjoinCnd} and zunsub.type not in ({zunsubCode}) '

                if len(dpunsubCode) > 0:
                    dpjoinCnd = f' {dpjoinCnd} and dpunsub.type not in ({dpunsubCode}) '
                if runQue:
                    query = f"merge into {self.main_request_table} as a using (select distinct a.*  from {self.main_request_table} a {cjoinCnd}  {fjoinCnd} {zjoinCnd} {cdjoinCnd} {gjoinCnd} {dpjoinCnd} {bounjoinCnd} {ccpajoinCnd} where  a.do_suppressionStatus ='CLEAN' AND a.do_matchStatus!='NON_MATCH' {wccond}  {wfcond} {zhcond} {cdcond} {gcond} {dpcond} {bouncond} {ccpacond} ) as b on lower(a.EMAIL_ID)=lower(b.EMAIL_ID)  when matched then update set do_suppressionStatus='{supCode[i]}'"
                    self.logger.info(f"{method}Executing Query {query} ")
                    with SnowflakeContextManager(self.sfcon) as sfcur:
                        self.logger.info(f"QUERY ::{query}")
                        sfcur.execute("ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE=false;")
                        sfcur.execute(query)
        self.logger.info(f"{method} has ended")
        return json_data
    def getDistinctListid(self) -> str:
        listids = ""
        try:
            with SnowflakeContextManager(self.sfcon) as sfcur:
                query = f" select distinct LIST_ID from {self.main_request_table} where LIST_ID is not NULL"
                self.logger.info(f"QUERY :: {query}")
                sfcur.execute(query)
                listids = ','.join([f"{r[0]}" for r in sfcur.fetchall()])
                if listids == '':
                    listids = "''"
        except Exception as e:
            self.logger.info(f"ERROR :: in getDistinctListid() {e}")

        return listids

    def getLiveFeedDetails(self, listids) -> list:
        liveFeedTbl = []
        with MysqlContextManager(self.mysqlcon) as mysqlcon:
            query = f" select id,feedName,listId,channelId,suppressionRuleIds,dataPartnerId from LIVE_FEED where active=true and listid in ({listids}) "
            self.logger.info(f"QUERY ::  {query}")
            mysqlcon.execute(query)
            rows = mysqlcon.fetchall()
            for r in rows:
                syncTbl = LiveFeed(r[0], r[1], r[2], r[3], r[4], r[5])
                liveFeedTbl.append(syncTbl)
        return liveFeedTbl

    # Invoke this method to apply feed level suppressions
    def applyFeedLevelSuppression(self) -> bool:
        self.logger.info(f"Running feed level suppressions applyFeedLevelSuppression():::{datetime.now()}")
        json_data = []
        try:
            with SnowflakeContextManager(self.sfcon) as sfcur:
                counts_before_filter = get_record_count(self.main_request_table, sfcur)
            listids = self.getDistinctListid()
            livefeedpojos = self.getLiveFeedDetails(listids)
            for livefeedpojo in livefeedpojos:
                self.updateGlobalTable(livefeedpojo)
            fetch_supp_codes = f" select name, code from LIVE_FEED_SUPPRESSION_RULE where code not in ('DUPCATE','RESPON') order by id "
            with MysqlContextManager(self.mysqlcon) as mysqlcur:
                mysqlcur.execute(fetch_supp_codes)
                supp_codes = mysqlcur.fetchall()
                for supp_code in supp_codes:
                    filter_details = {}
                    filter_details["offerId"], filter_details["filterType"], filter_details["associateOfferId"], \
                        filter_details["downloadCount"], filter_details["insertCount"] = "NA", "Suppression", "NA", 0, 0
                    with SnowflakeContextManager(self.sfcon) as sfcur:
                        sf_query = f"select count(1) from {self.main_request_table} where do_suppressionStatus = '{supp_code[1]}'"
                        sfcur.execute(sf_query)
                        counts_after_filter = counts_before_filter - sfcur.fetchone()[0]
                    filter_details["countsBeforeFilter"], filter_details["countsAfterFilter"], \
                        filter_details["filterName"] = counts_before_filter, counts_after_filter, APPTNESS_SUPP_CODES[supp_code[1]]
                    json_data.append(filter_details)
                    counts_before_filter = counts_after_filter
        except Exception as e:
            self.logger.info(f"Exception occurred in: applyFeedLevelSuppression() Please look into this....{str(e)}")
            return False , str(e)
        return True, json_data


def apply_global_fp_feed_level_suppression(main_request_table, result_breakdown_flag, logger):
    try:
        logger.info("Function initiated global_fp feed level suppression")
        mysql_con = mysql.connector.connect(**MYSQL_CONFIGS)
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        fobj = FeedLevelSuppression(sf_conn, mysql_con, main_request_table,result_breakdown_flag,logger)
        status, result = fobj.applyFeedLevelSuppression()
        logger.info(f"Fetched result: {result}")
        if not status:
            return False, str(result), 0
        #logger.info(f"Fetched result : {result}")
        sf_cursor = sf_conn.cursor()
        current_count = get_record_count(f"{main_request_table}", sf_cursor)
        return True, result,current_count
    except Exception as e:
        return False, str(result)+"::::"+str(e), 0
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def purdue_suppression(main_request_details, main_request_table, logger, counts_before_filter):
    try:
        logger.info(f"Purdue suppression initiated.")
        request_id = main_request_details['id']
        run_number = main_request_details['runNumber']
        os.makedirs(f"{SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_INPUT_FILES/", exist_ok=True)
        os.system(f"rm {SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_INPUT_FILES/*")
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        sf_cursor.execute(f"create or replace temporary stage purdue_stage_{request_id}_{run_number}")
        sf_cursor.execute(f"copy into @purdue_stage_{request_id}_{run_number} from (select distinct EMAIL_MD5 from "
                          f"{main_request_table} where do_matchStatus!='NON_MATCH' and do_suppressionStatus='CLEAN') "
                          f"FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP)")
        logger.info(f"Copying .gz files from stage to {SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_INPUT_FILES/ path")
        sf_cursor.execute(f"get @purdue_stage_{request_id}_{run_number} "
                          f"file://{SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_INPUT_FILES/")
        logger.info(f"Unzipping .gz files in {SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_INPUT_FILES/")
        os.system(f"gunzip {SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_INPUT_FILES/*.gz")
        logger.info(f"Copying {SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_INPUT_FILES/*.csv files into"
                    f" single file {SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_INPUT_FILES/Purdue_main_file_{request_id}_{run_number}.csv")
        os.system(f"cat {SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_INPUT_FILES/*.csv"
                  f" > {SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_INPUT_FILES/Purdue_main_file_{request_id}_{run_number}.csv")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        logger.info(f"Inserting into Purdue lookup table. Executing: {PURDUE_INSERT_QUERY,(request_id,run_number,'W')}")
        mysql_cursor.execute(PURDUE_INSERT_QUERY, (request_id,run_number,'W'))
        logger.info(f"Checking if any purdue supp configured requests are in-progress.")
        in_queue = True
        while in_queue:
            mysql_cursor.execute(PURDUE_CHECK_INPROGRESS_QUERY)
            result = mysql_cursor.fetchone()
            if result is not None:
                logger.info(f"Currently request_id: {result['requestId']}, run_number: {result['runNumber']} purdue"
                            f" supp is in-progress. So, waiting for {PURDUE_SUPP_WAITING_TIME} sec")
                time.sleep(PURDUE_SUPP_WAITING_TIME)
            else:
                logger.info(f"Currently no purdue supp requests are in-progress. Checking if any other purdue supp requests are in queue.")
                mysql_cursor.execute(PURDUE_CHECK_QUEUE_QUERY)
                result = mysql_cursor.fetchone()
                if result['requestId'] != request_id or result['runNumber'] != run_number:
                    logger.info(f"Currently request_id: {result['requestId']}, run_number: {result['runNumber']} purdue"
                                f" supp is prior in the queue and might initiated soon. So, checking again after "
                                f"{PURDUE_SUPP_WAITING_TIME} sec")
                    time.sleep(PURDUE_SUPP_WAITING_TIME)
                else:
                    logger.info("Currently, purdue supp requests queue is zero. So, making this request as in-progress")
                    mysql_cursor.execute(PURDUE_UPDATE_STATUS_QUERY,('I', request_id, run_number))

                    response = subprocess.run(["/bin/sh", "-x", f"{SUPP_SCRIPT_PATH}/purdue_supp.sh",
                                             f"{SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_INPUT_FILES/Purdue_main_file_{request_id}_{run_number}.csv",
                                             f"{SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_SUPP_FILES/"])
                    response.check_returncode()

                    logger.info(f"Making purdue status as Completed in {PURDUE_SUPP_LOOKUP_TABLE} for request_id: {result['requestId']}"
                                 f", run_number: {result['runNumber']} . Executing: {PURDUE_UPDATE_STATUS_QUERY, ('C', request_id, run_number)}  ")
                    mysql_cursor.execute(PURDUE_UPDATE_STATUS_QUERY, ('C', request_id, run_number))
                    in_queue = False
        purdue_supp_table = main_request_table + "_PURDUE_SUPP"
        sf_cursor.execute(f"create or replace temporary stage purdue_stage_{request_id}_{run_number}_supp")
        sf_cursor.execute(f"put file://{SUPP_FILE_PATH}/{request_id}/{run_number}/PURDUE_SUPP_FILES/suppression_list--PG_Unsubscribe_List_*.txt @purdue_stage_{request_id}_{run_number}_supp")
        sf_cursor.execute(f"create or replace transient table {purdue_supp_table}(email_md5 varchar)")
        sf_cursor.execute(f"copy into {purdue_supp_table} from @purdue_stage_{request_id}_{run_number}_supp")
        sf_cursor.execute(f"update {main_request_table} a set a.do_suppressionStatus = 'Purdue Suppression' FROM {purdue_supp_table} b "
                          f"WHERE a.email_md5=b.email_md5 and a.do_suppressionStatus = 'CLEAN' and a.do_matchStatus != 'NON_MATCH'")
        counts_after_filter = get_record_count(main_request_table, sf_cursor)
        mysql_cursor.execute(INSERT_SUPPRESSION_MATCH_DETAILED_STATS,(main_request_details['id'],main_request_details['ScheduleId'],main_request_details['runNumber'],'NA','Suppression','NA'
                                                                      ,'Purdue Suppression',counts_before_filter,counts_after_filter,0,0))
        logger.info(f"Purdue suppression completed.")
        return counts_after_filter
    except Exception as e:
        logger.error(f"Exception occurred while performing purdue suppression. {str(e)} " + str(traceback.format_exc()))
        logger.error(f"Making purdue status as Error in {PURDUE_SUPP_LOOKUP_TABLE} for request_id: {result['requestId']}"
                     f", run_number: {result['runNumber']} . Executing: {PURDUE_UPDATE_STATUS_QUERY, ('E', request_id, run_number)}  ")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        mysql_cursor.execute(PURDUE_UPDATE_STATUS_QUERY, ('E', request_id, run_number))
        raise CustomError('DO19',{'error': str(e)})
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.close()
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def populate_stats_table(main_request_details, main_request_table, logger, mysql_cursor, run_number):
    try:
        grouping_columns = str(main_request_details['groupingColumns'])
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor(DictCursor)
        logger.info("Updating Hotmail ISPS to Hotmail....")
        logger.info(f"Executing query :: update {main_request_table} set isp = 'Hotmail' where isp in ({HOTMAIL_ISPS})")
        sf_cursor.execute(f"update {main_request_table} set isp = 'Hotmail' where isp in ({HOTMAIL_ISPS})")
        sf_cursor.execute(f"alter table {main_request_table} add column do_originalMatchStatus varchar default 'NON_MATCH'")
        sf_query = f"update {main_request_table} set do_originalMatchStatus = DO_MATCHSTATUS, DO_MATCHSTATUS =  " \
                   f"case when STARTSWITH(DO_MATCHSTATUS,'By Attribute') then 'By_Field' else  " \
                   f"replace(SPLIT_PART(REGEXP_REPLACE(SPLIT_PART(DO_MATCHSTATUS,':',1),'^Dataset |^File ','')," \
                   f"'/',-1),' ','') end where DO_MATCHSTATUS!='NON_MATCH';"
        logger.info(f"Updating do_matchStatus value, Executing query: {sf_query}")
        sf_cursor.execute(sf_query)
        stats_pulling_query = f'''select 'NA' as "OfferId",{grouping_columns.upper().replace(
            'DO_INPUTSOURCE','DO_INPUTSOURCE as "Input Source"').replace('DO_MATCHSTATUS','DO_MATCHSTATUS as "Matched Source"').replace(
            'DO_JORNAYAMATCH','DO_JORNAYAMATCH as "Jornaya Status"').replace('DO_MOCKINGBIRDMATCH','DO_MOCKINGBIRDMATCH as "Mockingbird Status"').replace(
            'LIST_ID','DO_FEEDNAME as "Feed Name"')}, count(1) as Count from {main_request_table} where 
            do_matchStatus != 'NON_MATCH' and do_suppressionStatus = 'CLEAN' group by {grouping_columns.upper().replace('LIST_ID','DO_FEEDNAME')}'''
        logger.info(f"Pulling stats from snowflake. Executing query: {stats_pulling_query}")
        sf_cursor.execute(stats_pulling_query)
        stats = sf_cursor.fetchall()
        # replace none values in stats
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        if len(stats) > STATS_LIMIT:
            logger.info(f'''Executing query: { INSERT_INTO_STATS_TABLE_QUERY,(str(main_request_details['id']),str(main_request_details['ScheduleId']),str(main_request_details['runNumber']),str([{"COUNT":-1 ,"status":f"{STATS_LIMIT} no. of records limit reached."}]))}''')
            mysql_cursor.execute(INSERT_INTO_STATS_TABLE_QUERY,(str(main_request_details['id']),str(main_request_details['ScheduleId']),str(main_request_details['runNumber']),str([{'COUNT': -1 ,'status':f'{STATS_LIMIT} no. of records limit reached.'}]).replace("'",'"')))
        else:
            mysql_cursor.execute(FETCH_SUCCESS_OFFERS,(main_request_details['id'],run_number))
            fetched_offers = mysql_cursor.fetchone()
            if fetched_offers['success_offers'] is not None:
                offerids_list = str(fetched_offers['success_offers']).split(',')
                for offerid in offerids_list:
                    offer_stats_pulling_query = f'''select {offerid} as "OfferId",{grouping_columns.upper().replace(
                        'DO_INPUTSOURCE','DO_INPUTSOURCE as "Input Source"').replace('DO_MATCHSTATUS','DO_MATCHSTATUS as "Matched Source"').replace(
            'DO_JORNAYAMATCH','DO_JORNAYAMATCH as "Jornaya Status"').replace('DO_MOCKINGBIRDMATCH','DO_MOCKINGBIRDMATCH as "Mockingbird Status"').replace('LIST_ID','DO_FEEDNAME as "Feed Name"')}
                        , count(1) as Count from {main_request_table} where do_matchStatus != 'NON_MATCH' and do_suppressionStatus = 'CLEAN' and 
                        do_matchStatus_{offerid} != 'NON_MATCH' and do_suppressionStatus_{offerid} = 'CLEAN' group by {grouping_columns.upper().replace('LIST_ID','DO_FEEDNAME')}'''
                    logger.info(f"Pulling stats from snowflake. Executing query: {offer_stats_pulling_query}")
                    sf_cursor.execute(offer_stats_pulling_query)
                    stats += sf_cursor.fetchall()
                #replace None values
            stats = str(stats).replace("'",'"').replace(': None',': ""')
            mysql_cursor.execute(INSERT_INTO_STATS_TABLE_QUERY, (str(main_request_details['id']), str(main_request_details['ScheduleId']), str(main_request_details['runNumber']), str(stats)))
            logger.info(f"Successfully inserted stats in {SUPPRESSION_REQUEST_DATA_STATS_TABLE} mysql table")
    except Exception as e:
        logger.error(f"Exception occurred while populating stats table. {str(e)} " + str(traceback.format_exc()))
        raise CustomError('DO24',{'error': str(e)})
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.close()
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()

def populate_input_sources_table(main_request_details, main_request_table, logger, mysql_cursor):
    try:
        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIGS)
        sf_cursor = sf_conn.cursor()
        sf_query = f"select distinct do_inputSource from {main_request_table}"
        logger.info(f"Fetching input source details from request table. Executing query: {sf_query}")
        sf_cursor.execute(sf_query)
        input_sources = sf_cursor.fetchall()
        mysql_cursor.execute(DELETE_OLD_INPUT_SOURCES,(main_request_details['id'],))
        logger.info(f"Deleted old input source details from {SUPPRESSION_REQUEST_INPUT_SOURCES_TABLE} table.")
        for input_source in input_sources:
            mysql_cursor.execute(INSERT_INPUT_SOURCES,(main_request_details['id'],input_source[0]))
        logger.info(f"Successfully populated input source details in {SUPPRESSION_REQUEST_INPUT_SOURCES_TABLE} table.")
        return input_sources
    except Exception as e:
        logger.error(f"Exception occurred while populating {SUPPRESSION_REQUEST_INPUT_SOURCES_TABLE} table. {str(e)} " + str(traceback.format_exc()))
        raise Exception(f"Exception occurred while populating {SUPPRESSION_REQUEST_INPUT_SOURCES_TABLE} table. {str(e)} " + str(traceback.format_exc()))
    finally:
        if 'connection' in locals() and sf_conn.is_connected():
            sf_cursor.close()
            sf_conn.close()


def populate_file_generation_details(main_request_details, logger, mysql_cursor, input_sources):
    try:
        insert_values = (main_request_details['id'], main_request_details['offerSuppressionIds'],
                         main_request_details['groupingColumns'], ','.join([i[0] for i in input_sources]),
                         main_request_details['ftpIds'], 'GM_API', 'GM_API')
        logger.info(f"Auto generate files option is selected, so populating file generation details into "
                    f"{SUPPRESSION_REQUEST_FILES_INPUT_TABLE} table. Executing query: {INSERT_AUTO_GENERATE_FILE_DETAILS},{insert_values}")
        mysql_cursor.execute(INSERT_AUTO_GENERATE_FILE_DETAILS,insert_values)
        logger.info(f"Successfully populated file generation details.")
    except Exception as e:
        logger.error(f"Exception occurred while populating {SUPPRESSION_REQUEST_FILES_INPUT_TABLE} table. {str(e)} " + str(traceback.format_exc()))
        raise Exception(f"Exception occurred while populating {SUPPRESSION_REQUEST_FILES_INPUT_TABLE} table. {str(e)} " + str(traceback.format_exc()))


def add_table(main_request_details, run_number):
    table_msg = ''

    def connect_db(config):
        retries = 5
        while retries > 0:
            try:
                conn = mysql.connector.connect(**config)
                if conn.is_connected():
                    return conn
            except mysql.connector.Error:
                retries -= 1
                time.sleep(1)  # Wait before retrying
        raise Exception("Failed to connect to the database after multiple retries.")

    if main_request_details['offerSuppressionIds'] is not None:
        main_conn = connect_db(MYSQL_CONFIGS)
        try:
            with main_conn.cursor(dictionary=True) as mysql_cursor:
                success_query = (f"SELECT OFFERID FROM {SUPPRESSION_REQUEST_OFFERS_TABLE} "
                                 f"WHERE STATUS='S' AND requestId= {main_request_details['id']} "
                                 f"AND runNumber ={run_number}")
                mysql_cursor.execute(success_query)
                successful_offers = mysql_cursor.fetchall()
                table_msg += (f"<br><br><b>Offers completed successfully</b>: "
                              f"{','.join([offer_details['OFFERID'] for offer_details in successful_offers])}")

                failed_query = (f"SELECT OFFERID FROM {SUPPRESSION_REQUEST_OFFERS_TABLE} "
                                f"WHERE STATUS='F' AND requestId= {main_request_details['id']} "
                                f"AND runNumber ={run_number}")
                mysql_cursor.execute(failed_query)
                failed_offers = mysql_cursor.fetchall()
                table_msg += (f"<br><br><b>Offers got failed</b>:  "
                              f"{','.join([offer_details['OFFERID'] for offer_details in failed_offers])}")
        finally:
            main_conn.close()

    main_conn = connect_db(MYSQL_CONFIGS)
    try:
        with main_conn.cursor() as mysql_cursor:
            table_msg += "<br><br>Below are the match/suppression stats for the request.<br>"
            table_msg += """<table style="font-family: LatoWeb ;border: 1px solid; width: 100%; border-collapse: collapse;">
                            <thead>
                            <tr style="font-family: LatoWeb ;border: 1px solid ;background-color: #296695; color: white; border-color: black;">
                            <th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Seq #</th><th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Filter Type</th><th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Filter Name</th>
                            <th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Associate Offer ID</th><th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Download Count</th>
                            <th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Insert Count</th><th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Count Before Filter</th>
                            <th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Count After Filter</th></tr></thead><tbody>"""

            mysql_cursor.execute("SET @row_number=0")
            query = (f'''SELECT CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', (@row_number := @row_number + 1), '</td>') AS 'Seq #', '''
                     f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FILTERTYPE, '</td>') AS 'Filter Type', '''
                     f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FILTERNAME, '</td>') AS 'Filter Name', '''
                     f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', ASSOCIATEOFFERID, '</td>') AS 'Associate Offer ID', '''
                     f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FORMAT(downloadcount, 0), '</td>') AS 'Download Count', '''
                     f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FORMAT(insertcount, 0), '</td>') AS 'Insert Count', '''
                     f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FORMAT(countsbeforefilter, 0), '</td>') AS 'Count Before Filter', '''
                     f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FORMAT(countsafterfilter, 0), '</td>') AS 'Count After Filter' '''
                     f'''FROM SUPPRESSION_MATCH_DETAILED_STATS '''
                     f'''WHERE requestid = {main_request_details['id']} AND runNumber = {run_number} AND offerid=0''')
            mysql_cursor.execute(query)
            table_details = mysql_cursor.fetchall()

            for row in table_details:
                table_msg += '''<tr>'''
                table_msg += "".join(row)
                table_msg += "</tr>"
            table_msg += "</tbody></table>"
    finally:
        main_conn.close()

    if main_request_details['offerSuppressionIds'] is not None:
        table_msg += "<br><br>Below are the offer wise stats for the request."
        main_conn = connect_db(MYSQL_CONFIGS)
        try:
            with main_conn.cursor() as mysql_cursor:
                for offer in successful_offers:
                    offer_id = offer['OFFERID']
                    table_msg += f"<br><br><b>OFFER {offer_id} :</b><br><br>"
                    table_msg += """<table style="font-family: LatoWeb ;border: 1px solid; width: 100%; border-collapse: collapse;">
                                    <thead>
                                    <tr style="font-family: LatoWeb ;border: 1px solid ;background-color: #296695; color: white; border-color: black;">
                                    <th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Seq #</th><th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Filter Type</th><th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Filter Name</th>
                                    <th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Associate Offer ID</th><th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Download Count</th>
                                    <th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Insert Count</th><th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Count Before Filter</th>
                                    <th style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left; border-color: black;">Count After Filter</th></tr></thead><tbody>"""

                    mysql_cursor.execute("SET @row_number=0")
                    query = (f'''SELECT CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', (@row_number := @row_number + 1), '</td>') AS 'Seq #', '''
                             f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FILTERTYPE, '</td>') AS 'Filter Type', '''
                             f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FILTERNAME, '</td>') AS 'Filter Name', '''
                             f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', ASSOCIATEOFFERID, '</td>') AS 'Associate Offer ID', '''
                             f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FORMAT(downloadcount, 0), '</td>') AS 'Download Count', '''
                             f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FORMAT(insertcount, 0), '</td>') AS 'Insert Count', '''
                             f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FORMAT(countsbeforefilter, 0), '</td>') AS 'Count Before Filter', '''
                             f'''CONCAT('<td style="font-family: LatoWeb ;border: 1px solid ; padding: 8px; text-align: left;">', FORMAT(countsafterfilter, 0), '</td>') AS 'Count After Filter' '''
                             f'''FROM SUPPRESSION_MATCH_DETAILED_STATS '''
                             f'''WHERE requestid={main_request_details['id']} AND runNumber = {run_number} AND offerid={offer_id} '''
                             f'''ORDER BY lastupdated;''')
                    mysql_cursor.execute(query)
                    table_details = mysql_cursor.fetchall()

                    for row in table_details:
                        table_msg += '''<tr>'''
                        table_msg += "".join(row)
                        table_msg += "</tr>"
                    table_msg += "</tbody></table>"
        finally:
            main_conn.close()

    return table_msg


class CustomError(Exception):
    def __init__(self, error_code, args1=None, raise_exception=True):
        self.error_code = str(error_code)
        self.args1 = args1 or {}

        if self.error_code not in ERROR_CODES:
            self.message = self.error_code
        else:
            self.message = f"{ERROR_CODES[self.error_code]}".format(**self.args1)
        if raise_exception:
            super().__init__(self.message)



def sf_source_name_generator(source_name ,input_data_dict,touch_filter=False):
    new_name = ""
    # starting Category name generation
    if  "Delivered".lower() in source_name.lower():
        new_name += "Delivered_"
    elif "Responders".lower() in source_name.lower():
        if "Genuine".lower() in source_name.lower():
            new_name += "Resp_Genuine_"
        else:
            new_name += "Resp_All_"
    #elif "Profile".lower() in source_name.lower():
    #	new_name += "Profile_"
    #else:
    #	new_name += f"{source_name}_"
    # Filter conditions addition
    for filter in input_data_dict:
        if filter['fieldName'] == "dateRange":
            if filter['dataType'] == "date" and (filter['searchType'] == ">=" or filter['searchType'] == "between"):
                if filter['searchType'] == ">=" :
                    if filter['value'] == 'T':
                        new_name += "TillDate"
                    else:
                        new_name  += f"L{filter['value']}Days"
                elif filter['searchType'] == "between":
                    if filter['dateRangeType'] == 'C':
                        new_name += f"{str(filter['value']).split(',')[0]}_To_{str(filter['value']).split(',')[1]}"
                    elif filter['dateRangeType'] == 'R':
                        new_name += f"L{str(filter['value']).split(',')[0]}Days_To_L{str(filter['value']).split(',')[1]}Days"
            #if touch_filter:
            #	new_name += f"TC_{filter['touchCount']}"
        break
    return new_name