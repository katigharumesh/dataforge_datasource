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
        sf_conn = snowflake.connector.connect(**snowflake_configs)
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
            if sf_account != snowflake_configs['account']:
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
                    f"create or replace transient table {snowflake_configs['database']}.{snowflake_configs['schema']}.{source_table} "
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
        sf_conn = snowflake.connector.connect(**snowflake_configs)
        sf_cursor = sf_conn.cursor()
        main_datasource_table = MAIN_DATASOURCE_TABLE_PREFIX + request_id
        temp_datasource_table = MAIN_DATASOURCE_TABLE_PREFIX + request_id + "_TEMP"
        sf_cursor.execute(f"create or replace transient table "
                          f"{snowflake_configs['database']}.{snowflake_configs['schema']}.{temp_datasource_table} as "
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

