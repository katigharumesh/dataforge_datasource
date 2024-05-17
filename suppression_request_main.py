import queue

from serviceconfigurations import *
from basicudfs import *
from appudfs import *

global sources_loaded
global counts_before_filter
global counts_after_filter
counts_before_filter = 0
counts_after_filter = 0
sources_loaded = []
input_sources_count = 0
consumer_kill_condition = False
filter_and_match_file_sources_consumer_kill_condition = False
global match_or_filter_file_sources_loaded
match_or_filter_file_sources_loaded = []
def load_input_sources_producer(sources_queue, supp_request_id, queue_empty_condition, thread_count, main_logger):
    # mysql connection closing
    global input_sources_count
    main_logger.info(f"Producer execution started: {time.ctime()} ")
    main_logger.info(f"Acquiring mysql connection...")
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
    mysql_cursor = mysql_conn.cursor(dictionary=True)
    main_logger.info(f"Fetch input sources for the request id: {supp_request_id}")
    main_logger.info(f"Executing query: {FETCH_SUPP_SOURCE_DETAILS,(supp_request_id,)}")
    mysql_cursor.execute(FETCH_SUPP_SOURCE_DETAILS,(supp_request_id,))
    data_sources = mysql_cursor.fetchall()
    input_sources_count = len(data_sources)
    main_logger.info(f"Here are the fetched Data Sources: {data_sources}")
    for source in data_sources:
        sources_queue.put(source)
    main_logger.info(f"Producer finished producing tasks")
    print("Producer finished producing tasks")
    with queue_empty_condition:
        for _ in range(thread_count):  # Put sentinel value for each consumer
            sources_queue.put(None)  # Put sentinel value in the queue
        queue_empty_condition.notify_all()  # Notify all consumer threads
    main_logger.info(f"Producer Execution Ended: {time.ctime()} ")
    mysql_conn.close()


# Consumer thread function
def load_input_sources_consumer(sources_queue, main_request_details, queue_empty_condition,
                               main_logger):
    try:
        global consumer_kill_condition
        main_logger.info(f"Consumer execution started: {time.ctime()}")
        while True:
            if not consumer_kill_condition:
                with queue_empty_condition:
                    while sources_queue.empty():  # Wait for tasks to be available in the queue
                        queue_empty_condition.wait()
                    source = sources_queue.get()  # Get task from the queue
                    main_logger.info(f"Processing source : {str(source)}")
                if source is None:  # Sentinel value indicating end of tasks
                    main_logger.info(f"Consumer execution ended: End of queue: {time.ctime()}")
                    break
                main_logger.info("Calling function ... load_input_source")
                sources_loaded.append(load_input_source("SUPPRESSION_REQUEST" ,source, main_request_details))
                sources_queue.task_done()  # Notify the queue that the task is done
            else:
                break
            main_logger.info(f"Consumer exiting")
            print("Consumer exiting")
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        # update status to error
        main_logger.error(f"Exception occurred: {str(e)}")
        consumer_kill_condition = True

def filter_and_match_file_sources_producer(type_of_request, file_source_queue, file_source_details, match_or_filter_queue_empty_condition, thread_count, main_logger):
    main_logger.info(f"Processing producer for {type_of_request}")
    main_logger.info(F" filter_and_match_file_sources_producer Execution Started: {time.ctime()}")
    for i in range(len(file_source_details)):
        file_source_queue.put(tuple([i, file_source_details[i]]))
    main_logger.info(f"filter_and_match_file_sources_producer finished producing tasks")
    print("filter_and_match_file_sources_producer finished producing tasks")
    with match_or_filter_queue_empty_condition:
        for _ in range(thread_count):  # Put sentinel value for each consumer
            file_source_queue.put(None)  # Put sentinel value in the queue
        match_or_filter_queue_empty_condition.notify_all()  # Notify all consumer threads
    main_logger.info(f"filter_and_match_file_sources_producer Execution Ended: {time.ctime()} ")



def filter_and_match_file_sources_consumer(type_of_request, file_source_queue, match_or_filter_queue_empty_condition, main_logger, main_request_details):
    try:
        main_logger.info(f"Processing consumers for {type_of_request}")
        global filter_and_match_file_sources_consumer_kill_condition
        main_logger.info(f"Consumer execution started: {time.ctime()}")
        while True:
            if not filter_and_match_file_sources_consumer_kill_condition:
                with match_or_filter_queue_empty_condition:
                    while file_source_queue.empty():  # Wait for tasks to be available in the queue
                        match_or_filter_queue_empty_condition.wait()
                    file_source_tuple = file_source_queue.get()
                    file_source_index = int(file_source_tuple[0])
                    source_dict = file_source_tuple[1]
                    file_source = source_dict  # Get task from the queue
                    main_logger.info(f"Processing source : {str(file_source)}")
                if file_source is None:  # Sentinel value indicating end of tasks
                    main_logger.info(f"Consumer execution ended: End of queue: {time.ctime()}")
                    break
                main_logger.info("Calling function ... load_match_or_filter_file_sources(type_of_request, file_source)")
                match_or_filter_file_sources_loaded.append(load_match_or_filter_file_sources(type_of_request, file_source, file_source_index, main_request_details))
                file_source_queue.task_done()  # Notify the queue that the task is done
            else:
                break
            main_logger.info(f"Consumer exiting")
            print("Consumer exiting")
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        # update status to error
        main_logger.error(f"Exception occurred: {str(e)}")
        filter_and_match_file_sources_consumer_kill_condition = True

def perform_match_or_filter_selection(type_of_request,filter_details, main_request_details, main_request_table ,pid_file, mysql_cursor, main_logger, current_count):
    if type_of_request == "SUPPRESS_MATCH":
        key_to_fetch = 'matchedDataSources'
        channel_level_filter = filter_details['applyChannelFileMatch']
        channel_file_type = 'M'
        channel_filter_name = 'Channel_File_Match'
    if type_of_request == "SUPPRESS_FILTER":
        key_to_fetch = 'filterDataSources'
        channel_level_filter = filter_details['applyChannelFileSuppression']
        channel_file_type = 'S'
        channel_filter_name = 'Channel_File_Suppression'
    match_or_filter_source_details = json.loads(str(filter_details[key_to_fetch]))
    sorted_match_or_filter_sources_loaded = []
    if len(match_or_filter_source_details) == 0 and channel_level_filter == 0:
        main_logger.info(f"No {type_of_request} sources are chosen and channel level files filter is also not selected. Returning to main")
        if type_of_request == "SUPPRESS_MATCH":
            update_default_values(type_of_request, main_request_table, main_logger)
        return current_count
    if channel_level_filter:
        channel_files_db_conn = mysql.connector.connect(**CHANNEL_OFFER_FILES_DB_CONFIG)
        channel_files_db_cursor = channel_files_db_conn.cursor(dictionary=True)
        channel_files_db_cursor.execute(f"select TABLE_NAME,concat(FILENAME,DOWNLOAD_COUNT,INSERT_COUNT),'{channel_filter_name}' from"
                                        f" SUPPRESSION_MATCH_FILES where FILE_TYPE='{channel_file_type}' and  STATUS='A' and ID in "
                                        f"(select FILE_ID from OFFER_CHANNEL_SUPPRESSION_MATCH_FILES where "
                                        f"CHANNEL='{main_request_table['channelName']}' and PROCESS_TYPE='C' and STATUS='A')")
        sorted_match_or_filter_sources_loaded += channel_files_db_cursor.fetchall()
    if len(match_or_filter_source_details) != 0:
        match_or_filter_file_source_details = list(match_or_filter_source_details['FileSource'])
        match_or_filter_file_source_queue = queue.Queue()
        match_or_filter_queue_empty_condition = threading.Condition()
        producer_thread = threading.Thread(target=filter_and_match_file_sources_producer, args=(type_of_request, match_or_filter_file_source_queue, match_or_filter_file_source_details, match_or_filter_queue_empty_condition, THREAD_COUNT, main_logger))
        producer_thread.start()
        consumer_threads = []
        for i in range(THREAD_COUNT):
            # consumer_logger = create_logger(f"consumer_logger_{i}", log_to_stdout=True)
            consumer_thread = threading.Thread(target=filter_and_match_file_sources_consumer, args=(
                type_of_request, match_or_filter_file_source_queue, match_or_filter_queue_empty_condition, main_logger, main_request_details))
            consumer_thread.start()
            time.sleep(10)
            consumer_threads.append(consumer_thread)
        # Wait for producer thread to finish
        producer_thread.join()

        # Wait for consumer threads to finish
        for consumer_thread in consumer_threads:
            consumer_thread.join()
        print("match_or_filter_file_sources_loaded : " + str(match_or_filter_file_sources_loaded))
        if len(match_or_filter_file_sources_loaded) != len(match_or_filter_file_source_details):
            main_logger.info(
                f"Only {len(match_or_filter_file_sources_loaded)} {type_of_request} sources are successfully processed out of {len(match_or_filter_file_source_details)} sources. Considering the suppression request as failed.")
            print(
                f"Only {len(match_or_filter_file_sources_loaded)} {type_of_request} sources are successfully processed out of {len(match_or_filter_file_source_details)} sources. Considering the suppression request as failed.")
            mysql_cursor.execute(DELETE_FILE_DETAILS,
                                 (main_request_details['ScheduleId'], main_request_details['runNumber']))
            mysql_cursor.execute(UPDATE_SCHEDULE_STATUS, ('E', '0',
                                                          f'Only {len(match_or_filter_file_sources_loaded)} sources are successfully processed out of {len(match_or_filter_file_source_details)} sources.',supp_request_id, run_number))
            update_next_schedule_due(supp_request_id, run_number, main_logger)
            os.remove(pid_file)
            return
        main_logger.info(
            f" Match/Filter file sources are created successfully.. here are details for those tables, {str(match_or_filter_file_sources_loaded)}")
        sorted_match_or_filter_sources_loaded += [tuple([t[1], t[2], 'FileSource']) for t in sorted(match_or_filter_file_sources_loaded, key=lambda t: t[0])]
        if 'DataSource' in match_or_filter_source_details.keys():
            data_source_filter_list = list(match_or_filter_source_details['DataSource'])
            for i in data_source_filter_list:
                data_source_details_dict = json.loads(str(i))
                data_source_table_name = data_source_input(type_of_request, data_source_details_dict['dataSourceId'], mysql_cursor, main_logger)
                columns = data_source_details_dict['columns']
                sorted_match_or_filter_sources_loaded.append(tuple([data_source_table_name, columns, 'DataSource']))
        if 'ByField' in match_or_filter_source_details.keys():
            for filt in list(match_or_filter_source_details['ByField']):
                sorted_match_or_filter_sources_loaded.append(tuple(['', filt, 'ByField']))
    current_count = perform_filter_or_match(type_of_request, main_request_details, main_request_table, sorted_match_or_filter_sources_loaded, mysql_cursor, main_logger, current_count)

    main_logger.info(f"All {type_of_request} sources are successfully processed.")
    print(f"All {type_of_request} sources are successfully processed.")
    main_logger.info(f"Data {type_of_request} Success")
    return current_count


# Main function
def main(supp_request_id, run_number):
    try:
        os.makedirs(f"{SUPP_LOG_PATH}/{str(supp_request_id)}/{str(run_number)}", exist_ok=True)
        main_logger = create_logger(f"supp_request_{str(supp_request_id)}_{str(run_number)}", log_file_path=f"{SUPP_LOG_PATH}/{str(supp_request_id)}/{str(run_number)}/", log_to_stdout=True)
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        main_logger.info(f"Executing : {UPDATE_SUPP_SCHEDULE, ('I',supp_request_id, run_number)}")
        mysql_cursor.execute(UPDATE_SUPP_SCHEDULE, ('I', supp_request_id, run_number))
        main_logger.info(f"Executing : {UPDATE_SUPP_SCHEDULE_STATUS, ('I','0','', supp_request_id, run_number)}")
        mysql_cursor.execute(UPDATE_SUPP_SCHEDULE_STATUS, ('I', '0', '', supp_request_id, run_number))
        pid_file = SUPP_PID_FILE.replace('REQUEST_ID', str(supp_request_id))
        if os.path.exists(str(pid_file)):
            main_logger.info("Script execution is already in progress, hence skipping the execution.")
            send_skype_alert("Script execution is already in progress, hence skipping the execution.")
            mysql_cursor.execute(UPDATE_SUPP_SCHEDULE_STATUS, ('E', '0', 'Due to PID existence', supp_request_id, run_number))
            update_next_schedule_due(supp_request_id, run_number, main_logger) # FOR SUPP
            return
        Path(pid_file).touch()
        start_time = time.time()
        main_logger.info("Script Execution Started " + time.strftime("%H:%M:%S") + f" Epoch time: {start_time}")
        delete_old_files(SUPP_LOG_PATH, main_logger, LOG_FILES_REMOVE_LIMIT)
        main_logger.info(f"Fetch suppression request details, executing : {FETCH_SUPP_REQUEST_DETAILS,(supp_request_id, run_number)}")
        mysql_cursor.execute(FETCH_SUPP_REQUEST_DETAILS,(supp_request_id, run_number))
        main_request_details = mysql_cursor.fetchone()
        sources_queue = queue.Queue()
        queue_empty_condition = threading.Condition()
        # Preparing individuals tables for given data sources
        producer_thread = threading.Thread(target=load_input_sources_producer, args=(sources_queue, supp_request_id, queue_empty_condition, THREAD_COUNT, main_logger))
        producer_thread.start()
        # Create and start consumer threads
        consumer_threads = []
        for i in range(THREAD_COUNT):
            #consumer_logger = create_logger(f"consumer_logger_{i}", log_to_stdout=True)
            consumer_thread = threading.Thread(target=load_input_sources_consumer, args=(
                sources_queue, main_request_details, queue_empty_condition, main_logger))
            consumer_thread.start()
            time.sleep(10)
            consumer_threads.append(consumer_thread)
        # Wait for producer thread to finish
        producer_thread.join()

        # Wait for consumer threads to finish
        for consumer_thread in consumer_threads:
            consumer_thread.join()
        # add the logic to add the data source tables to sources_loaded.
        print("sources loaded: " + str(sources_loaded))
        if len(sources_loaded) != input_sources_count:
            main_logger.info(f"Only {len(sources_loaded)} sources are successfully processed out of {input_sources_count} "
                             f"sources. Considering the suppression request as failed.")
            print(f"Only {len(sources_loaded)} sources are successfully processed out of {input_sources_count} sources."
                  f" Considering the suppression request as failed.")
            mysql_cursor.execute(DELETE_FILE_DETAILS, (main_request_details['ScheduleId'], main_request_details['runNumber']))
            mysql_cursor.execute(UPDATE_SCHEDULE_STATUS, ('E', '0', f'Only {len(sources_loaded)} sources are successfully processed out of {input_sources_count} sources.', supp_request_id, run_number))
            update_next_schedule_due(supp_request_id, run_number, main_logger)
            os.remove(pid_file)
            return
        main_logger.info("All sources are successfully processed.")
        print("All sources are successfully processed.")
        # Preparing request level main input source
        ordered_sources_loaded = [x for x in sorted(sources_loaded, key=lambda x: x[1])]
        current_count, main_request_table = create_main_input_source(ordered_sources_loaded, main_request_details)
        # Fetching the configured Filters table
        if main_request_details['isCustomFilter']:
            filter_table = SUPPRESSION_REQUEST_FILTERS_TABLE
        else:
            filter_table = SUPPRESSION_PRESET_FILTERS_TABLE
        # Fetching request filter details
        main_logger.info(f"Fetching filter details, by executing : {FETCH_REQUEST_FILTER_DETAILS.format(filter_table,main_request_details['filterId'])}")
        mysql_cursor.execute(FETCH_REQUEST_FILTER_DETAILS.format(filter_table,main_request_details['filterId']))
        filter_details = mysql_cursor.fetchone()
        main_logger.info(f"Filter details : {str(filter_details)}")
        # Performing isps filtration
        current_count = isps_filteration(current_count, main_request_table, filter_details['isps'], main_logger, mysql_cursor, main_request_details)

        # Data Match Selection
        current_count = perform_match_or_filter_selection("SUPPRESS_MATCH",filter_details, main_request_details, main_request_table ,pid_file, mysql_cursor, main_logger, current_count)

        #Data filter Selection
        current_count = perform_match_or_filter_selection("SUPPRESS_FILTER",filter_details, main_request_details, main_request_table ,pid_file, mysql_cursor, main_logger, current_count)

        #Offer downloading and suppression
        if filter_details['offerSuppression']:
            main_logger.info("Request offers processing is initiated.")
            mysql_cursor.execute(FETCH_REQUEST_OFFERS, (main_request_details['id'], main_request_details['ScheduleId'], main_request_details['runNumber']))
            offers_list = mysql_cursor.fetchall()
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_OFFER_THREADS_COUNT) as executor:
                futures = [executor.submit(offer_download_and_suppression, offer, main_request_details, filter_details, mysql_cursor, main_request_table, current_count) for offer in offers_list]
                for future in concurrent.futures.as_completed(futures):
                    main_logger.info("Request offers processing is completed.")


        #data append
        data_append(filter_details, main_request_table , main_logger)
        update_next_schedule_due(supp_request_id, run_number, main_logger)
        end_time = time.time()
        main_logger.info(f"Script execution ended: {time.strftime('%H:%M:%S')} epoch time: {end_time}")
        os.remove(pid_file)
    except Exception as e:
        main_logger.info(f"Exception occurred in main: Please look into this. {str(e)}" + str(traceback.format_exc()))
        os.remove(pid_file)
    finally:
        if 'connection' in locals() and mysql_conn.is_connected():
            mysql_cursor.close()
            mysql_conn.close()

if __name__ == "__main__":
    try:
        supp_request_id = "10"
        run_number = "0"
        if len(sys.argv) > 1:
            supp_request_id = str(sys.argv[1])
            run_number = str(sys.argv[2])
        main(supp_request_id, run_number)

    except Exception as e:
        print(f"Exception raised . Please look into this.... {str(e)}" + str(traceback.format_exc()))
        exit_program(-1)

