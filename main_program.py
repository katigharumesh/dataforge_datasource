
from serviceconfigurations import *
from basicudfs import *
from appudfs import *

global sources_loaded
sources_loaded = []
data_sources_count=0
consumer_kill_condition = False

def load_data_sources_producer(sources_queue, request_id, queue_empty_condition, thread_count, main_logger):
    # mysql connection closing
    global data_sources_count
    main_logger.info(f"Producer execution started: {time.ctime()} ")
    main_logger.info(f"Acquiring mysql connection...")
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
    mysql_cursor = mysql_conn.cursor(dictionary=True)
    main_logger.info(f"Fetch data sources for the request id: {request_id}")
    main_logger.info(f"Executing query: {FETCH_SOURCE_DETAILS,(request_id,'')}")
    mysql_cursor.execute(FETCH_SOURCE_DETAILS,(request_id,''))
    data_sources = mysql_cursor.fetchall()
    data_sources_count= len(data_sources)
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
def load_data_sources_consumer(sources_queue, main_datasource_details, queue_empty_condition,
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
                main_logger.info("Calling function ... load_data_source")
                sources_loaded.append(load_data_source(source, main_datasource_details))
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


# Main function
def main(request_id, run_number):
    try:
        os.makedirs(f"{LOG_PATH}/{str(request_id)}/{str(run_number)}", exist_ok=True)
        main_logger = create_logger(f"request_{str(request_id)}_{str(run_number)}", log_file_path=f"{LOG_PATH}/{str(request_id)}/{str(run_number)}/", log_to_stdout=True)
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        main_logger.info(f"Executing : {MAKE_SCHEDULE_IN_PROGRESS, (request_id, run_number)}")
        mysql_cursor.execute(MAKE_SCHEDULE_IN_PROGRESS, (request_id, run_number))
        main_logger.info(f"Executing : {UPDATE_SCHEDULE_STATUS, ('I','0','', request_id, run_number)}")
        mysql_cursor.execute(UPDATE_SCHEDULE_STATUS, ('I','0','', request_id, run_number))
        pid_file = PID_FILE.replace('REQUEST_ID',str(request_id))
        if os.path.exists(str(pid_file)):
            main_logger.info("Script execution is already in progress, hence skipping the execution.")
            send_skype_alert("Script execution is already in progress, hence skipping the execution.")
            mysql_cursor.execute(UPDATE_SCHEDULE_STATUS,('E', '0', 'Due to PID existence', request_id, run_number))
            update_next_schedule_due(request_id, run_number, main_logger)
            return
        Path(pid_file).touch()
        start_time = time.time()
        main_logger.info("Script Execution Started" + time.strftime("%H:%M:%S") + f" Epoch time: {start_time}")
        delete_old_files(LOG_PATH, main_logger, LOG_FILES_REMOVE_LIMIT)
        main_logger.info(f"Fetch main data source details, executing : {FETCH_MAIN_DATASOURCE_DETAILS,(request_id, run_number)}")
        mysql_cursor.execute(FETCH_MAIN_DATASOURCE_DETAILS,(request_id, run_number))
        main_datasource_details = mysql_cursor.fetchone()
        sources_queue = queue.Queue()
        queue_empty_condition = threading.Condition()
        # Preparing individuals tables for given data sources
        producer_thread = threading.Thread(target=load_data_sources_producer, args=(sources_queue, request_id, queue_empty_condition, THREAD_COUNT, main_logger))
        producer_thread.start()
        # Create and start consumer threads
        consumer_threads = []
        for i in range(THREAD_COUNT):
            #consumer_logger = create_logger(f"consumer_logger_{i}", log_to_stdout=True)
            consumer_thread = threading.Thread(target=load_data_sources_consumer, args=(
                sources_queue, main_datasource_details, queue_empty_condition, main_logger))
            consumer_thread.start()
            time.sleep(10)
            consumer_threads.append(consumer_thread)
        # Wait for producer thread to finish
        producer_thread.join()

        # Wait for consumer threads to finish
        for consumer_thread in consumer_threads:
            consumer_thread.join()
        print("sources loaded: " + str(sources_loaded))
        if len(sources_loaded) != data_sources_count:
            main_logger.info(f"Only {len(sources_loaded)} sources are successfully processed out of {data_sources_count} "
                             f"sources. Considering the datasource preparation request as failed.")
            print(f"Only {len(sources_loaded)} sources are successfully processed out of {data_sources_count} sources."
                  f" Considering the datasource preparation request as failed.")
            mysql_cursor.execute(DELETE_FILE_DETAILS, (main_datasource_details['dataSourceScheduleId'], main_datasource_details['runNumber']))
            mysql_cursor.execute(UPDATE_SCHEDULE_STATUS, ('E', '0', f'Only {len(sources_loaded)} sources are successfully processed out of {data_sources_count} sources.', request_id, run_number))
            update_next_schedule_due(request_id, run_number, main_logger)
            os.remove(pid_file)
            return
        main_logger.info("All sources are successfully processed.")
        print("All sources are successfully processed.")
        # Preparing request level main_datasource
        create_main_datasource(sources_loaded, main_datasource_details)
        update_next_schedule_due(request_id, run_number, main_logger)
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
        request_id = "2"
        run_number = "0"
        if len(sys.argv) > 1:
            request_id = str(sys.argv[1])
            run_number = str(sys.argv[2])
        main(request_id, run_number)

    except Exception as e:
        print(f"Exception raised . Please look into this.... {str(e)}" + str(traceback.format_exc()))
        exit_program(-1)


