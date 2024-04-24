from serviceconfigurations import *
from basicudfs import *
from appudfs import *

global sources_loaded
sources_loaded = []
data_sources_count=0

def load_data_sources_producer(sources_queue, request_id, queue_empty_condition, thread_count, main_logger):
    global data_sources_count
    main_logger.info(f"Producer execution started: {time.ctime()} ")
    main_logger.info(f"Acquiring mysql connection...")
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
    mysql_cursor = mysql_conn.cursor(dictionary=True)
    main_logger.info(f"Fetch data sources for the request id: {request_id}")
    main_logger.info(f"executing query: {FETCH_SOURCE_DETAILS.replace('REQUEST_ID', request_id)}")
    mysql_cursor.execute(FETCH_SOURCE_DETAILS.replace("REQUEST_ID", request_id))
    data_sources = mysql_cursor.fetchall()
    data_sources_count=len(data_sources)
    main_logger.info(f"Here are the fetched Data Sources: {data_sources}")
    for source in data_sources:
        sources_queue.put(source)
    main_logger.info(f"Producer finished producing tasks")
    print("Producer finished producing tasks")
    with queue_empty_condition:
        for _ in range(thread_count):  # Put sentinel value for each consumer
            sources_queue.put(None)  # Put sentinel value in the queue
        queue_empty_condition.notify_all()  # Notify all consumer threads
    main_logger.info(f"Producer execution ended: {time.ctime()} ")


# Consumer thread function
def load_data_sources_consumer(sources_queue, main_datasource_details, queue_empty_condition,
                               consumer_logger):
    try:
        consumer_logger.info(f"Consumer execution started: {time.ctime()}")
        while True:
            with queue_empty_condition:
                while sources_queue.empty():  # Wait for tasks to be available in the queue
                    queue_empty_condition.wait()
                source = sources_queue.get()  # Get task from the queue
                consumer_logger.info(str(source))
            if source is None:  # Sentinel value indicating end of tasks
                consumer_logger.info(f"Consumer execution ended: End of queue: {time.ctime()}")
                break
            consumer_logger.info("Calling function ... load_data_source")
            sources_loaded.append(load_data_source(source, main_datasource_details, consumer_logger))
            sources_queue.task_done()  # Notify the queue that the task is done
        consumer_logger.info(f"Consumer exiting")
        print("Consumer exiting")
    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        # update status to error
        consumer_logger.error(f"Exception occurred: {str(e)}")


# Main function
def main(request_id, run_number):
    main_logger = create_logger("main_program", log_to_stdout=True)
    if os.path.exists(PID_FILE):
        main_logger.info("Script execution is already in progress, hence skipping the execution.")
        send_skype_alert("Script execution is already in progress, hence skipping the execution.")
        sys.exit(-1)
    Path(PID_FILE).touch()
    start_time = time.time()
    main_logger.info("Script Execution Started" + time.strftime("%H:%M:%S") + f" epoch time: {start_time}")
    delete_old_files(LOG_PATH, main_logger, LOG_FILES_REMOVE_LIMIT)
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
    mysql_cursor = mysql_conn.cursor(dictionary=True)
    mysql_cursor.execute(FETCH_MAIN_DATASOURCE_DETAILS.replace("REQUEST_ID", request_id).replace("RUN_NUMBER", run_number))
    main_datasource_details = mysql_cursor.fetchone()
    sources_queue = queue.Queue()
    queue_empty_condition = threading.Condition()
    # Preparing individuals tables for given data sources
    producer_thread = threading.Thread(target=load_data_sources_producer, args=
    (sources_queue, request_id, queue_empty_condition, THREAD_COUNT, main_logger))
    producer_thread.start()
    # Create and start consumer threads
    consumer_threads = []
    for i in range(THREAD_COUNT):
        consumer_logger = create_logger(f"consumer_logger_{i}", log_to_stdout=True)
        consumer_thread = threading.Thread(target=load_data_sources_consumer, args=(
            sources_queue, main_datasource_details, queue_empty_condition, consumer_logger))
        consumer_thread.start()
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
        mysql_cursor.execute(ERROR_SCHEDULE_STATUS, (main_datasource_details['dataSourceScheduleId'], main_datasource_details['runNumber']))
        exit_program(-1)
    main_logger.info("All sources are successfully processed.")
    print("All sources are successfully processed.")
    # Preparing request level main_datasource
    create_main_datasource(sources_loaded, main_datasource_details)
    end_time = time.time()
    main_logger.info(f"Script execution ended: {time.strftime('%H:%M:%S')} epoch time: {end_time}")
    exit_program(0)



if __name__ == "__main__":
    try:
        request_id = "46"
        run_number = "1"
        if len(sys.argv) > 1:
            request_id = str(sys.argv[1])
            run_number = str(sys.argv[2])
        main(request_id, run_number)

    except Exception as e:
        print(f"Exception raised . Please look into this.... {str(e)}")
        exit_program(-1)

