
from serviceconfigurations import *
from basicudfs import *
from appudfs import *

class Dataset:
    def __init__(self):
        self.sources_loaded = []
        self.input_sources_count = 0
        self.consumer_kill_condition = False
        self.failed_sources_desc = ''
        self.sources_failed_count = 0
    def load_input_sources_producer(self,sources_queue, request_id, queue_empty_condition, thread_count, main_logger):
        # mysql connection closing
        main_logger.info(f"Producer execution started: {time.ctime()} ")
        main_logger.info(f"Acquiring mysql connection...")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        main_logger.info(f"Fetch data sources for the request id: {request_id}")
        main_logger.info(f"Executing query: {FETCH_SOURCE_DETAILS,(request_id,)}")
        mysql_cursor.execute(FETCH_SOURCE_DETAILS,(request_id,))
        data_sources = mysql_cursor.fetchall()
        self.input_sources_count = len(data_sources)
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
    def load_input_sources_consumer(self, sources_queue, main_request_details, queue_empty_condition,
                                   main_logger):
        try:
            main_logger.info(f"Consumer execution started: {time.ctime()}")
            while True:
                if not self.consumer_kill_condition:
                    with queue_empty_condition:
                        while sources_queue.empty():  # Wait for tasks to be available in the queue
                            queue_empty_condition.wait()
                        source = sources_queue.get()  # Get task from the queue
                        main_logger.info(f"Processing source: {str(source)}")
                    if source is None:  # Sentinel value indicating end of tasks
                        main_logger.info(f"Consumer execution ended: End of queue: {time.ctime()}")
                        break
                    main_logger.info("Calling function ... load_input_source")
                    self.sources_loaded.append(load_input_source("SUPPRESSION_DATASET", source, main_request_details))
                    sources_queue.task_done()  # Notify the queue that the task is done
                else:
                    break
                main_logger.info(f"Consumer exiting")
                print("Consumer exiting")
        except CustomError as e:
            self.consumer_kill_condition = True
            self.failed_sources_desc += str(e)
            self.sources_failed_count += 1
            raise CustomError(e)
        except Exception as e:
            self.consumer_kill_condition = True
            self.failed_sources_desc += str(e)
            self.sources_failed_count += 1
            raise CustomError('DO4', {'error': str(e)})


    # Main function
    def dataset_processor(self, request_id, run_number, schedule_time = None, notification_mails='', sendNotificationsFor= 'E'):
        try:
            recipient_mails = RECEPIENT_EMAILS + notification_mails.split(',')
            os.makedirs(f"{LOG_PATH}/{str(request_id)}/{str(run_number)}", exist_ok=True)
            main_logger = create_logger(f"request_{str(request_id)}_{str(run_number)}", log_file_path=f"{LOG_PATH}/{str(request_id)}/{str(run_number)}/", log_to_stdout=False)
            mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
            mysql_cursor = mysql_conn.cursor(dictionary=True)
            main_logger.info(f"Executing : {MAKE_SCHEDULE_IN_PROGRESS, (request_id, run_number)}")
            mysql_cursor.execute(MAKE_SCHEDULE_IN_PROGRESS, (request_id, run_number))
            main_logger.info(f"Executing : {UPDATE_SCHEDULE_STATUS, ('I','0','', request_id, run_number)}")
            mysql_cursor.execute(UPDATE_SCHEDULE_STATUS, ('I','0','', request_id, run_number))
            main_logger.info(f"Fetch main data source details, executing : {FETCH_MAIN_DATASET_DETAILS, (request_id, run_number)}")
            mysql_cursor.execute(FETCH_MAIN_DATASET_DETAILS, (request_id, run_number))
            main_request_details = mysql_cursor.fetchone()
            pid_file = PID_FILE.replace('REQUEST_ID',str(request_id))
            if os.path.exists(str(pid_file)):
                main_logger.info("Script execution is already in progress, hence skipping the execution.")
                raise CustomError('DO0', {'pidfile': str(pid_file)})
            Path(pid_file).touch()
            start_time = time.time()
            main_logger.info("Script Execution Started" + time.strftime("%H:%M:%S") + f" Epoch time: {start_time}")
            sources_queue = queue.Queue()
            queue_empty_condition = threading.Condition()
            # Preparing individuals tables for given data sources
            producer_thread = threading.Thread(target=self.load_input_sources_producer, args=(sources_queue, request_id, queue_empty_condition, THREAD_COUNT, main_logger))
            producer_thread.start()
            # Create and start consumer threads
            consumer_threads = []
            for i in range(THREAD_COUNT):
                #consumer_logger = create_logger(f"consumer_logger_{i}", log_to_stdout=True)
                consumer_thread = threading.Thread(target=self.load_input_sources_consumer, args=(
                    sources_queue, main_request_details, queue_empty_condition, main_logger))
                consumer_thread.start()
                time.sleep(10)
                consumer_threads.append(consumer_thread)
            # Wait for producer thread to finish
            producer_thread.join()

            # Wait for consumer threads to finish
            for consumer_thread in consumer_threads:
                consumer_thread.join()
            main_logger.info("Sources loaded: " + str(self.sources_loaded))
            if len(self.sources_loaded) != self.input_sources_count:
                main_logger.info(f"Out of {self.input_sources_count} input sources, {self.sources_failed_count} input sources are unable to process.")
                raise CustomError('DO1',{'n': str(self.input_sources_count), 'm': self.sources_failed_count, 'error': self.failed_sources_desc})
            main_logger.info("All sources are successfully processed.")
            # Preparing request level main_datasource
            ordered_sources_loaded = [x[0] for x in sorted(self.sources_loaded, key=lambda x: x[1])]
            schedule_status_value = create_main_datasource(ordered_sources_loaded, main_request_details, main_logger)
            update_next_schedule_due("SUPPRESSION_DATASET",request_id, run_number, main_logger, schedule_status_value)
            end_time = time.time()
            if sendNotificationsFor =="A":
                send_mail("DATASET", request_id, run_number,
                      EMAIL_SUBJECT.format(type_of_request="Dataset",request_name= str(main_request_details['name']),request_id= str(request_id)),
                      MAIL_BODY.format(channel=main_request_details['channelName'] ,type_of_request="Dataset", request_id=str(request_id),run_number=str(run_number), schedule_time=str(schedule_time),status=schedule_status_value,table=''),recipient_emails=recipient_mails)
            main_logger.info(f"Script execution ended: {time.strftime('%H:%M:%S')} epoch time: {end_time}")
            os.remove(pid_file)
        except Exception as e:
            main_logger.info(f"Exception occurred: {str(e)}" + str(traceback.format_exc()))
            error_desc = f"DO23: Unknown Exception occurred while processing the Dataset. Error: {str(e)}"
            mysql_cursor.execute(UPDATE_SCHEDULE_STATUS, ('E', '0', error_desc, request_id, run_number))
            update_next_schedule_due("SUPPRESSION_DATASET", request_id, run_number, main_logger)
            send_mail("DATASET", request_id, run_number,
                      ERROR_EMAIL_SUBJECT.format(type_of_request="Dataset",request_name= str(main_request_details['name']), request_id= str(request_id)),
                      MAIL_BODY.format(channel=main_request_details['channelName'] ,type_of_request="Dataset",request_id=str(request_id),run_number= str(run_number), schedule_time= str(schedule_time),
                                       status=f'E <br>Error Reason: {error_desc}',table=''),recipient_emails=recipient_mails)
            if "DO0: Error occured due to processing of another instance" not in str(e):
                os.remove(pid_file)
        finally:
            if 'connection' in locals() and mysql_conn.is_connected():
                mysql_cursor.close()
                mysql_conn.close()

if __name__ == "__main__":
    try:
        request_id = "76"
        run_number = "1"
        schedule_time = "2024-01-02 00:50:10"
        notification_mails = ""
        dataset_obj = Dataset()
        dataset_obj.dataset_processor(request_id, run_number, schedule_time, notification_mails)

    except Exception as e:
        print(f"Exception raised . Please look into this.... {str(e)}" + str(traceback.format_exc()))
        exit_program(-1)





