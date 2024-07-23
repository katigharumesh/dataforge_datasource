from serviceconfigurations import *
from basicudfs import *
from appudfs import *

class Suppression_Request:
    def __init__(self):
        self.sources_loaded = []
        self.counts_before_filter = 0
        self.counts_after_filter = 0
        self.consumer_kill_condition = False
        self.failed_sources_desc = ''
        self.sources_failed_count = 0

    def load_input_sources_producer(self, sources_queue, supp_request_id, queue_empty_condition, thread_count, main_logger):
        # mysql connection closing
        main_logger.info(f"Producer execution started: {time.ctime()} ")
        main_logger.info(f"Acquiring mysql connection...")
        mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        main_logger.info(f"Fetch input sources for the request id: {supp_request_id}")
        main_logger.info(f"Executing query: {FETCH_SUPP_SOURCE_DETAILS,(supp_request_id,)}")
        mysql_cursor.execute(FETCH_SUPP_SOURCE_DETAILS,(supp_request_id,))
        data_sources = mysql_cursor.fetchall()
        self.input_sources_count = len(data_sources)
        main_logger.info(f"Here are the fetched Data Sources: {data_sources}")
        for source in data_sources:
            sources_queue.put(source)
        main_logger.info(f"Producer finished producing tasks")
        with queue_empty_condition:
            for _ in range(thread_count):  # Put sentinel value for each consumer
                sources_queue.put(None)  # Put sentinel value in the queue
            queue_empty_condition.notify_all()  # Notify all consumer threads
        main_logger.info(f"Producer Execution Ended: {time.ctime()} ")
        mysql_conn.close()


    # Consumer thread function
    def load_input_sources_consumer(self, sources_queue, main_request_details, queue_empty_condition,main_logger):
        try:
            main_logger.info(f"Consumer execution started: {time.ctime()}")
            while True:
                if not self.consumer_kill_condition:
                    with queue_empty_condition:
                        while sources_queue.empty():  # Wait for tasks to be available in the queue
                            queue_empty_condition.wait()
                        source = sources_queue.get()  # Get task from the queue
                        main_logger.info(f"Processing source : {str(source)}")
                    if source is None:  # Sentinel value indicating end of tasks
                        main_logger.info(f"Consumer execution ended: End of queue: {time.ctime()}")
                        break
                    main_logger.info("Calling function ... load_input_source")
                    self.sources_loaded.append(load_input_source("SUPPRESSION_REQUEST", source, main_request_details))
                    sources_queue.task_done()  # Notify the queue that the task is done
                else:
                    break
                main_logger.info(f"Consumer exiting")
        except CustomError as e:
            self.consumer_kill_condition = True
            self.failed_sources_desc += str(e)
            self.sources_failed_count += 1
            raise CustomError(e)
        except Exception as e:
            self.consumer_kill_condition = True
            self.failed_sources_desc += str(e)
            self.sources_failed_count += 1
            raise CustomError('DO4',{'error': str(e)})

    # Main function
    def suppression_request_processor(self, supp_request_id, run_number, schedule_time=None, notification_mails="",sendNotificationsFor="E"):
        try:
            recipient_emails = RECEPIENT_EMAILS + notification_mails.split(',')
            os.makedirs(f"{SUPP_LOG_PATH}/{str(supp_request_id)}/{str(run_number)}", exist_ok=True)
            main_logger = create_logger(f"supp_request_{str(supp_request_id)}_{str(run_number)}", log_file_path=f"{SUPP_LOG_PATH}/{str(supp_request_id)}/{str(run_number)}/", log_to_stdout=False)
            mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
            mysql_cursor = mysql_conn.cursor(dictionary=True)
            main_logger.info(f"Executing : {UPDATE_SUPP_SCHEDULE, ('I',supp_request_id, run_number)}")
            mysql_cursor.execute(UPDATE_SUPP_SCHEDULE, ('I', supp_request_id, run_number))
            main_logger.info(f"Executing : {UPDATE_SUPP_SCHEDULE_STATUS, ('I','0','', supp_request_id, run_number)}")
            mysql_cursor.execute(UPDATE_SUPP_SCHEDULE_STATUS, ('I', '0', '', supp_request_id, run_number))
            main_logger.info(f"Fetch suppression request details, executing : {FETCH_SUPP_REQUEST_DETAILS, (supp_request_id, run_number)}")
            mysql_cursor.execute(FETCH_SUPP_REQUEST_DETAILS, (supp_request_id, run_number))
            main_request_details = mysql_cursor.fetchone()
            main_logger.info(f"Fetched supp request details are: {main_request_details}")
            pid_file = SUPP_PID_FILE.replace('REQUEST_ID', str(supp_request_id))
            if os.path.exists(str(pid_file)):
                raise CustomError('DO0',{'pidfile': str(pid_file)})
            Path(pid_file).touch()
            start_time = time.time()
            main_logger.info("Script Execution Started " + time.strftime("%H:%M:%S") + f" Epoch time: {start_time}")

            # Fetching the configured Filters table
            if main_request_details['isCustomFilter']:
                filter_table = SUPPRESSION_REQUEST_FILTERS_TABLE
            else:
                filter_table = SUPPRESSION_PRESET_FILTERS_TABLE

            # Fetching request filter details
            main_logger.info(f"Fetching filter details, by executing: {FETCH_REQUEST_FILTER_DETAILS.format(filter_table,main_request_details['filterId'])}")
            mysql_cursor.execute(FETCH_REQUEST_FILTER_DETAILS.format(filter_table,main_request_details['filterId']))
            filter_details = mysql_cursor.fetchone()

            main_logger.info(f"Filter details: {str(filter_details)}")
            if not filter_details['isActive']:
                raise CustomError('DO5',{'Filter/Dataset': 'Filter', 'id': str(main_request_details['filterId'])})

            sources_queue = queue.Queue()
            queue_empty_condition = threading.Condition()
            # Preparing individuals tables for given data sources
            producer_thread = threading.Thread(target=self.load_input_sources_producer, args=(sources_queue, supp_request_id, queue_empty_condition, THREAD_COUNT, main_logger))
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
            # add the logic to add the data source tables to sources_loaded.
            main_logger.info("sources loaded: " + str(self.sources_loaded))
            if len(self.sources_loaded) != self.input_sources_count:
                main_logger.info(f"Out of {self.input_sources_count} input sources, {self.sources_failed_count} input sources are unable to process. ")
                raise CustomError('DO1',{'n': str(self.input_sources_count), 'm': self.sources_failed_count, 'error': self.failed_sources_desc})
            main_logger.info("All sources are successfully processed.")

            # Preparing request level main input source
            ordered_sources_loaded = [x for x in sorted(self.sources_loaded, key=lambda x: x[1])]
            current_count, main_request_table = create_main_input_source(ordered_sources_loaded, main_request_details, filter_details, main_logger)

            # Performing isps filtration
            if filter_details['id'] != 0:
                current_count = isps_filtration(current_count, main_request_table, filter_details['isps'], main_logger, mysql_cursor, main_request_details)

            # Profile non-match filtration
            current_count = profile_non_match_filtration(current_count, main_request_table, main_logger, mysql_cursor, main_request_details)

            # Channel level adhoc match files
            if filter_details['applyChannelFileMatch']:
                current_count = channel_adhoc_files_match_and_suppress("Match",filter_details, main_request_details, main_request_table, mysql_cursor, main_logger, current_count)

            # Channel level adhoc suppression files
            if filter_details['applyChannelFileSuppression']:
                current_count = channel_adhoc_files_match_and_suppress("Suppress",filter_details, main_request_details, main_request_table, mysql_cursor, main_logger, current_count)

            if filter_details['id'] != 0:
                # Jornaya match
                jornaya_match_details = json.loads(filter_details['matchJornayaData'])
                if jornaya_match_details['isEnabled']:
                    current_count = jornaya_and_mockingbird_match('Jornaya', current_count, main_request_table, main_logger, mysql_cursor, main_request_details, jornaya_match_details)

                # Mockingbird match
                mockingbird_match_details = json.loads(filter_details['matchMockingBirdData'])
                if mockingbird_match_details['isEnabled']:
                    current_count = jornaya_and_mockingbird_match('Mockingbird', current_count, main_request_table, main_logger, mysql_cursor, main_request_details, mockingbird_match_details)

                # Data Match Selection
                current_count = perform_match_or_filter_selection("Match",filter_details, main_request_details, main_request_table, mysql_cursor, main_logger, current_count)

                # Validate Remaining Data (Non-matched)
                if filter_details['outputRemainingData']:
                    current_count = validate_remaining_data(main_request_details, main_request_table, mysql_cursor, main_logger, current_count)

                # Data filter Selection
                current_count = perform_match_or_filter_selection("Suppression",filter_details, main_request_details, main_request_table, mysql_cursor, main_logger, current_count)

                # Performing channel suppression
                current_count = channel_suppression(main_request_details, filter_details, main_request_table, main_logger,
                                                    mysql_cursor)
                # Performing ZIPs suppression
                if filter_details['zipSuppression']:
                    current_count = state_and_zip_suppression('Zipcode Suppression', current_count, main_request_table,
                                                              filter_details['zipSuppression'], main_logger, mysql_cursor, main_request_details)

                # Performing States suppression
                if filter_details['stateSuppression']:
                    current_count = state_and_zip_suppression('State Suppression', current_count, main_request_table,
                                                              filter_details['stateSuppression'], main_logger, mysql_cursor, main_request_details)
            else:
                update_default_values('Match', main_request_table, main_logger)
            # Performing Purdue suppression
            if main_request_details['purdueSuppression']:
                current_count = purdue_suppression(main_request_details, main_request_table, main_logger, current_count)

            #Offer downloading and suppression
            if main_request_details['offerSuppressionIds'] is not None:
                main_logger.info(f"Acquiring Channel/Offer static files DB mysql connection")
                offer_files_db_conn = mysql.connector.connect(**CHANNEL_OFFER_FILES_DB_CONFIG)
                offer_files_db_cursor = offer_files_db_conn.cursor(dictionary=True)
                main_logger.info(f"Channel/Offer static files DB mysql connection acquired successfully...")
                offer_files_db_cursor.execute(FETCH_AFFILIATE_CHANNEL_VALUE, (main_request_details['channelName'],))
                affiliate_channel_details = offer_files_db_cursor.fetchone()
                main_logger.info(
                    f"Fetched affiliate channel details successfully, affiliate_channel_details: {affiliate_channel_details}")
                affiliate_channel = affiliate_channel_details['channelvalue']
                offer_table_prefix = affiliate_channel_details['table_prefix']
                main_logger.info(f"Closing Channel/Offer static files DB mysql connection")
                offer_files_db_cursor.close()
                offer_files_db_conn.close()
                main_logger.info("Request offers processing is initiated.")
                offers_list = str(main_request_details['offerSuppressionIds']).split(',')
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_OFFER_THREADS_COUNT) as executor:
                    futures = [executor.submit(offer_download_and_suppression, offer, main_request_details,
                                               filter_details, main_request_table, current_count, affiliate_channel,
                                               offer_table_prefix) for offer in offers_list]
                    for future in concurrent.futures.as_completed(futures):
                        main_logger.info(".. ")
                    main_logger.info("Request offers processing is completed.")
            else:
                main_logger.info(f"No offers are configured for suppression.")

            if filter_details['id'] != 0:
                #data append
                data_append(main_request_details,filter_details, main_request_table, main_logger, mysql_cursor)

            populate_stats_table(main_request_details, main_request_table, main_logger, mysql_cursor, run_number)

            input_sources = populate_input_sources_table(main_request_details, main_request_table, main_logger, mysql_cursor)

            if main_request_details['autoGenerateFiles']:
                populate_file_generation_details(main_request_details, main_logger, mysql_cursor, input_sources)

            main_logger.info("Fetching Error desc to find any failed files... ")
            main_logger.info(f"Executing query: {SUPP_FETCH_ERROR_MSG, (str(main_request_details['ScheduleId']), str(run_number))}")
            mysql_cursor.execute(SUPP_FETCH_ERROR_MSG, (str(main_request_details['ScheduleId']), str(run_number)))
            error_desc_dict = mysql_cursor.fetchone()
            main_logger.info("Fetching failed offer details, if any... ")
            main_logger.info(f"Executing query: {FETCH_FAILED_OFFERS, (main_request_details['id'], run_number)}")
            mysql_cursor.execute(FETCH_FAILED_OFFERS, (main_request_details['id'], run_number))
            failed_offer_dict = mysql_cursor.fetchone()
            if failed_offer_dict['failed_offers'] is not None:
                if len(str(failed_offer_dict['failed_offers']).split(",")) == len(str(main_request_details['offerSuppressionIds']).split(",")):
                    raise CustomError('DO28')
            if error_desc_dict['error_msg'] is not None or failed_offer_dict['failed_offers'] is not None:
                main_logger.info(f"Fetched Error message is :: {error_desc_dict['error_msg']}")
                main_logger.info(f"Fetched failed offer details :: {failed_offer_dict['failed_offers']}")
                schedule_status_value = 'P'
                mysql_cursor.execute(UPDATE_SUPP_SCHEDULE_STATUS, (schedule_status_value,current_count, '', supp_request_id, run_number))
            else:
                schedule_status_value = 'C'
                main_logger.info(f"Executing: {UPDATE_SUPP_SCHEDULE_STATUS, (schedule_status_value, current_count, '', supp_request_id, run_number)}")
                mysql_cursor.execute(UPDATE_SUPP_SCHEDULE_STATUS,(schedule_status_value, current_count, '', supp_request_id, run_number))
            update_next_schedule_due("SUPPRESSION_REQUEST", supp_request_id, run_number, main_logger,schedule_status_value)
            if sendNotificationsFor == "A":
                send_mail("SUPP", supp_request_id, run_number, EMAIL_SUBJECT.format(type_of_request="Suppression Request", request_name=str(main_request_details['name']), request_id= str(supp_request_id)),
                      MAIL_BODY.format(channel=main_request_details['channelName'] ,type_of_request= "Suppression Request",request_id= str(supp_request_id),run_number= str(run_number),schedule_time= str(schedule_time),
                                       status =schedule_status_value, table= add_table(main_request_details,run_number)), recipient_emails=recipient_emails)
            end_time = time.time()
            main_logger.info(f"Script execution ended: {time.strftime('%H:%M:%S')} epoch time: {end_time}")
            os.remove(pid_file)
        except Exception as e:
            main_logger.info(f"Exception occurred: {str(e)}" + str(traceback.format_exc()))
            error_desc = str(e)
            mysql_conn = mysql.connector.connect(**MYSQL_CONFIGS)
            mysql_cursor = mysql_conn.cursor(dictionary=True)
            mysql_cursor.execute(UPDATE_SUPP_SCHEDULE_STATUS,('E', '0', error_desc, supp_request_id, run_number))
            update_next_schedule_due("SUPPRESSION_REQUEST", supp_request_id, run_number, main_logger)
            send_mail("SUPP", supp_request_id, run_number, ERROR_EMAIL_SUBJECT.format(type_of_request= "Suppression Request",request_name=  str(main_request_details['name']), request_id= str(supp_request_id)),
                      MAIL_BODY.format(channel=main_request_details['channelName'] ,type_of_request= "Suppression Request",request_id= str(supp_request_id),run_number= str(run_number),schedule_time= str(schedule_time),
                                       status= f"E <br>Error Reason: {error_desc}", table=''),recipient_emails=recipient_emails)
            if "Error occured due to processing of another instance" not in str(e):
                os.remove(pid_file)
        finally:
            if 'connection' in locals() and mysql_conn.is_connected():
                mysql_cursor.close()
                mysql_conn.close()

if __name__ == "__main__":
    try:
        supp_request_id = "16"
        run_number = "1"
        schedule_time = "2024-01-02 00:50:10"
        notification_mails = "glenka@aptroid.com"
        sendNotificationsFor = "E"
        wasInActive = 0
        supp_obj = Suppression_Request()
        supp_obj.suppression_request_processor(supp_request_id, run_number, schedule_time, notification_mails,sendNotificationsFor,wasInActive)


    except Exception as e:
        print(f"Exception raised . Please look into this.... {str(e)}" + str(traceback.format_exc()))
        exit_program(-1)



