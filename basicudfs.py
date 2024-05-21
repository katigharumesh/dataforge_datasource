
from serviceconfigurations import *

def send_skype_alert(msg, channel=skype_configurations['default_channel']):
    try:
        random_number = random.randrange(100000, 1000000)
        current_milliseconds = int(time.time() * 1000)
        file = open(f"{skype_configurations['file_path']}/{str(current_milliseconds)}_{str(random_number)}.txt", "w")
        file.write(
            f"Hi Team \nPlease look into issue below.\nService: DATA FORGE\nScript_path:{skype_configurations['script_path']}\nScript_name={skype_configurations['script_name']}\nserver:{skype_configurations['server']}\nlogpath: {skype_configurations['log_path']}\nError:\n")
        file.write(str(msg))
        file.close()
        url = f"{skype_configurations['url']}{channel}"
        data = {
            "file": open(f"{skype_configurations['file_path']}/{str(current_milliseconds)}_{str(random_number)}.txt",
                         "rb")
        }
        response = requests.post(url, files=data)
    except Exception as e:
        print(str(e))




def create_logger(base_logger_name: object, log_file_path: object = LOG_PATH, log_to_stdout: object = False) -> object:
    # Append current date to the base logger name
    today_date = time.strftime("%Y%m%d")
    logger_name = f"{base_logger_name}_{today_date}"

    # Define a custom logging format
    log_format = '%(asctime)s - %(levelname)s - Thread:%(thread)d - %(name)s - Line:%(lineno)d - %(message)s'

    # Create a formatter with the custom format
    formatter = logging.Formatter(log_format)

    # Create a logger and set the formatter
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # Create a file handler, set the formatter, and add it to the logger
    file_handler = logging.FileHandler(str(log_file_path) + '/' + str(logger_name) + ".log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Check if log_to_stdout flag is True
    if log_to_stdout:
        # Create a StreamHandler to log to stdout
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger




def exit_program(code=-1,pid_file=PID_FILE):
    if os.path.exists(pid_file):
        os.remove(pid_file)
    sys.exit(code)


def delete_old_files(directory_path, main_logger, days_threshold=30):
    current_time = datetime.now()
    threshold_time = current_time - timedelta(days=days_threshold)

    # Walk through the directory and find files older than 'days_threshold' days
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            last_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))

            if last_modified_time < threshold_time:
                try:
                    os.remove(file_path)
                    main_logger.info("Deleted: "+file_path)
                except Exception as e:
                    main_logger.error("Error deleting "+file_path+" Error:"+str(e))




def send_mail(subject, message_body, sender_email=FROM_EMAIL, recipient_emails=RECEPIENT_EMAILS,message_type='html'):


    # Create a MIME multipart message
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = ', '.join(recipient_emails)
    message['Subject'] = subject

    # Add message body
    message.attach(MIMEText(message_body, message_type))
    with open (MAIL_HTML_FILE,"wb") as file:
        file.write(message_body)
    # Connect to localhost SMTP server
    with smtplib.SMTP('localhost', 25) as server:
        server.sendmail(sender_email, recipient_emails, message.as_string())

