set -euo pipefail

LPT=$1
channel=$2
request_id=$3
run_number=$4

if [ "$LPT" == "DATAOPS" ]; then
    homepath=/home/zxdev/zxcustom/DATAOPS/SUPPRESSION_REQUEST/
    app_db_name=CAMPAIGN_TOOL_QA
fi

request_path=$homepath/rlogs/$request_id/$run_number/
request_offer_log_path=$homepath/logs/$request_id/$run_number/

lpt_path=/home/zxdev/zxcustom/DATAOPS/SUPPRESSION_REQUEST/
python_path=/usr/local/bin/python3
python_packages_path=/usr/local/bin/python3
export PYTHONPATH=$python_packages_path
offer_supp_script=/home/zxdev/zxcustom/LptRequestProcess/download_and_upload_offers.py
offer_downloading_threads=$lpt_path/threads/offer_downloading/
file_writing_threads=$lpt_path/threads/file_writing/

mysql_path=/usr/bin/mysql
db_host=10.100.6.181
source_db_name=GR_TOOL_DB
db_user=zxdev
db_pass='zxdev12#$'

snowsql_path=/home/zxdev/snowsql/snowsql
snowflake_conn="lpt"
request_offer_table_schema="LPT_REQUESTS_DATA_QA"
match_or_supp_file_table_schema="LIST_PROCESSING_UAT"
offer_supp_schema="LIST_PROCESSING_UAT"

if [ "$channel" == "GREEN" ]; then
    if [ "$LPT" == "GMAIL" ]; then
        snowflake_WH="GREEN_LPT2"
    elif [ "$LPT" == "YAHOO" ]; then
        snowflake_WH="GREEN_LPT_Y"
    fi
    snowflake_sch="LIST_PROCESSING_UAT"
    responder_table="GREEN_LPT_RESPONDERS"
    opens_table="GREEN_LPT_QA.OPEN_DETAILS"
elif [ "$channel" == "INFS" ]; then
    snowflake_WH="GREEN_LPT_B"
    snowflake_sch="INFS_LPT_QA"
    responder_table="INFS_LPT_RESPONDERS"
    opens_table="OPEN_DETAILS_OTEAM"
else
    snowflake_WH="GREEN_LPT2"
    snowflake_sch="LIST_PROCESSING_UAT"
fi

offer_details_table=APT_SNOWFLAKE_OFFER_SUPPRESSION_DOWNLOAD_DETAILS
SUPPRESSION_REQUEST_OFFERS_TABLE=SUPPRESSION_REQUEST_OFFERS
SUPPRESSION_MATCH_DETAILED_STATS_TABLE=SUPPRESSION_MATCH_DETAILED_STATS
