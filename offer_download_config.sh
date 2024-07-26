set -euo pipefail

LPT=$1
channel=$2
request_id=$3
run_number=$4

if [ "$LPT" == "DATAOPS" ]; then
    homepath=/gmservices/DATAOPS/SUPPRESSION_REQUEST/
    app_db_host='zds-prod-mdb-vip.bo3.e-dialog.com'
    app_db_name=GR_TOOL_DB
    app_db_user=techuser
    app_db_pass='tech12#$'
fi

request_path=$homepath/rlogs/$request_id/$run_number/
request_offer_log_path=$homepath/logs/$request_id/$run_number/

lpt_path=/gmservices/DATAOPS/SUPPRESSION_REQUEST/OFFER_DOWNLOADING_SERVICES/
python_path=/gmservices/DATAOPS/PYTHON_FILES/python37/bin/python3.7
python_packages_path=/gmservices/DATAOPS/PYTHON_FILES/site-packages
export PYTHONPATH=$python_packages_path
offer_supp_script=/u3/zx_tenant/zxcustom/lptServices/GlobalFpLptRequestProcess/download_and_upload_offers.py
offer_downloading_threads=$lpt_path/threads/offer_downloading/
file_writing_threads=$lpt_path/threads/file_writing/

mysql_path=/usr/bin/mysql
db_host='zds-prod-mdb-vip.bo3.e-dialog.com'
source_db_name=GR_TOOL_DB
db_user=techuser
db_pass='tech12#$'



snowsql_path=/usr/bin/snowsql
snowflake_conn="lpt"
request_offer_table_schema="LPT_REQUESTS_DATA"
match_or_supp_file_table_schema="LIST_PROCESSING"
offer_supp_schema="LIST_PROCESSING"

if [ "$channel" == "GREEN" ]; then
    if [ "$LPT" == "GMAIL" ]; then
        snowflake_WH="GREEN_LPT2"
    elif [ "$LPT" == "YAHOO" ]; then
        snowflake_WH="GREEN_LPT_Y"
    fi
    snowflake_sch="LIST_PROCESSING"
    responder_table="GREEN_LPT_RESPONDERS"
    opens_table="GREEN_LPT.OPEN_DETAILS"
elif [ "$channel" == "INFS" ]; then
    snowflake_WH="GREEN_LPT_B"
    snowflake_sch="INFS_LPT"
    responder_table="INFS_LPT_RESPONDERS"
    opens_table="OPEN_DETAILS_OTEAM"
else
    snowflake_WH="GREEN_LPT2"
    snowflake_sch="LIST_PROCESSING"
fi

offer_details_table=APT_SNOWFLAKE_OFFER_SUPPRESSION_DOWNLOAD_DETAILS
SUPPRESSION_REQUEST_OFFERS_TABLE=SUPPRESSION_REQUEST_OFFERS
SUPPRESSION_MATCH_DETAILED_STATS_TABLE=SUPPRESSION_MATCH_SUPPRESSION_BREAKDOWN_STATS
