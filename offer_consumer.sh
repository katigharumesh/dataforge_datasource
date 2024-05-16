request_id=$1
offer_id=$2
channel=$3
pid=$4
LPT=$5
schedule_id=$6
run_number=$7

source /home/zxdev/zxcustom/DATAOPS/SUPPRESSION_REQUEST/OFFER_DOWNLOADING_SERVICES/offer_download_config.sh $LPT $channel $request_id $run_number

set +euo pipefail

offer_download_status=""

mkdir -p $request_path/$offer_id/
>$request_path/$offer_id/offer_status

$mysql_path -u$db_user -p$db_pass -h$db_host -D$app_db_name -A -vv -e "delete from $SUPPRESSION_MATCH_DETAILED_STATS_TABLE where requestId=$request_id and requestScheduledId=$schedule_id and runNumber=$run_number and offerId=$offer_id;"

$mysql_path -u$db_user -p$db_pass -h$db_host -D$app_db_name -A -vv -e "UPDATE $SUPPRESSION_REQUEST_OFFERS_TABLE SET STATUS='IN_PROGRESS' where requestId=$request_id and requestScheduledId=$schedule_id and runNumber=$run_number and offerId=$offer_id "

is_sub_offer=$($mysql_path -u$db_user -p$db_pass -h$db_host -D$source_db_name -A -ss -e "select concat(count(SUB_OFFER_ID), '|',GROUP_CONCAT(SUB_OFFER_ID)) from OFFER_SUBOFFERS where CHANNEL='$channel' and OFFER_ID=$offer_id and STATUS='A';")

if [ "$is_sub_offer" != "NULL" ]; then
    sub_offer_ids=$(echo $is_sub_offer | cut -d'|' -f2)
    no_of_sub_offers=$(echo $is_sub_offer | cut -d'|' -f1)

    /usr/bin/sh -x $lpt_path/sub_offer_producer.sh "$offer_id" "$request_id" "$sub_offer_ids" "$no_of_sub_offers" "$channel" "$LPT" "$schedule_id" "$run_number"
else
    var=`$python_path $offer_supp_script "$offer_id" "$channel" | egrep 'Success|Failed'`
    echo $var >$request_path/$offer_id/offer_status

fi

offer_download_status=$(cat $request_path/$offer_id/offer_status)

if [[ "$offer_download_status" != "Success"* ]]; then
    echo "unable to download offer suppression"
    echo $offer_id >>$request_path/failed_offers
    $mysql_path -u$db_user -p$db_pass -h$db_host -D$app_db_name -A -ss -e "UPDATE $SUPPRESSION_REQUEST_OFFERS_TABLE SET STATUS='FAILED' where requestId=$request_id and requestScheduledId=$schedule_id and runNumber=$run_number and offerId=$offer_id "
    #$mysql_path -u$db_user -p$db_pass -h$db_host -D$app_db_name -A -ss -e "UPDATE APT_REQUEST_DETAILS SET ERROR_DESC =CONCAT(if(ERROR_DESC is null,'',ERROR_DESC),', OFFER FAILED : $offer_id ') where id=$request_id "
else
    echo $offer_id >>$request_path/success_offers
    offer_download_count=$(echo $offer_download_status | cut -d'|' -f3)
    offer_insert_count=$(echo $offer_download_status | cut -d'|' -f2)
    $mysql_path -u$db_user -p$db_pass -h$db_host -D$app_db_name -A -ss -e "UPDATE $SUPPRESSION_REQUEST_OFFERS_TABLE SET DOWNLOADEDCOUNT=$offer_download_count , INSERTEDCOUNT=$offer_insert_count ,STATUS='SUCCESS' where requestId=$request_id and requestScheduledId=$schedule_id and runNumber=$run_number and offerId=$offer_id "
    if [ "$is_sub_offer" == "NULL" ]; then
        $mysql_path -u$db_user -p$db_pass -h$db_host -D$app_db_name -A -ss -e "insert into $SUPPRESSION_MATCH_DETAILED_STATS_TABLE(requestId,requestScheduledId,runNumber,offerId,filterType,associateOfferId,downloadCount,insertCount) values ($request_id,$schedule_id,$run_number,$offer_id,'TEMPORARY',$offer_id,$offer_download_count,$offer_insert_count)"
    fi
    exit -1
fi

#touch $pid
