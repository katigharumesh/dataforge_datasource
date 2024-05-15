sub_offer_id=$1
offer_id=$2
request_id=$3
channel=$4
pid=$5
LPT=$6
schedule_id=$7
run_number=$8

source /home/zxdev/zxcustom/DATAOPS/REQUESTS/offer_download_config.sh $LPT $channel $request_id $run_number

set +euo pipefail

offer_download_status=""
main_offer_path=$homepath/rlogs/$request_id/$run_number/$offer_id/
sub_offer_path=$main_offer_path/$sub_offer_id/
mkdir -p $main_offer_path
mkdir -p $sub_offer_path

>$sub_offer_path/offer_status

var=`$python_path $offer_supp_script "$sub_offer_id" "$channel" | egrep 'Success|Failed'`
echo $var >$sub_offer_path/offer_status

offer_download_status=$(cat $sub_offer_path/offer_status)

if [[ "$offer_download_status" != "Success"* ]]; then
    echo "unable to download offer suppression"
    echo $offer_id >>$main_offer_path/failed_offers
else
    echo $offer_id >>$main_offer_path/success_offers
    offer_download_count=$(echo $offer_download_status | cut -d'|' -f3)
    offer_insert_count=$(echo $offer_download_status | cut -d'|' -f2)
    $mysql_path -u$db_user -p$db_pass -h$db_host -D$app_db_name -A -ss -e "insert into $SUPPRESSION_MATCH_DETAILED_STATS_TABLE(requestId,requestScheduledId,runNumber,offerId,filterType,associateOfferId,downloadCount,insertCount) values ($request_id,$schedule_id,$run_number,$offer_id,'TEMPORARY',$sub_offer_id,$offer_download_count,$offer_insert_count)"

fi

touch $pid
