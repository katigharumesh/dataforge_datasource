#!/bin/bash
offer_id=$1
request_id=$2
sub_offer_ids=$3
no_of_sub_offers=$(($4 + 1))
channel=$5
LPT=$6
schedule_id=$7
run_number=$8

echo "Request_id : $request_id , Main-Offer : $offer_id : Sub-Offer_Producer start time: $(date)"

source /gmservices/DATAOPS/SUPPRESSION_REQUEST/OFFER_DOWNLOADING_SERVICES/offer_download_config.sh $LPT $channel $request_id $run_number

set +euo pipefail

main_offer_path=$homepath/rlogs/$request_id/$run_number/$offer_id/
log=$homepath/logs/$request_id/$run_number/$offer_id/

mkdir -p $main_offer_path
mkdir -p $log

echo $offer_id >$main_offer_path/offer_list
echo $sub_offer_ids | sed 's/,/\n/g' >>$main_offer_path/offer_list
>$main_offer_path/success_offers
>$main_offer_path/failed_offers

declare -a bg_jobs=()

while read sub_offer_id; do

    for (( ; ; )); do
        if [ $(ls $offer_downloading_threads | wc -l) -gt 1 ]; then
            filename=$(ls $offer_downloading_threads | head -1)
            rm -f $offer_downloading_threads/$filename
            /usr/bin/sh -x $lpt_path/sub_offer_consumer.sh "$sub_offer_id" "$offer_id" "$request_id" "$channel" "$offer_downloading_threads/$filename" "$LPT" "$schedule_id" "$run_number">>$log/$sub_offer_id.log 2>>$log/$sub_offer_id.log &
            bg_jobs+=($!)
            break
        fi
        sleep 5
    done

done <$main_offer_path/offer_list

sleep 5

for job_pid in "${bg_jobs[@]}"; do
    wait $job_pid
done

if [[ `cat $main_offer_path/success_offers|wc -l` -ne $no_of_sub_offers ]] || [[ -s $main_offer_path/failed_offers ]]
then
echo "SUB OFFERS DOWNLOAD FAILED">$main_offer_path/offer_status
exit 0
fi

cat $main_offer_path/*/offer_status >$main_offer_path/counts.txt

total_download_count=`awk -F'|' '{sum+=$3} END {print sum}' $main_offer_path/counts.txt`
total_inserted_count=`awk -F'|' '{sum+=$2} END {print sum}' $main_offer_path/counts.txt`

echo "Success|"$total_download_count"|"$total_inserted_count >$main_offer_path/offer_status

echo "Request_id : $request_id , Main-Offer : $offer_id : Sub-Offer_Producer end time: $(date)"
