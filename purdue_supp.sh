set -euo pipefail

INPUT_FILE = $1
CLEANSED_DATA_PATH = $2

mkdir -p $CLEANSED_DATA_PATH
rm -f CLEANSED_DATA_PATH/*

echo "Shell script start time :: " $(date)

if [ ! -s $INPUT_FILE ]; then
  echo "Please check whether input file is empty and try again..."
  exit -1
fi

cd $CLEANSED_DATA_PATH

link=$(curl --location --request POST 'https://mailer-api.optizmo.net/accesskey/cleanse/m-lnqm-k73-e93da0e8923e73e1baf46e6cfe9bcc54?token=F7m8ryNA7CSHJhNRAjuiBRMGunYPxfH9' --form "file=@${INPUT_FILE}" | awk -F'download_link":' '{print $2}' | awk -F'"' '{print $2}' | sed 's/"//g;s/{//g;s/}//g')

if [ "$link" == "" ]; then
  echo "Empty link returned"
  exit -1
fi

#echo $link

curl -m 900 -o A.zip --insecure -Lk $link
if [ $? -ne 0 ]; then
  echo "curl failed. Please try after sometime..."
  exit -1
fi

unzip A.zip
if [ $? -ne 0 ]; then
  echo "Unzip failed. Please try after sometime..."
  exit -1
fi

#suppressionfile=$(ls $SPOOLPATH/cleansed_list--PG_Unsubscribe_List_*.txt)

dos2unix $CLEANSED_DATA_PATH/*.txt
if [ $? -ne 0 ]; then
  echo "dos2unix failed. Please try after sometime..."
  exit -1
fi

