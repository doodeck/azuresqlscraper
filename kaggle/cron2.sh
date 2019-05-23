#!/bin/bash

echo "cron running on `date`" # if anything is written directly from the script it's timepstamped
cd "$( dirname "${BASH_SOURCE[0]}" )"
logfile="`basename $0`.log"
. ../../bin/activate
echo "### Start >>> `date`" >> $logfile 2>&1
python nextbike-dataset-scraper-prod.py >> $logfile 2>&1
echo "### End <<< `date`" >> $logfile 2>&1
# exit

# Careful Azure VM is working in UTC
if (( `date +%H` == 0 && `date +%M` < 30)); then
  . ../credentials.py
  export PATH="$PATH:/opt/mssql-tools/bin" # apparently .bashrc and .bash-profile do not take effect from crontab
  for tbl in "countries" "cities" "places" "bike_list"
  do
    # echo $tbl
    sqlcmd -S "${server}" -d "${database}" -U "${username}" -P "${password}" -s',' -W -Q "set nocount on; SELECT * FROM NextBscraper.${tbl}" | grep -v '^\-\-\-\-,' > ./DB/${tbl}.csv
    done
  kaggle datasets version --message "cron update `date`" -p ./DB/ >> $logfile 2>&1
fi
