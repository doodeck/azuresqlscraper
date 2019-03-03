#!/bin/bash

cd "$( dirname "${BASH_SOURCE[0]}" )"
logfile="`basename $0`.log"
. ../../bin/activate
echo "### Start >>> `date`" >> $logfile 2>&1
python nextbike-dataset-scraper-prod.py >> $logfile 2>&1
echo "### End <<< `date`" >> $logfile 2>&1
# exit

if (( `date +%H` == 0 && `date +%M` < 10)); then
  . ../credentials.py
  for tbl in "countries" "cities" "places" "bike_list"
  do
    # echo $tbl
    sqlcmd -S "${server}" -d "${database}" -U "${username}" -P "${password}" -s, -W -Q "set nocount on; SELECT * FROM NextBscraper.${tbl}" | grep -v '^\-\-\-\-,' > ./DB/${tbl}.csv
    done
  kaggle datasets version --message "cron update `date`" -p ./DB/ >> $logfile 2>&1
fi
