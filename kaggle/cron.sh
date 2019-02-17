#!/bin/bash

cd "$( dirname "${BASH_SOURCE[0]}" )"
logfile="`basename $0`.log"
. ../../bin/activate
echo "### `date`" >> $logfile 2>&1
python nextbike_dataset_scraper.py >> $logfile 2>&1

if (( `date +%H` == 0 && `date +%M` < 20)); then
  kaggle datasets version --message "cron update `date`" -p ./DB/ >> $logfile 2>&1
fi
