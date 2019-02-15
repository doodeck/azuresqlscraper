#!/bin/bash

cd "$( dirname "${BASH_SOURCE[0]}" )"
logfile="`basename $0`.log"
. ../../bin/activate
echo "### `date`" >> $logfile
python nextbike_dataset_scraper.py >> $logfile

if (( `date +%H` == 20 && `date +%M` < 10)); then
  kaggle datasets version --message "cron update `date`" -p ./DB/ >> $logfile
fi
