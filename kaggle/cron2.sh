#!/bin/bash

cd "$( dirname "${BASH_SOURCE[0]}" )"
logfile="`basename $0`.log"
. ../../bin/activate
echo "### Start >>> `date`" >> $logfile 2>&1
python nextbike-dataset-scraper-prod.py >> $logfile 2>&1
echo "### End <<< `date`" >> $logfile 2>&1
exit

if (( `date +%H` == 0 && `date +%M` < 20)); then
  # kaggle datasets version --message "cron update `date`" -p ./DB/ >> $logfile 2>&1
fi
