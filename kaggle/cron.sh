#!/bin/bash

cd "$( dirname "${BASH_SOURCE[0]}" )"
logfile="`basename $0`.log"
. ../../bin/activate
echo "### `date`" >> $logfile
python nextbike_dataset_scraper.py >> $logfile
