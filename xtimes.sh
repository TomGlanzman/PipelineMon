#!/bin/bash

## Setup and run the xtimes.py script

source ~srs/oracle/bin/setup-11g-j7.sh

cmd='/u/ec/dragon/lsst/DC2mon/xtimes.py >| /u/ec/dragon/public_html/DESC/xtimes.txt'

eval $cmd
