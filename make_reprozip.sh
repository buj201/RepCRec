#!/bin/sh

reprozip trace --overwrite ./run_custom_tests.sh
reprozip trace --continue python3 -m src.transaction_manager tests/provided_tests/test1.txt
reprozip pack RepCRec.rpz
