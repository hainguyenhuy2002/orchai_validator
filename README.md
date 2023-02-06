# orchai_validator
```
python ./runs/uploader.py -cf ./config/local_db.yaml    -s 7059473 -e 7071323
python ./runs/uploader.py -cf ./config/local_file.yaml  -s 7059473 -e 7076123
python ./runs/uploader.py -cf ./config/local_db.yaml    -s 7059473 -e 7083323
python ./runs/uploader.py -cf ./config/local_file.yaml  -s 7059473 -e 7083323

python ./runs/uploader.py -cf ./config/etl_file.yaml    -s 7059473 -e 7071323
python ./runs/uploader.py -cf ./config/etl_db.yaml      -s 7059473 -e 7071323
python ./runs/uploader.py -cf ./config/etl_file.yaml    -s 7059473 -e 9583823
python ./runs/uploader.py -cf ./config/etl_db.yaml      -s 7059473 -e 9583823

python ./runs/uploader.py -cf ./config/etl_file_1m.yaml -s 7059473 -e 9583823
python ./runs/uploader.py -cf ./config/etl_file_3w.yaml -s 7059473 -e 9583823

python ./runs/validate_etl.py -c ./config/etl.yaml
python ./runs/validate_etl.py -c ./config/local_full.yaml
```
# back test
Small test set in  the interval of 1 month (test each day for 1 month data)
```
python ./runs/backtester.py -p ./data/percent.parquet  -cr 0.1  -s 7103573  -e 7535573 -st 14400 -t 432000 
```

All test set in  the interval of 1 month(test each day for the whole data
```
python ./runs/backtester.py -p ./data/percent.parquet  -cr 0.1  -s 7103573  -e 9151823 -st 14400 -t 432000 
```

Get backtest result:
```
python labeling/back_test.py
```

python ./src/runs/tunning_score.py 