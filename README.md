# orchai_validator
```
python ./runs/uploader.py -cf ./config/local_full.yaml -s 7059473 -e 7071323
python ./runs/uploader.py -cf ./config/local_full.yaml -s 7059473 -e 7083323

python ./runs/uploader.py -cf ./config/etl.yaml -s 7059473 -e 9583823
python ./runs/uploader.py -cf ./config/local.yaml -s 7100000 -e 7103999

python ./runs/uploader.py -cf ./config/etl.yaml -s 7051000 -e 8200000
python ./runs/uploading_scheduler.py -cf ./config/etl.yaml -bs 3

python ./runs/validate_etl.py -c ./config/etl.yaml
python ./runs/validate_etl.py -c ./config/local_full.yaml
```

# Bugs:
## upload.py
+ get_batch_intervals():
    + missing 1 blocks between 2 continuous intervals