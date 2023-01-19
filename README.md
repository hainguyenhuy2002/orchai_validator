# orchai_validator
```
python ./runs/uploader.py -cf ./config/local_db.yaml   -s 7059473 -e 7071323
python ./runs/uploader.py -cf ./config/local_file.yaml -s 7059473 -e 7071323
python ./runs/uploader.py -cf ./config/local_db.yaml   -s 7059473 -e 7083323
python ./runs/uploader.py -cf ./config/local_file.yaml -s 7059473 -e 7083323

python ./runs/uploader.py -cf ./config/etl_file.yaml   -s 7059473 -e 7071323
python ./runs/uploader.py -cf ./config/etl_db.yaml     -s 7059473 -e 7071323
python ./runs/uploader.py -cf ./config/etl_file.yaml   -s 7059473 -e 9583823
python ./runs/uploader.py -cf ./config/etl_db.yaml     -s 7059473 -e 9583823

python ./runs/validate_etl.py -c ./config/etl.yaml
python ./runs/validate_etl.py -c ./config/local_full.yaml
```

# Bugs:
## upload.py
+ get_batch_intervals():
    + missing 1 blocks between 2 continuous intervals