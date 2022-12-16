# orchai_validator
```
python ./uploader.py -cf ./config/etl.yaml -s 7100000 -e 7103999
python ./uploader.py -cf ./config/local.yaml -s 7100000 -e 7103999

python ./uploader.py -cf ./config/etl.yaml -s 7051000 -e 8200000
```

# Bugs:
## upload.py
+ get_batch_intervals():
    + missing 1 blocks between 2 continuous intervals