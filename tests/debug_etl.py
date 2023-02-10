import os, sys
sys.path.append(os.path.join(os.getcwd(), "src"))


from orchai.upload import get_spark, sampling, uploading, run_uploading
from orchai.constants import Constants
from orchai.etl_processor import validating, ETLProcessor
from omegaconf import OmegaConf
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
import numpy as np


spark = get_spark()
config = OmegaConf.load("config/etl_file_1m.yaml")
sb, eb = 7059473, 9583823
print((eb - sb) / 150 + 1)

dt = ETLProcessor.data_scoring(sampling(spark, config, from_block=8906273, to_block=9506273), cal_score=True, **config.hp.etl)
validating(dt)