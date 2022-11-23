import os
import sys
sys.path.append(os.getcwd())

from src.labeling.utils import get_spark, query
from omegaconf import OmegaConf

spark = get_spark()
config = OmegaConf.load("./config.yml")

print(config.db)

df = query(spark, **config.db)
df.printSchema()
spark.stop()
