module load python/gnu/3.4.4
module load spark/2.2.0

spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python funcs/main.py /user/ss8955/project/title_index.txt /user/ss8955/project/column_index_sample.txt /user/ss8955/project/content_index.txt /user/ss8955/project/Master_Index_New.csv /user/ss8955/project/tag_index.csv /user/ss8955/project/Table_Desc.csv