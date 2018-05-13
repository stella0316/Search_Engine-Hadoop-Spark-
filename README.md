# Search Engine for Structured Data

We have built a search engine for structured data enabling title, column, content and topic search with customized filters. As an example, we use a subset of [NYCOpenData](https://opendata.cityofnewyork.us/) datasets to show how it makes seaching over structured data more productive and efficient.

## Running the search engine on DFS

To run the search engine on DFS (e.g. Dumbo), first copy the files in src/data folder into HDFS.
On Dumbo, run

```
hadoop fs -put src/data
cd src
python prompt.py
```


## Running the search engine locally

To run the search engine locally on a machine with PySpark installed, run

```
cd src
python prompt.py
```


## Built With

* [Hadoop Streaming](http://hadoop.apache.org/docs/current/hadoop-streaming/HadoopStreaming.html#Hadoop_Streaming) - Running Map/Reduce Jobs with Scripts
* [PySpark](http://spark.apache.org/docs/2.1.0/api/python/index.html) - Python API for Spark

