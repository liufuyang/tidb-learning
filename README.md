# Ti-DB

https://www.pingcap.com/blog/how_to_spin_up_an_htap_database_in_5_minutes_with_tidb_tispark/

## Things required:
* docker (or docker-compose)

## Step to test Ti-DB out

* Start a TiDB docker-compose 
    * `git clone https://github.com/pingcap/tidb-docker-compose`
    * `cd tidb-docker-compose`
    * `docker-compose up -d`
* Start our tispark-notebook and wire it onto the TiDB/TiSpark cluster we just created
    * cd into your favorite workplace directory
    * `git clone https://github.com/liufuyang/tidb-learning`
    * `cd tidb-learning`
    * `docker-compose up`
    * visit `localhost:8888`, try run the Demo notebook
    * or if you don't have docker-compose, you can use docker as well:
        ```
        docker run -it --rm --name=tispark-notebook --network=tidb-docker-compose_default -p 8888:8888 \
          -v $(pwd)/notebook:/home/jovyan \
          liufuyang/tispark-notebook
        ```
* Take a look at https://github.com/pingcap/tidb-docker-compose
* Take a look at https://github.com/pingcap/tispark/tree/master/python

## Test Ti-DB in local kubernetes:
    * `brew install md5sha1sum`  ## if you have errors, do this `xcode-select --install`
    * `cp /etc/localtime ${HOME}/localtime`
    * `./dind-cluster-v1.10.sh`, or check this awesome [kubeadm-dind-cluster](https://github.com/kubernetes-sigs/kubeadm-dind-cluster)
    * `tidb-operator/charts`
    * `kubectl logs -n pingcap -l name=tidb-scheduler -c tidb-scheduler`
    

## Param calcuation:
* --executor-cores = 3
* 3 core per node (6 in total via docker locally, but 2 slaves so each has 3)
* Then the number of executors per node/slave is (3 - 1) / 3 = 1.
* We have 2 node, so --num-executors = 2 * 1 = 2

---

## Example code in notebook

```py
import os
# make sure pyspark tells workers to use python3 not 2 if both are installed
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python'

import pyspark
conf = pyspark.SparkConf()
conf.setMaster("spark://tispark-master:7077")
sc = pyspark.SparkContext(conf=conf)


spark = pyspark.sql.SparkSession.builder \
        .master("spark://tispark-master:7077") \
        .appName("demo-tispark") \
        .config("spark.tispark.pd.addresses", "pd0:2379") \
        .getOrCreate()
        
import pytispark.pytispark as pti
 
ti = pti.TiContext(spark)
 
ti.tidbMapDatabase("mysql")
 
spark.sql("select count(*) from user").show()

```
----


Reference:
https://community.hortonworks.com/questions/160875/spark2-javalangoutofmemoryerror-java-heap-space.html
https://dlab.epfl.ch/2017-09-30-what-i-learned-from-processing-big-data-with-spark/
https://community.hortonworks.com/questions/65934/spark-executor-memory.html
https://spark.apache.org/docs/2.1.1/configuration.html
