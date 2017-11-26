# Apache Spark aggregation, filtering and cube calculation of large sets of txt files

## Steps

* Read all txt files from disk and create an RDD
* Remove bad rows of RDD
* Group RDD by store_id
* Group RDD by store_id and salesman_id
* Group RDD by store_id and product_id
* Group RDD by product_id
* Save files
