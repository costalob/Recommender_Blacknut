# RECOMMENDER BLACKNUT

## To compile the recommender :
Launch `RECOMMENDER_BLACKNUT/recommender/script/binder.sh compile`

## To run the recommender :
Launch `RECOMMENDER_BLACKNUT/recommender/script/binder.sh run`

## To change the setting of the recommender : 
Edit `RECOMMENDER_BLACKNUT/recommender/app/src/main/ressources/default_config.yml`  
Or create a new config file and in `RECOMMENDER_BLACKNUT/recommender/app/run.sh` uncomment `--config="$CONFIG` and indicate the path in `CONFIG="..."`

## The config file : 
```java
data: local                                   /* local | online */
dataset: src/main/resources/streams.csv       /* if data is set to local, this indicates the path where to collect the datas */
resultPath: result.json                       /* the path and the name of the result file */
keyPath: blacknut-analytics-7488e0e6d73d.json /* if data is set to online, this indicates the path and the name of the key file to access to the BigQuery datas */
nbUserPerFile: 0                              /* the number of users per result file, if set to 0 all results will be store in one file */
nbRecommendation: 5                           /* the number of recommendations per users and per algorithms */
configs:                                      /* the list of the recommendation algorithms and their config file to run for each user */
- mf/config79.yml
- ibknn/config0.yml
normalize: false                              /* normalize the data */
binarize: false                               /* binarize the data */
```

## Sources : 

This project use the modifed version of Apache Mahout™ made by Florestan De Moor : 
[`https://github.com/fdemoor/mahout/tree/branch-0.13.0`](https://github.com/fdemoor/mahout/tree/branch-0.13.0)

For additional information about Mahout, visit the [Mahout Home Page](http://mahout.apache.org/)