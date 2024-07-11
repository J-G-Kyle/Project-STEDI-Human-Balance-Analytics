CREATE EXTERNAL TABLE `machine_learning_curated`(
  `sensorreadingtime` bigint COMMENT 'from deserializer',
  `serialnumber` string COMMENT 'from deserializer',
  `distancefromobject` int COMMENT 'from deserializer',
  `x` float COMMENT 'from deserializer',
  `y` float COMMENT 'from deserializer',
  `z` float COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://jonathan-kyle-spark/step_trainer/curated/'
TBLPROPERTIES (
  'classification'='json')