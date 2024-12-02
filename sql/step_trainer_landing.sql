CREATE EXTERNAL TABLE IF NOT EXISTS `stedi2`.`trainer_landing` (
  `sensorReadingTime` TIMESTAMP,
  `serialNumber` STRING,
  `distanceFromObject` FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://dung4476-cd0030bucket-step-trainer/trainer/landing/'
TBLPROPERTIES ('classification' = 'json');

