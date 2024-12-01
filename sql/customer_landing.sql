CREATE EXTERNAL TABLE IF NOT EXISTS `stedi2`.`customer_landing` (
  `serialnumber` string,
  `customername` string,
  `email` string,
  `phone` string,
  `birthday` string,
  `registrationdate` bigint,
  `lastupdatedate` bigint,
  `sharewithpublicasofdate` bigint,
  `sharewithfriendsasofdate` bigint,
  `sharewithresearchasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://dung4476-cd0030bucket-customers/customer/landing/'
TBLPROPERTIES ('classification' = 'json');