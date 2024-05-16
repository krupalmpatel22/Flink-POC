CREATE TABLE sales_usd (
    seller_id STRING,
    amount_usd DOUBLE,
    sale_ts TIMESTAMP
)
WITH (
    'connector' = 'kafka',
    'topic' = 'sales-usd',
    'properties.bootstrap.servers' = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'format' = 'json',
    'properties.group.id' = 'demoGroup',
    'scan.startup.mode' = 'earliest-offset',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'PLAIN',
    'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="CSZHJHLM4KPTQGH6" password="tIINIPPlk1H5nVBQeODYd1Ekk1E9i0G/yy4MDvoTLra0VefReZcryEtUbJpvsdqj";',
    'sink.partitioner' = 'fixed'
);

----------------------------------------------------------------------------------------------

SELECT seller_id, SUM(amount_usd) * 0.85 AS window_sales FROM sales_usd GROUP BY seller_id;

----------------------------------------------------------------------------------------------

CREATE TABLE sales_euros (
  seller_id VARCHAR,
  window_sales DOUBLE,
  PRIMARY KEY (`seller_id`) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'sales-euros',
  'properties.bootstrap.servers' = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="CSZHJHLM4KPTQGH6" password="tIINIPPlk1H5nVBQeODYd1Ekk1E9i0G/yy4MDvoTLra0VefReZcryEtUbJpvsdqj";',
  'key.format' = 'json',
  'value.format' = 'json'
);

----------------------------------------------------------------------------------------------

INSERT INTO sales_euros SELECT seller_id, SUM(amount_usd * 0.85) FROM sales_usd GROUP BY seller_id;