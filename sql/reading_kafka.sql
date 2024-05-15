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