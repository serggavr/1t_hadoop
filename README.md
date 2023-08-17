#### Решение

Запускаем `.\app\app.py`

Копируем в `.\tmp\` докер контейнера 1t_hadoop-datanode-1 папку с фалами `./for_load` с хоста
``` 
docker cp ./for_load 1t_hadoop-datanode-1:tmp\
```  
Подключаемся к контейнеру  
```
docker container exec -it 1t_hadoop-datanode-1 bash
```  
Переходим в папку `tmp\for_load`
```
cd tmp
cd for_load
```
Создаем папку в Hadoop
``` 
hadoop fs -mkdir /user/hive/my_loaded_csv
```
Загружаем sum.txt на hdfs в папку `/user/hive/my_loaded_csv`
``` 
hadoop fs -put /tmp/for_load/ /user/hive/my_loaded_csv/
```
Определяем права доступа к csv файлам
``` 
hadoop fs -chmod 755  /user/hive/my_loaded_csv/for_load/people_processed.csv
hadoop fs -chmod 755  /user/hive/my_loaded_csv/for_load/organizations_processed.csv
hadoop fs -chmod 755  /user/hive/my_loaded_csv/for_load/customers_processed.csv
```
Создаем таблицы в Hive и заполняем их данными
```sql
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 1000;

CREATE SCHEMA raw

CREATE TABLE IF NOT EXISTS people (
index string,
user_id string,
first_name string,
last_name string,
sex string,
email string,
phone string,
dob date,
job_title string,
age_groupe string,
index_group string )
COMMENT 'People Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'

LOAD DATA INPATH '/user/hive/my_loaded_csv/for_load/people_processed.csv'
INTO TABLE people;

CREATE TABLE IF NOT EXISTS raw.people(
  index string,
  user_id string,
  first_name string,
  last_name string,
  sex string,
  email string,
  phone string,
  dob date,
  job_title string,
  age_groupe string)
PARTITIONED BY (index_group string)
CLUSTERED BY (age_groupe) INTO 64 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
    
    
CREATE TABLE IF NOT EXISTS customers (
index string,
customer_id string,
first_name string,
last_name string,
company string,
city string,
country string,
phone_1 date,
phone_2 string,
email string, 
subscription_date Date,
website string,
index_group string )
COMMENT 'Customers Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'

LOAD DATA INPATH '/user/hive/my_loaded_csv/for_load/customers_processed.csv'
INTO TABLE customers;

CREATE TABLE IF NOT EXISTS raw.customers(
  index string,
  customer_id string,
  first_name string,
  last_name string,
  company string,
  city string,
  country string,
  phone_1 date,
  phone_2 string,
  email string, 
  subscription_date Date,
  website string)
PARTITIONED BY (index_group string)
CLUSTERED BY (country) INTO 64 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';';


CREATE TABLE IF NOT EXISTS organizations (
index string,
organization_id string,
name string,
website string,
country string,
description string,
founded string,
industry string,
num_of_employees int,
index_group string )
COMMENT 'Organizations Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'

LOAD DATA INPATH '/user/hive/my_loaded_csv/for_load/organizations_processed.csv'
INTO TABLE organizations;

CREATE TABLE IF NOT EXISTS raw.organizations(
  index string,
  organization_id string,
  name string,
  website string,
  country string,
  description string,
  founded string,
  industry string,
  num_of_employees int)
PARTITIONED BY (index_group string)
CLUSTERED BY (country) INTO 64 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';';


INSERT OVERWRITE TABLE raw.organizations PARTITION (index_group) 
SELECT * FROM default.organizations;

INSERT OVERWRITE TABLE raw.customers PARTITION (index_group) 
SELECT * FROM default.customers;

INSERT OVERWRITE TABLE raw.people PARTITION (index_group) 
SELECT * FROM default.people;
```
Формируем витрину
```sql
WITH sales AS (
  SELECT 
    c.customer_id,
    year(c.subscription_date) AS year,
    p.index_group AS age_group,
    o.name AS name
  FROM raw.customers AS c
  JOIN raw.people AS p ON p.email = c.email AND p.first_name = c.first_name AND p.last_name = c.last_name
  join raw.organizations AS o ON o.name = c.company 
),

sales_with_max_group AS (
  SELECT 
    year,
    name,
    age_group,
    max(age_group) OVER (PARTITION BY year, name) AS max_sales_in_group
  FROM sales
)

SELECT 
  name, year, max_sales_in_group
FROM sales_with_max_group
GROUP BY name, year, max_sales_in_group;
```