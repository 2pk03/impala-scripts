#!/bin/bash
HIVE_TABLE=ZIPCODE_HBASE
HBASE_TABLE=zipcode_hive
IMPALA_HOST=`hostname`:21000
IMPALA_OPTS=-k

unzip -u median_income_by_zipcode_census_2000.zip
# Remove Header
awk 'FNR>2' DEC_00_SF3_P077_with_ann.csv > DEC_00_SF3_P077_with_ann_noheader.csv
sed -i 's/\"//g' DEC_00_SF3_P077_with_ann_noheader.csv
sed -i 's/\ //g' DEC_00_SF3_P077_with_ann_noheader.csv

clear

# drop any existing tables
echo 
echo ------------------------------------------------------
echo Delete old data
echo ------------------------------------------------------
echo 

hive -e "drop table $HIVE_TABLE"
echo "disable '$HBASE_TABLE'" > hb1
echo "drop '$HBASE_TABLE'" >> hb1
echo "exit" >> hb1

hbase shell hb1

echo 
echo ------------------------------------------------------
echo  Create the Hive table using HBaseStorageHandler
echo ------------------------------------------------------
echo 

echo "create '$HBASE_TABLE', 'id', 'zip', 'desc', 'income'" > hb1
echo "exit" >> hb1
hbase shell hb1
rm -rf hb1

echo 
echo ------------------------------------------------------
echo "Create Hive's external table"
echo ------------------------------------------------------
echo 

hive -e "CREATE EXTERNAL TABLE $HIVE_TABLE (key STRING,zip STRING,desc1 STRING,desc2 STRING,income STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,zip:zip,desc:desc1,desc:desc2,income:income\") TBLPROPERTIES(\"hbase.table.name\" = \"$HBASE_TABLE\");"

echo 
echo ------------------------------------------------------
echo Load data into HBase via PIG
echo ------------------------------------------------------
echo 

echo "copyFromLocal DEC_00_SF3_P077_with_ann_noheader.csv ziptest.csv" > pig1
echo "A = LOAD 'ziptest.csv' USING PigStorage(',') as (id:chararray, zip:chararray, desc1:chararray, desc2:chararray, income:chararray); STORE A INTO 'hbase://$HBASE_TABLE' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('zip:zip,desc:desc1,desc:desc2,income:income');" >> pig1

pig pig1 && rm -f pig1

echo 
echo ------------------------------------------------------
echo "Scan the HBase table"
echo ------------------------------------------------------
echo

echo "scan '$HBASE_TABLE', LIMIT => 2"
echo
echo "scan '$HBASE_TABLE', LIMIT => 2" > hb2
echo "exit" >> hb2
hbase shell hb2 && rm -rf hb2

echo 
echo ------------------------------------------------------
echo "Refresh Impala's catalog (press Enter)"
echo ------------------------------------------------------
echo 
read

impala-shell -i $IMPALA_HOST $IMPALA_OPTS -q "refresh"

echo 
echo ------------------------------------------------------
echo "Run sample queries "
echo ------------------------------------------------------
echo 

echo "=> select * from $HIVE_TABLE limit 4 (press Enter)"
echo
read
impala-shell -i $IMPALA_HOST $IMPALA_OPTS -q "select * from $HIVE_TABLE limit 4"
echo

echo
echo "=> select count(*) from $HIVE_TABLE where income>'0' and income<'7000' (press Enter)"
echo 
read
impala-shell -i $IMPALA_HOST $IMPALA_OPTS -q "select count(*) from $HIVE_TABLE where income>'0' and income<'7000';"

echo
echo " => select * from ZIPCODE_HBASE where income between '1000' and '5000' order by income DESC limit 20; (press Enter)"
echo
read
impala-shell -i $IMPALA_HOST $IMPALA_OPTS -q "select * from ZIPCODE_HBASE where income between '1000' and '5000' order by income DESC limit 20;"
echo

# delete temp data
rm -f DEC_00* aff*.txt
