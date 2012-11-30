#!/bin/bash


# Flip interval is the delay between operations
# simple shell for comparison of same SQL being run on Hive and Impala
flipinterval=20

echo "Example shell for connecting to Hive and to Impala	     "
echo " "




# this is the impala query
demoquery="Select a.sip, a.sport, a.dip, a.dport, b.protocol, sum(a.bytes) as Bytes, sum(a.packets) as Packets  from sherpa.netflow_silk a, sherpa.protocols b where a.protocol=b.id group by 1,2,3,4,5 LIMIT 20;"
# this is the hive query slightly different syntax due to the join
demoqueryhive="Select a.sip, a.sport, a.dip, a.dport, b.protocol, sum(a.bytes) as Bytes, sum(a.packets) as Packets  from sherpa.netflow_silk a join sherpa.protocols b on (a.protocol=b.id) group by a.sip, a.sport, a.dip, a.dport, b.protocol LIMIT 20;"
# both hive and Impala use the database
database="use sherpa;"


echo "First a  simple example....... summarizing netflow packets and bytes for five tuple addresses"
echo "Source IP, Source Port, Destination IP, Destination Port, Protocol, SUM Bytes and Packets	"
echo "						"
echo "A very simple Query: 			"
echo "						"
echo "           $demoquery			"
echo "						"
echo "						"
echo "Many people are familiar with HIVE which provides SQL like access on top a map reduce cluster"
echo "WATCH.... Our simple query is translated into a map reduce Job which is run in batch mode  "
echo "							"

HStarted=$(date '+%s')

hive <<EOF
$database
$demoqueryhive
$gottago
EOF

HFinished=$(date '+%s')

HElapsedTime=$((HFinished - HStarted));
ds=$((HElapsedTime % 60))

echo "					"
echo "	the elapsed time for Hive was $ds seconds		"
echo "					"
echo " OK, same query that we just ran using HIVE running on the same cluster against the same  "
echo " data sets........ on CLOUDERA IMPALA Real Time Query(RTQ)"

sleep $flipinterval
clear

ImpalaStartTime=$(date '+%s')

connectstring="connect sherpa3:21000;"
gottago="Quit;"
Started=$(date)
impala-shell <<EOF
$connectstring 
$database
$demoquery
$gottago
EOF

ImpalaEndTime=$(date '+%s')
esi=$((ImpalaEndTime - ImpalaStartTime))


sleep $flipinterval
clear


echo "								"
echo " POC sherpasurfing@gmail.com			"
echo " "
