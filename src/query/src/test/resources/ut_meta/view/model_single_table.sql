SELECT "LINEORDER"."LO_ORDERKEY" as "THE_KEY",
 "LINEORDER"."LO_CUSTKEY" as "L O CU ST KEY",
 "LINEORDER"."LO_ORDTOTALPRICE" as "LO_ORDTOTALPRICE",
 "LINEORDER"."LO_LINENUMBER" as "LO_LINENUMBER"
 FROM "SSB"."LINEORDER" as "LINEORDER"