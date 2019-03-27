// dummy data
import spark.implicits._
val df = spark.createDataFrame(Seq(
      ("43081", "Westerville",1,None,Some(70000))
    , ("43081", "Westerville",2,Some(25),None)   
    , ("43081", "Westerville",3,Some(25),Some(68000))    
    , ("43081", "Westerville",4,Some(35),Some(80000))
    , ("43081", "Westerville",5,Some(35),Some(80000))
    , ("43081", "Westerville",6,Some(35),Some(1000000))
    , ("43081", "Westerville",7,Some(25),Some(66000))
    , ("43081", "Westerville",8,Some(25),Some(64000))
    , ("43081", "Westerville",9,Some(25),Some(62000))    
    , ("60657", "Chicago",1,None,Some(70000))
    , ("60657", "Chicago",2,Some(35),None)   
    , ("60657", "Chicago",3,Some(35),Some(75000))    
    , ("60657", "Chicago",4,Some(35),Some(80000))
    , ("60657", "Chicago",5,Some(35),Some(85000))
    , ("60657", "Chicago",6,Some(35),Some(90000))
    , ("60657", "Chicago",7,Some(25),Some(65000))
    , ("60657", "Chicago",8,Some(25),Some(65000))
    , ("60657", "Chicago",9,Some(25),Some(65000)) 
    )).toDF("zipcode", "city","id","age","salary")


df.createOrReplaceTempView("baseTable")

// define key columns and mode columns (features)
val keyColList = List("zipcode","city")
val allkeyColumns = keyColList.mkString(" ,")
val modeColList  = List("age","salary")
val allModeColumns = "," + modeColList.mkString(" ,")


// generate count by value query
var countPartitionByClause = "\n, sum(1) over(partition by %s, %s) as %s_perval_count"
var countPartitionByClauses = ""
for (modeCol<-modeColList) {
    countPartitionByClauses = countPartitionByClauses.concat(countPartitionByClause.format(allkeyColumns,modeCol,modeCol))
}
print(countPartitionByClauses)


val baseSQLQuery = """
select %s %s %s 
from baseTable a
"""

val dynamicSQLQuery = baseSQLQuery.format(allkeyColumns,allModeColumns,countPartitionByClauses)

val countbyValueTable = spark.sql(dynamicSQLQuery)
countbyValueTable.show()
countbyValueTable.createOrReplaceTempView("countbyValueTable")


// generate max of value over count
// if the count results in two mode values , randomly pick one 
var maxPartitionByClause = "\n, row_number() over(partition by %s  order by %s_perval_count desc ) as %s_max_count_rank"
var maxPartitionByClauses = ""
for (modeCol<-modeColList) {
    maxPartitionByClauses = maxPartitionByClauses.concat(maxPartitionByClause.format(allkeyColumns,modeCol,modeCol))
}
print(maxPartitionByClauses)


var modeColumn = "\n, case when(%s_max_count_rank == 1) then %s else null end as %s_mode"
var modeColumns = ""
for (modeCol<-modeColList) {
    modeColumns = modeColumns.concat(modeColumn.format(modeCol,modeCol,modeCol))
}
print(modeColumns)

var maxColumn = "\n, max(%s_mode) as %s_mode"
var maxColumns = ""
for (modeCol<-modeColList) {
    maxColumns = maxColumns.concat(maxColumn.format(modeCol,modeCol))
}
print(maxColumns)


var whereClause = "%s_max_count_rank = 1"
var whereClauses = ""
var i = 0
for (modeCol<-modeColList) {
    if (i > 0) {
        whereClauses = whereClauses.concat("\nor ")
    }
    whereClauses = whereClauses.concat(whereClause.format(modeCol))
    i = i + 1
}
print(whereClauses)

var whereNotNullClause = "%s is not null"
var whereNotNullClauses = ""
var i = 0
for (modeCol<-modeColList) {
    if (i > 0) {
        whereNotNullClauses = whereNotNullClauses.concat("\nor ")
    }
    whereNotNullClauses = whereNotNullClauses.concat(whereNotNullClause.format(modeCol))
    i = i + 1
}
print(whereNotNullClauses)


//Generate the entire query
val qry = """
select %s %s from (
select  %s %s from (
select %s %s %s
from countbyValueTable
) where %s
) group by %s
""".format(allkeyColumns,maxColumns,allkeyColumns,modeColumns,allkeyColumns,allModeColumns,maxPartitionByClauses,whereClauses,allkeyColumns)


/*
The above code will generate the following SQL
select zipcode ,city
, max(age_mode) as age_mode
, max(salary_mode) as salary_mode from (
select  zipcode ,city
, case when(age_max_count_rank == 1) then age else null end as age_mode
, case when(salary_max_count_rank == 1) then salary else null end as salary_mode from (
select zipcode ,city ,age ,salary
, row_number() over(partition by zipcode ,city  order by age_perval_count desc ) as age_max_count_rank
, row_number() over(partition by zipcode ,city  order by salary_perval_count desc ) as salary_max_count_rank
from countbyValueTable
) where age_max_count_rank = 1
or salary_max_count_rank = 1
) group by zipcode ,city
*/

//Execute Max Count Query to get Mode
val modeDF = spark.sql(qry)
modeDF.show()
modeDF.createOrReplaceTempView("modeDF")

/*
Result 
+-------+-----------+--------+-----------+
|zipcode|       city|age_mode|salary_mode|
+-------+-----------+--------+-----------+
|  43081|Westerville|      25|      80000|
|  60657|    Chicago|      35|      65000|
+-------+-----------+--------+-----------+
*/
