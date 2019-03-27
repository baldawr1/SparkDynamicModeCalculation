# Spark Dynamic Mode Calculation - Calculate Mode across multiple columns(features) dynamically

Data scientist using notebooks (Jupyter/Zeppelin) often need to calculate mode (https://www.mathsisfun.com/definitions/mode.html) across various columns/features of a dataframe.  

The columns/features name change from one dataset to another.

The code takes in column names for which mode needs to be calculated and dynamically generate spark sql utilizing sparks catalyst optimizer for optimum performance.

Example -
Sample Dataset
```
+-------+-----------+---+----+-------+
|zipcode|       city| id| age| salary|
+-------+-----------+---+----+-------+
|  43081|Westerville|  1|null|  70000|
|  43081|Westerville|  2|  25|   null|
|  43081|Westerville|  3|  25|  68000|
|  43081|Westerville|  4|  35|  80000|
|  43081|Westerville|  5|  35|  80000|
|  43081|Westerville|  6|  35|1000000|
|  43081|Westerville|  7|  25|  66000|
|  43081|Westerville|  8|  25|  64000|
|  43081|Westerville|  9|  25|  62000|
|  60657|    Chicago|  1|null|  70000|
|  60657|    Chicago|  2|  35|   null|
|  60657|    Chicago|  3|  35|  75000|
|  60657|    Chicago|  4|  35|  80000|
|  60657|    Chicago|  5|  35|  85000|
|  60657|    Chicago|  6|  35|  90000|
|  60657|    Chicago|  7|  25|  65000|
|  60657|    Chicago|  8|  25|  65000|
|  60657|    Chicago|  9|  25|  65000|
+-------+-----------+---+----+-------+
```

Lets assume primary keys are - zipcode and city and mode needs to generated across - age and salary.

Based on the column/features name provided as input , the code will generate the following spark sql dynamically based on the above inputs
```
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
```

Results -
```
+-------+-----------+--------+-----------+
|zipcode|       city|age_mode|salary_mode|
+-------+-----------+--------+-----------+
|  43081|Westerville|      25|      80000|
|  60657|    Chicago|      35|      65000|
+-------+-----------+--------+-----------+
```
