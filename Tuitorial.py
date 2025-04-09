# Databricks notebook source
# MAGIC %md
# MAGIC ### Data reading in json format

# COMMAND ----------

df_json = spark.read.format('json').option('inferschmea', True)\
                    .option('header', True)\
                    .option('multiline', False)\
                    .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df = spark.read.format('csv').option("inferschema",True).option("header", True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

# df.show() this will not give data in good manner

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Defination

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### DDL Schema

# COMMAND ----------

my_ddl_schema = '''
                      Item_Identifier string,
                      Item_Weight string,
                      Item_Fat_Content string,
                      Item_Visibility double,
                      Item_Type string,
                      Item_MRP double,
                      Outlet_Identifier string,
                      Outlet_Establishment_Year integer,
                      Outlet_Size string,
                      Outlet_Location_Type string,
                      Outlet_Type string,
                      Item_Outlet_Sales double
                '''

# COMMAND ----------

df = spark.read.format('csv')\
          .schema(my_ddl_schema)\
          .option('header',True)\
          .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_strct_schema = StructType([
                                StructField('Item_Identifier', StringType(), True),
                                StructField('Item_Weight', StringType(), True),
                                StructField('Item_Fat_Content', StringType(), True),
                                StructField('Item_Visibility', StringType(), True),
                                StructField('Item_Type', StringType(), True),
                                StructField('Item_MRP', StringType(), True),
                                StructField('Outlet_Identifier', StringType(), True),
                                StructField('Outlet_Establishment_Year', StringType(), True),
                                StructField('Outlet_Size', StringType(), True),
                                StructField('Outlet_Location_Type', StringType(), True),
                                StructField('Outlet_Type', StringType(), True),
                                StructField('Item_Outlet_Sales', StringType(), True)
])

# COMMAND ----------

df = spark.read.format('csv')\
               .schema(my_strct_schema)\
               .option('header', True)\
               .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SELECT

# COMMAND ----------

df_select = df.select(col('Item_Identifier'), col('Item_Weight'),col('Item_Fat_Content')).display() # converted col into object

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER / WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scenario 1 -> Filter data with fat content = Regular

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2 -> slice the data with item type = soft drinks and weight < 10

# COMMAND ----------

df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_weight') < 10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 3 -> Fetch data with tier in (tier 1 or tier 2) and outlet size is null

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight', 'Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1 -> creating col with val new

# COMMAND ----------

df.withColumn('FLAG',lit('new')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1 -> create a col with value mutilplication of 2 col ****

# COMMAND ----------

df.withColumn('multiply', col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2 -> replacing value in existing col

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'), 'Regular', 'Reg'))\
   .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'), 'Low Fat', 'Lf')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Type Casting

# COMMAND ----------

df = df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))
df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC # Sort/Order by

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1 - desc

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2 - asc

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3 - sort mutiple cols in one 

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'], ascending = [0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 4 - sort mutiple cols in one but different order

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'], ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1 -> drop col 

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2 -> drop mutiple col at once

# COMMAND ----------

df.drop('Item_Visibility','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # drop_duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1 -> simple drop duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2 -> drop duplicates from particular col

# COMMAND ----------

df.dropDuplicates(subset = ['Item_Type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### drop duplicates using distinct

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing dataframe

# COMMAND ----------

data1 = [('1','kad'),
         ('2','sid')]
schema1 = 'id STRING, name STRING'

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
         ('4','sid')]
schema2 = 'id STRING, name STRING'

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### union 

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [('kad','1'),
         ('sid','2')]
schema1 = 'name STRING, id STRING'

df1 = spark.createDataFrame(data1,schema1)


# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### union by name 

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # String functions

# COMMAND ----------

# MAGIC %md 
# MAGIC ### INIT CAP

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lower

# COMMAND ----------

df.select(lower('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upper

# COMMAND ----------

df.select(upper('Item_Type').alias('upper_item_type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Date Funcations

# COMMAND ----------

# MAGIC %md 
# MAGIC ### current date

# COMMAND ----------

df = df.withColumn('curr_date',current_date())
df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### date add

# COMMAND ----------

df = df.withColumn('week_after',date_add('curr_date',7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### date sub

# COMMAND ----------

df = df.withColumn('week_before',date_sub('curr_date',7))
df.display();

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2nd way for date diff

# COMMAND ----------

df = df.withColumn('week_before',date_add('curr_date',-7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Date Diff

# COMMAND ----------

df = df.withColumn('date diff',datediff('week_after','curr_date'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # date format

# COMMAND ----------

df = df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Handling nulls

# COMMAND ----------

# MAGIC %md
# MAGIC ### dropping nulls

# COMMAND ----------

df.dropna('all').display() # del records which has all col null

# COMMAND ----------

df.dropna('any').display() # drop record which has any col null

# COMMAND ----------

df.dropna(subset=['Item_Type']).display() #helps to delete in a particular cols

# COMMAND ----------

# MAGIC %md
# MAGIC # filling nulls

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('NotAvailable',subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Split and indexing

# COMMAND ----------

# MAGIC %md
# MAGIC ### split

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexing

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Explode

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Array Contains

# COMMAND ----------

df_exp.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # group by

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### scenario 2

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### scenario 4

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # collect list

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Pivot

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # When Otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC ### scenario 1

# COMMAND ----------

df = df.withColumn('Veg_flag',when(col('Item_Type') == 'Meat', 'Non-Veg').otherwise('Veg'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### scenario 2

# COMMAND ----------

df.withColumn('veg_exp_flag',when(((col('veg_flag') == 'Veg') & (col('Item_MRP')<100)),'Veg_Inexpensive')\
  .when((col('veg_flag')=='Veg') & (col('Item_MRP')>100),'Veg_Expensive')\
  .otherwise('Non_Veg')).display() 

# COMMAND ----------

# MAGIC %md
# MAGIC # Joins

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Join

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right Join

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti Join

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Window function

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Number

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RANK VS DENSE RANK

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
        .withColumn('denseRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

df.withColumn('sum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

### Cumulative Sum

# COMMAND ----------

df.withColumn('cum_sum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('cum_sum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # User Defined Function (UFD)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step - 1

# COMMAND ----------

def my_func(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2

# COMMAND ----------

my_udf = udf(my_func)

# COMMAND ----------

df.withColumn('mynewcol',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data writing

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV

# COMMAND ----------

df.write.format('csv')\
  .save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Modes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append

# COMMAND ----------

df.write.format('csv')\
  .mode('append')\
  .save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------


df.write.format('csv')\
        .mode('append')\
        .option('path','/FileStore/tables/CSV/data.csv')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwrite

# COMMAND ----------

df.write.format('csv')\
        .mode('overwrite')\
        .option('path','/FileStore/tables/CSV/data.csv')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Error

# COMMAND ----------

df.write.format('csv')\
.mode('error')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ignore

# COMMAND ----------


df.write.format('csv')\
.mode('ignore')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PARQUET

# COMMAND ----------

df.write.format('parquet')\
.mode('overwrite')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC # TABLE

# COMMAND ----------

df.write.format('parquet')\
.mode('overwrite')\
.saveAsTable('my_table')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark SQL

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_Fat_Content = 'Low Fat'

# COMMAND ----------

df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'Low Fat'")

# COMMAND ----------

df_sql.display()

# COMMAND ----------

