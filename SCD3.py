from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,current_date


jdbc_driver_path = "C:\\Users\\aadar\\Downloads\\postgresql-42.7.4.jar"
spark = SparkSession.builder.appName("SCD_TYPE3").config("spark.jars", jdbc_driver_path).getOrCreate()

def get_existing_data():
    return spark.read.format("jdbc").options(
        url="jdbc:postgresql://localhost:5432/firstDB",
        dbtable="Customer_SCD3",
        user="postgres",
        password="adarshjain",
        driver="org.postgresql.Driver"
    ).load()

def update_table(records):
    return records.write.format("jdbc").options(
        url="jdbc:postgresql://localhost:5432/firstDB",
        dbtable="Customer_SCD3",
        user="postgres",
        password="adarshjain",
        driver="org.postgresql.Driver"
    ).mode("overwrite").save()


def SCD3(new_data):

    existing_df=get_existing_data()
    new_df=spark.createDataFrame(new_data)

    # existing_df.show()
    # new_df.show()

    merged_df=new_df.alias("new").join(
        existing_df.alias("existing"),
        col("new.CustomerID")==col("existing.Customer_id"),
        "left"
    )

    updating_data=merged_df.filter(col('Customer_id').isNotNull())

    updating_data = updating_data.withColumn('Previous_Address', col('existing.Current_Address'))
    updating_data = updating_data.withColumn('Current_Address', col('new.Address'))


    updating_data = updating_data.select(
    col('existing.Customer_id').alias('Customer_id'),
    col('existing.Name').alias('Name'),
    col('Current_Address'),  
    col('Previous_Address')  
    )

    inserting_data=merged_df.filter(col('Customer_id').isNull())
    inserting_data=inserting_data.select(
        col('CustomerID').alias('Customer_id'),
        col('new.Name').alias('Name'),
        col('new.Address').alias('Current_Address'),
        lit(None).alias('Previous_Address')
        )

    result_df=updating_data.union(inserting_data)
    result_df.show()
    update_table(result_df)

new_records=[{'CustomerID': 1, 'Name': 'X', 'Address': 'new york'},
{'CustomerID': 2, 'Name': 'Jhone doe', 'Address': 'San Francisco'}]
SCD3(new_records)
print("done")