from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit

jdbc_driver_path = "C:\\Users\\aadar\\Downloads\\postgresql-42.7.4.jar"
spark = SparkSession.builder.appName("SCD_TYPE2").config("spark.jars", jdbc_driver_path).getOrCreate()

def load_existing_data():
    return spark.read.format("jdbc").options(
        url="jdbc:postgresql://ep-icy-meadow-a8zfunjg.eastus2.azure.neon.tech:5432/firstDB?user=firstDB_owner&password=qLmOcA3FMj8C&sslmode=require",
        dbtable="Customer_SCD2",
        user="firstDB_owner",
        password="qLmOcA3FMj8C",
        driver="org.postgresql.Driver"
    ).load()

def save_new_data_df(records):
    return records.write.format("jdbc").options(
        url="jdbc:postgresql://ep-icy-meadow-a8zfunjg.eastus2.azure.neon.tech:5432/firstDB?user=firstDB_owner&password=qLmOcA3FMj8C&sslmode=require",
        dbtable="Customer_SCD2",
        user="firstDB_owner",
        password="qLmOcA3FMj8C",
        driver="org.postgresql.Driver"
    ).mode("overwrite").save()

def SCD2(new_records):

    existing_data = load_existing_data()

    new_dataset = spark.createDataFrame(new_records)
    new_dataset = new_dataset.withColumn('modify_on', current_date())

    merged_df = new_dataset.alias('new').join(
        existing_data.filter(col('Is_current') == True).alias('existing'),
        col('existing.Customer_id') == col('new.CustomerID'),
        "left"
    )
    # existing_data.show()
    # new_dataset.show()
    not_in_use=existing_data.filter(col("Is_current")==False)

    updating_records = merged_df.filter(merged_df["Customer_id"].isNotNull()) \
        .select(
            col("Customer_id").alias("Customer_id"),
            col("existing.Name").alias("Name"),
            col("existing.Address").alias("Address"),
            col("existing.Start_date").alias("Start_date"),
            col("new.modify_on").alias("End_date"),
            lit(False).alias("Is_current")
        )

    new_version_records_df = merged_df.filter(col("Customer_id").isNotNull()).select(
        col("CustomerID").alias("Customer_id"),
        col("new.Name").alias("Name"),
        col("new.Address").alias("Address"),
        current_date().alias("Start_date"),
        lit(None).cast("date").alias("End_date"),
        lit(True).alias("Is_current")
    )

    inserting_records = merged_df.filter(merged_df["Customer_id"].isNull()) \
        .select(
            col("CustomerID").alias("Customer_id"),
            col("new.Name").alias("Name"),
            col("new.Address").alias("Address"),
            col("modify_on").alias("Start_date"),
            lit(None).alias("End_date"),
            lit(True).alias("Is_current")
            )
    
    resulting_df=not_in_use.union(updating_records).union(new_version_records_df).union(inserting_records)
    resulting_df.show()
    save_new_data_df(resulting_df)
    print("data_inserted")




SCD2([{'CustomerID': 1, 'Name': 'John D', 'Address': 'San Francisco'},
      {'CustomerID': 3, 'Name': 'Joe', 'Address': 'New york'}])
print("done")



