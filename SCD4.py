from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,current_date


jdbc_driver_path = "C:\\Users\\aadar\\Downloads\\postgresql-42.7.4.jar"
spark = SparkSession.builder.appName("SCD_TYPE4").config("spark.jars", jdbc_driver_path).getOrCreate()

def get_existing_Current_data():
    return spark.read.format("jdbc").options(
        url="jdbc:postgresql://localhost:5432/firstDB",
        dbtable="Customer_SCD4_Current",
        user="postgres",
        password="adarshjain",
        driver="org.postgresql.Driver"
    ).load()

def get_existing_History_data():
    return spark.read.format("jdbc").options(
        url="jdbc:postgresql://localhost:5432/firstDB",
        dbtable="Customer_SCD4_History",
        user="postgres",
        password="adarshjain",
        driver="org.postgresql.Driver"
    ).load()

def update_Current_table(current_data):
    return current_data.write.format("jdbc").options(
        url="jdbc:postgresql://localhost:5432/firstDB",
        dbtable="Customer_SCD4_Current",
        user="postgres",
        password="adarshjain",
        driver="org.postgresql.Driver"
    ).mode("overwrite").save()

def update_History_table(history_data):
    return history_data.write.format("jdbc").options(
        url="jdbc:postgresql://localhost:5432/firstDB",
        dbtable="Customer_SCD4_History",
        user="postgres",
        password="adarshjain",
        driver="org.postgresql.Driver"
    ).mode("append").save()


def SCD4(new_data):

    existing_df=get_existing_Current_data()
    
    new_df=spark.createDataFrame(new_data)

    # existing_df.show()
    # History_df.show()

    merged_df=new_df.alias("new").join(
        existing_df.alias("existing"),
        col("new.CustomerID")==col("existing.Customer_id"),
        "left"
    )

    removing_data=merged_df.filter(col('Customer_id').isNotNull()).select(
        col("Customer_id"),
        col("existing.Name").alias("Name"),
        col("existing.Address").alias("Address")
    )
    removing_data = removing_data.withColumn('Modify_on', current_date())

    # removing_data.show()

    inserting_data=new_df.select(
        col('CustomerID').alias('Customer_id'),
        col('Name').alias('Name'),
        col('Address').alias('Address'),
        )

    # inserting_data.show()

    residing_data=existing_df.alias("r").join(
        new_df.alias("n"),
        existing_df["Customer_id"]==new_df["CustomerID"],
        "left"
    ).filter(col("CustomerID").isNull()).select(
        col("r.Customer_id"),
        col("r.Name"),
        col("r.Address")
    )

    # residing_data.show()
    current_data=residing_data.union(inserting_data)
    # current_data.show()
    update_Current_table(current_data)
    update_History_table(removing_data)

new_records=[{'CustomerID': 1, 'Name': 'X', 'Address': 'new york'},
{'CustomerID': 2, 'Name': 'Jhone doe', 'Address': 'San Francisco'}]
SCD4(new_records)
print("done")