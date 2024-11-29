import psycopg2
import pandas as pd
from datetime import datetime

# Database connection
def get_connection():
    return psycopg2.connect(
        dbname="firstDB", user="firstDB_owner", password="qLmOcA3FMj8C", host="ep-icy-meadow-a8zfunjg.eastus2.azure.neon.tech", port="5432"
    )

# Load existing data from the database
def load_existing_data():
    conn = get_connection()
    query = "SELECT * FROM Customer_SCD2;"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Bulk insert new records
def bulk_insert_new_records(new_records):
    conn = get_connection()
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO Customer_SCD2 ("Customer_id", "Name", "Address", "Start_date", "End_date", "Is_current")
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    cursor.executemany(insert_query, new_records)
    conn.commit()
    cursor.close()
    conn.close()

# Update existing records to set Is_current = FALSE and End_date
def bulk_update_end_dates(update_records):
    conn = get_connection()
    cursor = conn.cursor()
    update_query = """
        UPDATE Customer_SCD2 
        SET "End_date" = %s, "Is_current" = FALSE
        WHERE "Customer_id" = %s AND "Is_current" = TRUE;
    """
    cursor.executemany(update_query, update_records)
    conn.commit()
    cursor.close()
    conn.close()

# SCD Type 2 using Pandas
def scd_type_2_pipeline_pandas(new_data):
    # Load existing data from the database
    existing_data = load_existing_data()

    # Convert new data to DataFrame
    new_data_df = pd.DataFrame(new_data)
    new_data_df['Effective_Date'] = datetime.now().date()

    # Identify records to update
    merged_df = new_data_df.merge(
        existing_data[existing_data['Is_current'] == True], 
        how='left', 
        left_on='CustomerID', 
        right_on='Customer_id', 
        suffixes=('_new', '_existing')
    )

    # Records to update end dates
    updates = merged_df[~merged_df['Customer_id'].isna()]
    update_records = updates.apply(
        lambda row: (row['Effective_Date'], row['CustomerID']), axis=1
    ).tolist()

    new_records = updates.apply(
        lambda row: (
            row['CustomerID'],        
            row['Name_new'],             
            row['Address_new'],          
            row['Effective_Date'],       
            None,                       
            True                         
        ), axis=1
    ).tolist()


    new_inserts = merged_df[merged_df['Customer_id'].isna()].apply(
        lambda row: (
            row['CustomerID'], 
            row['Name_new'], 
            row['Address_new'], 
            row['Effective_Date'], 
            None, 
            True
        ), axis=1
    ).tolist()

    new_records.extend(new_inserts)
    
    # Perform database operations
    if update_records:
        bulk_update_end_dates(update_records)
    if new_records:
        bulk_insert_new_records(new_records)

def load_new_data(csv_path):
    new_records=pd.read_csv(csv_path)
    return new_records

csv_path='C:/Users/aadar/Desktop/zecdata/raw_data.csv'
new_data=load_new_data(csv_path)
scd_type_2_pipeline_pandas(new_data)
print('done')
