import pandas as pd 
import psycopg2
import datetime


def get_connection():
    return psycopg2.connect(
        dbname='firstDB',user='postgres',password='adarshjain',host='localhost',port='5432'
    )


def load_existing_data():
    conn=get_connection()
    query="SELECT * FROM Customer_SCD3"
    df=pd.read_sql_query(query,conn)
    conn.close()
    return df

def update_databse(records):
    conn=get_connection()
    cursor=conn.cursor()
    query="""UPDATE Customer_SCD3 
    SET Previous_Address=Current_Address,Current_Address=%s
    WHERE Customer_id=%s """
    cursor.executemany(query,records)
    conn.commit()
    cursor.close()
    conn.close()

def insert_in_database(records):
    conn=get_connection()
    cursor=conn.cursor()
    query="""
    INSERT INTO Customer_SCD3 (Customer_id,Name,Current_Address,Previous_Address)
    Values(%s,%s,%s,%s)"""
    cursor.executemany(query,records)
    conn.commit()
    cursor.close()
    conn.close()

def scd_type3(new_data):
    existing_dataframe=load_existing_data()
    new_dataframe=pd.DataFrame(new_data)

    merged_data=new_dataframe.merge(
        existing_dataframe,
        how='left',
        left_on='CustomerID',
        right_on='customer_id',
        suffixes=('_new','_existing')
    )

    updates=merged_data[~merged_data['customer_id'].isna()]
    print(updates)
    if not updates.empty:
        updates=updates.apply(
            lambda row : (row['Address'],row['customer_id']),axis=1
            ).tolist()
    else:
        updates=[]

    insert_data=merged_data[merged_data['customer_id'].isna()].apply(
        lambda row : (
            row['CustomerID'],
            row['Name'],
            row['Address'],
            None
        ),axis=1
    ).tolist()

    if updates:
        update_databse(updates)
        print('updates done')
    if insert_data:
        insert_in_database(insert_data)
        print('insertion done')

new_data=[{'CustomerID': 3, 'Name': 'John Doe', 'Address': 'Francisco'}]
scd_type3(new_data)
print("done")