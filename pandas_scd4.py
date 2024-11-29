import pandas as pd
import psycopg2
from datetime import datetime

def get_connection():
    return psycopg2.connect(
        dbname='firstDB',user='postgres',password='adarshjain',host='localhost',port='5432'
    )


def load_existing_data():
    conn=get_connection()
    query="""
    SELECT * FROM Customer_SCD4_Current
    """
    df=pd.read_sql_query(query,conn)
    conn.close()
    return df
def updating_database(records):
    conn=get_connection()
    cursor=conn.cursor()
    for r in records:
        modify_on=datetime.now().date()
        customer_id, name, address = r

        cursor.execute("""
        INSERT INTO Customer_SCD4_History(Customer_id,Name,Address,modify_on)
        SELECT Customer_id,Name,Address,%s
        FROM Customer_SCD4_Current
        WHERE Customer_id=%s
        """,(modify_on,customer_id))

        cursor.execute("""
        UPDATE Customer_SCD4_Current
        SET Customer_id=%s,Name=%s,Address=%s
        WHERE Customer_id = %s;
        """, (customer_id,name, address, customer_id))
    conn.commit()
    cursor.close()
    conn.close()
    

def inserting_in_database(records):
    conn=get_connection()
    cursor=conn.cursor()
    query="""
    INSERT INTO Customer_SCD4_Current(Customer_id,Name,Address)
    VALUES (%s,%s,%s)
    """
    cursor.executemany(query,records)
    conn.commit()
    cursor.close()
    conn.close()

def scdtype4_pipeline(new_data):
    existing_df=load_existing_data()
    new_df=pd.DataFrame(new_data)

    merged_df=new_df.merge(
        existing_df,
        how='left',
        left_on='CustomerID',
        right_on='customer_id'
    )

    updates=merged_df[~merged_df['customer_id'].isna()]
    if not updates.empty:
        updates=updates.apply(
            lambda row : (
                row['CustomerID'],
                row['Name'],
                row['Address']
                ),axis=1
        ).tolist()
    else:
        updates=[]

    insert_new_data=merged_df[merged_df['customer_id'].isna()].apply(
        lambda row: (
            row['CustomerID'],
            row['Name'],
            row['Address']
        ),axis=1
    ).tolist()

    print("updates",updates)
    print("insert_new_data",insert_new_data)
    inserting_in_database(insert_new_data)
    updating_database(updates)
    print("done")

new_data=[{'CustomerID': 5, 'Name': 'Jhon Doe', 'Address': 'San Francisco'},
{'CustomerID': 2, 'Name': 'william', 'Address': 'San Francisco'}]
scdtype4_pipeline(new_data)
