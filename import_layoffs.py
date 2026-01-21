"""
I'm importing layoffs.csv into my MySQL database.
This handles converting data types and imports everything.
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# I load my environment variables from .env file
load_dotenv()

# My database connection settings
DB_CONFIG = {
    'host': 'localhost',  # I can also use '127.0.0.1'
    'user': 'root',  # My MySQL username
    'password': '8282Johnny',  # My MySQL password
    'database': 'world_layoffs',
    'allow_local_infile': True
}

# Where my CSV file is located
CSV_FILE = '/Users/orproja/Downloads/layoffs.csv'

def clean_numeric(value):
    """I convert a value to an integer, handling missing data"""
    # I check if the value is missing in any form
    if pd.isna(value) or value == '' or value == 'NULL' or str(value).lower() == 'nan':
        return None
    try:
        # I convert to float first, then to int
        num_value = float(value)
        if pd.isna(num_value):  # I check again after converting
            return None
        return int(num_value)
    except (ValueError, TypeError, OverflowError):
        return None

def clean_decimal(value):
    """I convert a value to a float, handling missing data"""
    # I check if the value is missing in any form
    if pd.isna(value) or value == '' or value == 'NULL' or str(value).lower() == 'nan':
        return None
    try:
        num_value = float(value)
        if pd.isna(num_value):  # I check again after converting
            return None
        return num_value
    except (ValueError, TypeError, OverflowError):
        return None

def clean_string(value):
    """I clean string values and convert NaN to None"""
    if pd.isna(value) or str(value).lower() == 'nan' or value == '':
        return None
    cleaned = str(value).strip()
    if cleaned.lower() == 'nan' or cleaned == '':
        return None
    return cleaned

def import_csv_to_mysql():
    """I read the CSV file and import it into MySQL"""
    
    print("Reading CSV file...")
    # I read the CSV and treat these as missing values
    df = pd.read_csv(CSV_FILE, na_values=['NULL', 'null', '', 'NaN', 'nan', 'None', 'none'])
    
    print(f"Found {len(df)} rows in CSV")
    print(f"Columns: {list(df.columns)}")
    print("\nFirst few rows:")
    print(df.head())
    
    # I clean the data by replacing NaN with None
    print("\nCleaning data...")
    # I replace all NaN values with None first
    df = df.where(pd.notnull(df), None)
    
    # I clean each column using my cleaning functions
    df['company'] = df['company'].apply(clean_string)
    df['location'] = df['location'].apply(clean_string)
    df['industry'] = df['industry'].apply(clean_string)
    df['total_laid_off'] = df['total_laid_off'].apply(clean_numeric)
    df['percentage_laid_off'] = df['percentage_laid_off'].apply(clean_decimal)
    df['date'] = df['date'].apply(clean_string)
    df['stage'] = df['stage'].apply(clean_string)
    df['country'] = df['country'].apply(clean_string)
    df['funds_raised_millions'] = df['funds_raised_millions'].apply(clean_numeric)
    
    # I do a final check to replace any remaining NaN with None
    df = df.where(pd.notnull(df), None)
    
    # I connect to MySQL
    print("\nConnecting to MySQL...")
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        print("Connected successfully!")
        
        # I select my database
        cursor.execute("USE world_layoffs")
        
        # I create the table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS layoffs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company VARCHAR(255),
            location VARCHAR(255),
            industry VARCHAR(255),
            total_laid_off INT NULL,
            percentage_laid_off DECIMAL(10,4) NULL,
            date VARCHAR(50),
            stage VARCHAR(100),
            country VARCHAR(100),
            funds_raised_millions INT NULL
        )
        """
        cursor.execute(create_table_query)
        print("Table created/verified")
        
        # I clear existing data before importing
        cursor.execute("TRUNCATE TABLE layoffs")
        print("Cleared existing data")
        
        # I insert the data
        print("\nInserting data...")
        insert_query = """
        INSERT INTO layoffs 
        (company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # I track how many rows I insert and how many fail
        rows_inserted = 0
        rows_failed = 0
        
        for index, row in df.iterrows():
            try:
                # I make sure any NaN or None values are handled
                def safe_value(val):
                    if pd.isna(val) or val is None or str(val).lower() == 'nan':
                        return None
                    return val
                
                values = (
                    safe_value(row['company']),
                    safe_value(row['location']),
                    safe_value(row['industry']),
                    safe_value(row['total_laid_off']),
                    safe_value(row['percentage_laid_off']),
                    safe_value(row['date']),
                    safe_value(row['stage']),
                    safe_value(row['country']),
                    safe_value(row['funds_raised_millions'])
                )
                
                # I double-check no 'nan' strings are in my values
                values = tuple(None if (v is not None and str(v).lower() == 'nan') else v for v in values)
                
                cursor.execute(insert_query, values)
                rows_inserted += 1
                
                if (rows_inserted % 100) == 0:
                    print(f"Inserted {rows_inserted} rows...")
                    
            except Error as e:
                rows_failed += 1
                if rows_failed <= 5:  # I only show the first 5 errors
                    print(f"Error inserting row {index + 2}: {e}")  # +2 because CSV has header row
                    print(f"Row data: {row.to_dict()}")
                continue
        
        # I save all my changes to the database
        connection.commit()
        print(f"\nSuccessfully inserted {rows_inserted} rows")
        if rows_failed > 0:
            print(f"Failed to insert {rows_failed} rows")
        
        # I check how many rows are in the database
        cursor.execute("SELECT COUNT(*) FROM layoffs")
        count = cursor.fetchone()[0]
        print(f"\nTotal rows in database: {count}")
        
        # I show a sample of the data
        cursor.execute("SELECT * FROM layoffs LIMIT 5")
        print("\nSample data:")
        for row in cursor.fetchall():
            print(row)
        
    except Error as e:
        print(f"Error: {e}")
        if connection.is_connected():
            connection.rollback()
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("\nMySQL connection closed")

if __name__ == "__main__":
    # I ask for my MySQL password if I didn't set it
    if not DB_CONFIG['password']:
        DB_CONFIG['password'] = input("Enter MySQL password (press Enter if no password): ")
    
    import_csv_to_mysql()

