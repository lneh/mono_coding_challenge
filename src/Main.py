import csv
import sqlite3
from contextlib import closing

# Paths to the CSV files
user_csv = 'data/Users_Data.csv'
weddings_csv = 'data/Weddings_Data.csv'
weddings_06_2024_csv = 'data/weddings_06_2024.csv'
weddings_two_weeks_csv = 'data/weddings_two_weeks.csv'

# Create the 'users' and 'weddings' tables in the database.
# Drops the tables if they already exist to ensure a clean slate.
def create_tables(cursor):
    # Drop tables if they exist
    cursor.execute("DROP TABLE IF EXISTS users")
    cursor.execute("DROP TABLE IF EXISTS weddings")

    # Create 'users' table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY NOT NULL, 
            user_name TEXT NOT NULL
        )
    """)

    # Create 'weddings' table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weddings (
            user_id TEXT NOT NULL, 
            wedding_date DATE NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            PRIMARY KEY (user_id, wedding_date)
        )
    """)

# Insert data into the 'users' and 'weddings' tables from the provided CSV files.
def insert_data(cursor, user_csv, weddings_csv):
    try:
        # Insert data into 'users' table
        with open(user_csv) as file:
            reader = csv.DictReader(file)
            for row in reader:
                cursor.execute("""
                    INSERT INTO users (user_id, user_name) VALUES (?, ?)
                """, (row['user_id'], row['user_name']))
    except Exception as e:
        print(f"Error inserting data into users table: {e}")

    try:
        # Insert data into 'weddings' table
        with open(weddings_csv) as file:
            reader = csv.DictReader(file)
            for row in reader:
                cursor.execute("""
                    INSERT INTO weddings (user_id, wedding_date) VALUES (?, ?)
                """, (row['user_id'], row['wedding_date']))
    except Exception as e:
        print(f"Error inserting data into weddings table: {e}")

# Write the query result into a CSV file.
def write_in_file(filename, fieldnames, result):
    try:
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames)
            writer.writeheader()
            for row in result:
                writer.writerow({fieldnames[0]: row[0], fieldnames[1]: row[1]})
    except Exception as e:
        print(f"Error writing to file {filename}: {e}")

# Query for weddings happening in June 2024 and write the results to a CSV file.
def query_weddings_06_2024(cursor):
    cursor.execute("""
        SELECT user_name, wedding_date 
        FROM weddings
        NATURAL JOIN users 
        WHERE wedding_date BETWEEN '2024-06-01' AND '2024-06-30'
    """)
    write_in_file(weddings_06_2024_csv, ['user_name', 'wedding_date'], cursor.fetchall())

# Query for weddings happening within the next two weeks and write the results to a CSV file.
def query_weddings_two_weeks(cursor):
    cursor.execute("""
        SELECT user_name, wedding_date 
        FROM weddings
        NATURAL JOIN users
        WHERE wedding_date BETWEEN date('now') AND date('now', '+14 day')
    """)
    write_in_file(weddings_two_weeks_csv, ['user_name', 'wedding_date'], cursor.fetchall())

if __name__ == '__main__':
    # Connect to the SQLite database
    with sqlite3.connect("../mono.db") as con:
        # No need to explicitly close the cursor, it will be closed automatically when the block exits
        with closing(con.cursor()) as cursor:
            create_tables(cursor)
            insert_data(cursor, user_csv, weddings_csv)
            # Query and write weddings happening in June 2024
            query_weddings_06_2024(cursor)
            # Query and write weddings happening in the next two weeks
            query_weddings_two_weeks(cursor)
        # Commit the changes
        con.commit()
