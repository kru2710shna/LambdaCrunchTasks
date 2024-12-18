import sqlite3
import json
import os

# Database configuration
DATABASE_NAME = "stores.db"
TABLE_NAME = "stores"

# Function to create the database and table
def create_database():
    connection = sqlite3.connect(DATABASE_NAME)
    cursor = connection.cursor()

    # Create table without 'url' column if it doesn't exist
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            store_name TEXT,
            store_description TEXT,
            store_location TEXT,
            store_phone_number TEXT
            -- 'url' will be handled separately
        )
    """)

    # Check existing columns
    cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
    columns = [info[1] for info in cursor.fetchall()]
    print(f"Existing columns: {columns}")

    # Add 'url' column if it doesn't exist without UNIQUE
    if "url" not in columns:
        cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN url TEXT")
        print("Column 'url' added to the table.")
    else:
        print("Column 'url' already exists.")

    # Create a UNIQUE index on 'url' if it doesn't exist
    cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_url ON {TABLE_NAME} (url)")
    print("Unique index on 'url' ensured.")

    connection.commit()
    connection.close()
    print(f"Database '{DATABASE_NAME}' is ready with table '{TABLE_NAME}'!")

# Function to print the table schema for verification
def print_table_schema():
    connection = sqlite3.connect(DATABASE_NAME)
    cursor = connection.cursor()
    cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
    columns = cursor.fetchall()
    print("\nTable Schema:")
    for col in columns:
        print(col)
    
    # Check indexes
    cursor.execute(f"PRAGMA index_list({TABLE_NAME})")
    indexes = cursor.fetchall()
    print("\nIndexes:")
    for idx in indexes:
        print(idx)
    
    connection.close()

# Function to insert data into the SQLite database
def insert_data(file_path):
    if not os.path.exists(file_path):
        print(f"JSON file '{file_path}' does not exist.")
        return

    connection = sqlite3.connect(DATABASE_NAME)
    cursor = connection.cursor()

    # Load JSON data
    with open(file_path, "r", encoding='utf-8') as json_file:
        try:
            data = json.load(json_file)
            if not isinstance(data, list):
                print(f"JSON data is not a list. Found type: {type(data)}")
                return
            else:
                print(f"Loaded {len(data)} entries from JSON.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            return

    # Insert each store into the database
    inserted_count = 0
    skipped_count = 0
    for idx, store in enumerate(data, start=1):
        try:
            url = store.get("url", "").strip()
            store_name = store.get("store_name", "").strip()
            store_description = store.get("store_description", "").strip()
            store_location = store.get("store_location", "").strip()
            store_phone_number = store.get("store_phone_number", "").strip()

            if not url:
                print(f"Entry {idx}: Missing 'url'. Skipping.")
                skipped_count += 1
                continue

            cursor.execute(
                f"""
                INSERT INTO {TABLE_NAME} (url, store_name, store_description, store_location, store_phone_number)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    url,
                    store_name,
                    store_description,
                    store_location,
                    store_phone_number,
                ),
            )
            inserted_count += 1
            print(f"Entry {idx}: Inserted '{store_name}' successfully.")
        except sqlite3.IntegrityError as e:
            print(f"Entry {idx}: Skipping duplicate or invalid entry for URL '{url}': {e}")
            skipped_count += 1
        except Exception as e:
            print(f"Entry {idx}: Error inserting data for URL '{url}': {e}")
            skipped_count += 1

    connection.commit()
    connection.close()
    print(f"\nData insertion complete. Inserted: {inserted_count}, Skipped: {skipped_count}.")

# Function to check for duplicates and NULLs in 'url' column
def check_duplicates_and_nulls():
    connection = sqlite3.connect(DATABASE_NAME)
    cursor = connection.cursor()
    
    # Check for NULL or empty URLs
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE url IS NULL OR url = ''")
    null_count = cursor.fetchone()[0]
    print(f"Number of NULL or empty URLs: {null_count}")
    
    # Check for duplicate URLs
    cursor.execute(f"""
        SELECT url, COUNT(*) as cnt
        FROM {TABLE_NAME}
        GROUP BY url
        HAVING cnt > 1
    """)
    duplicates = cursor.fetchall()
    if duplicates:
        print("Duplicate URLs found:")
        for dup in duplicates:
            print(dup)
    else:
        print("No duplicate URLs found.")
    
    connection.close()

# Optional: Function to display all entries in the table
def display_all_entries():
    connection = sqlite3.connect(DATABASE_NAME)
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {TABLE_NAME}")
    rows = cursor.fetchall()
    print(f"\nTotal entries in '{TABLE_NAME}': {len(rows)}")
    for row in rows:
        print(row)
    connection.close()

if __name__ == "__main__":
    # Step 1: Create the database and table
    create_database()

    # Step 1.5: Print table schema for verification
    print_table_schema()

    # Step 1.6: Check for duplicates and NULLs before insertion
    print("\nChecking for duplicates and NULLs before insertion:")
    check_duplicates_and_nulls()

    # Step 2: Insert data from the JSON file
    insert_data("final_scrape_21.json")

    # Step 3: Optional - Re-check duplicates and NULLs after insertion
    print("\nAfter data insertion:")
    check_duplicates_and_nulls()

    # Step 4: Optional - Display all entries
    display_all_entries()
