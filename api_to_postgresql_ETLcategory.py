import requests
import psycopg2

# Set up API credentials and endpoint
API_ENDPOINT = 'http://northwind.now.sh/api/categories'

# Set up PostgreSQL credentials
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'northwind'
DB_USER = 'postgres'
DB_PASSWORD = 'Alohomora94@'

def fetch_data_from_api():
    # Make API request
    response = requests.get(API_ENDPOINT)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Failed to fetch data from API. Status code: {response.status_code}')
        return None
    
def store_data_in_postgresql(data):
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS category_table (
            id INT,
            description VARCHAR,
            name VARCHAR
        )
    '''
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into the table
    for entry in data:
        insert_query = '''
            INSERT INTO category_table (id, description, name)
            VALUES (%s, %s, %s)
        '''
        cursor.execute(insert_query, (entry['id'], entry['description'], entry['name']))
        conn.commit()

    # Close the database connection
    cursor.close()
    conn.close()

def main():
    # Fetch data from API
    data = fetch_data_from_api()

    if data:
        # Store data in PostgreSQL
        store_data_in_postgresql(data)
        print('Data successfully stored in PostgreSQL.')

if __name__ == '__main__':
    main()