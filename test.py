import json
import psycopg2
from faker import Faker
import requests
import os
from dotenv import load_dotenv

load_dotenv()

class HubspotContactManager:
    def __init__(self):
        self.hubspot_token = os.getenv('HUBSPOT_TOKEN')  # Get HubSpot token from environment variable
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),  # Get database host from environment variable
            database=os.getenv('DB_NAME'),  # Get database name from environment variable
            user=os.getenv('DB_USER'),  # Get database user from environment variable
            password=os.getenv('DB_PASSWORD')  # Get database password from environment variable
        )
    
    def create_table(self):
        try:
            curr = self.conn.cursor()
            create_table_query = """
            CREATE TABLE IF NOT EXISTS jansher_contact (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            hubspot_id SERIAL,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            curr.execute(create_table_query)  # Execute the SQL query to create the table
            self.conn.commit()  # Commit the changes to the database
            curr.close()  # Close the cursor
            print("Table created successfully.")
        except (Exception, psycopg2.DatabaseError) as error:
            print('Error:', error)
    
    def insert_records_in_db(self):
        total_db_records_created = 0
        try:
            curr = self.conn.cursor()
            fake = Faker()  # Create a Faker instance for generating fake data
            new_records = []

            # Generate and insert 2 records into the table
            for _ in range(2):
                first_name = fake.first_name()
                last_name = fake.last_name()
                email = fake.email()
                # Define the SQL query to insert a record into the table
                insert_query = """
                    INSERT INTO jansher_contact (first_name, last_name, email)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """

                # Execute the SQL query to insert the record
                curr.execute(insert_query, (first_name, last_name, email))
                record_id = curr.fetchone()[0]
                new_records.append({'id': record_id, 'first_name': first_name, 'last_name': last_name, 'email': email})
                total_db_records_created += 1

            # Commit the changes to the database
            self.conn.commit()
            curr.close()
            print(f'Records created in DB: {total_db_records_created}')
            return new_records
        except (Exception, psycopg2.DatabaseError) as error:
            print('Error:', error)

    def update_hubspot_ids_in_db(self, new_contacts):
        total_db_records_updated = 0
        try:
            cursor = self.conn.cursor()
            # Update the contacts in db with the hubspot_id
            for contact in new_contacts:
                update_query = """
                UPDATE jansher_contact
                SET hubspot_id = %s
                WHERE id = %s
                """
                # Execute the SQL query to update the record
                cursor.execute(update_query, (contact['hubspot_id'], contact['db_id']))
                total_db_records_updated += 1

            # Commit the changes to the database
            self.conn.commit()
            cursor.close()
            print(f'Records updated in DB: {total_db_records_updated}')
        except (Exception, psycopg2.DatabaseError) as error:
            print('Error:', error)

    def check_and_insert_records_in_hubspot(self, new_records):
        search_contact_url = "https://api.hubapi.com/contacts/v1/search/query"
        create_contact_url = "https://api.hubapi.com/contacts/v1/contact/"
        headers = {
            "Authorization": f'Bearer {self.hubspot_token}',  # Set the Authorization header with HubSpot token
            "Content-Type": "application/json",
        }
        total_records_created = 0
        records_to_update_in_db = []
        try:
            for record in new_records:
                params = {
                    "q": f'{record["first_name"]} {record["last_name"]}',
                    "property": ["firstname", "lastname"],  # Search properties for first name and last name
                }
                search_contact_response = requests.get(search_contact_url, params=params, headers=headers)

                if search_contact_response.status_code == 200:
                    data = search_contact_response.json()
                    total_matching_results = data['total']
                    if total_matching_results == 0:
                        print(f'No matching results. Creating a new contact with name {record["first_name"]} {record["last_name"]}.')
                        new_contact_data = json.dumps({
                            "properties": [
                                {
                                    "property": "email",
                                    "value": record['email']
                                },
                                {
                                    "property": "firstname",
                                    "value": record['first_name']
                                },
                                {
                                    "property": "lastname",
                                    "value": record['last_name']
                                },
                            ]
                        })
                        create_contact_response = requests.post(url=create_contact_url, data=new_contact_data, headers=headers)
                        if create_contact_response.status_code == 200:
                            total_records_created += 1
                            contact_vid = json.loads(create_contact_response.text)['vid']
                            records_to_update_in_db.append({'db_id': record['id'], 'hubspot_id': contact_vid})
                        else:
                            print("Error creating contact..")
                    else:
                        print(f'Contact already exists for {record["first_name"]} {record["last_name"]}.')
                else:
                    print("Request failed with status code:", search_contact_response.status_code)
        except (Exception, psycopg2.DatabaseError) as error:
            print('Error:', error)
        finally:
            print(f'Contacts created in HubSpot: {total_records_created}')
            return records_to_update_in_db

if __name__ == '__main__':
    contact_manager = HubspotContactManager()  # Create an instance of HubspotContactManager class
    contact_manager.create_table()  # Create the table if it doesn't exist
    new_db_records = contact_manager.insert_records_in_db()  # Insert records into the database
    hubspot_ids_to_update_in_db = contact_manager.check_and_insert_records_in_hubspot(new_db_records)  # Check and insert records into HubSpot
    contact_manager.update_hubspot_ids_in_db(hubspot_ids_to_update_in_db)  # Update the hubspot_id in the database
