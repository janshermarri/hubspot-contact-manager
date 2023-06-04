# Table Schema

### Table 1 Name: {Your Name} - Contact

### Schema: First Name, Last Name, Email, HubSpot ID, Create Date

# Problem Description

Create a Python script that runs every 15 minutes and performs the following actions:

    1. Create a table with the given schema if it doesn't already exist.
    2. Insert two random records into the table.
    3. Retrieve an updated Access Token from the HubSpot API.
    4. Get the newly created records from the database.
    5. Search for the contacts in HubSpot and create a record if it doesn't exist, or update the existing record.
    6. Update the records in the SQL database with the HubSpot ID obtained from the response.

# Notes

    1. The code should follow a class architecture.
    2. Proper code-handling techniques should be used.
    3. The code should be well-defined and properly commented.
    4. You are free to use any package for the cron service or PostgreSQL queries.
    5. It is recommended not to use HubSpot's Python package as it can be complex and requires a deep understanding of HubSpot CRM.

# Deployment

Deploy the script to an EC2 instance.
