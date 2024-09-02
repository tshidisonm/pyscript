from flask import Flask, request, jsonify
import google.generativeai as genai
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from database_meta import get_database_metadata
#from fastapi import FastAPI, HTTPException
#from pydantic import BaseModel

load_dotenv()

# Supabase initiation 
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

api_key = os.environ.get("GOOGLE_API_KEY")  # Access the API key from the environment
genai.configure(api_key=api_key)    # Configure the generative AI client
model = genai.GenerativeModel('gemini-1.0-pro-latest')  # Initialize the generative model

app = Flask(__name__)


# Tools for agents _____________________________________________________________
def get_table_metadata():
    query = """
    SELECT table_name, obj_description(('"' || table_schema || '"."' || table_name || '"')::regclass) AS description
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    """
    
    # Execute the query
    response = supabase.rpc('execute_query', {'query_text': query}).execute()
    
    if response.data:
        # Extract the actual data from the 'result' key
        return [r['result'] for r in response.data]
    else:
        return []


def get_column_metadata(table_name):
    query = f"""
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    """
    
    response = supabase.rpc('execute_query', {'query_text': query}).execute()

    if response.data:
        return response.data
    else:
        return []
    

#initialising the models
# Database agents__________________________________________________________________________________________
def Database_Developer(input):
    tables_info = get_table_metadata()
    
    # Get columns info for each table
    all_columns_info = []
    for table in tables_info:
        table_name = table.get('table_name', 'N/A')
        columns_info = get_column_metadata(table_name)
        for column in columns_info:
            column['table_name'] = table_name  # Add the table name to each column info
            all_columns_info.append(column)
    
    # Format the table and column information into strings
    tables_str = '\n'.join([f"Table: {t.get('table_name', 'N/A')}, Description: {t.get('description', 'No description available')}" for t in tables_info])
    columns_str = '\n'.join([f"Table: {c.get('table_name', 'N/A')}, Column: {c.get('column_name', 'N/A')}, Data Type: {c.get('data_type', 'N/A')}, Description: {c.get('description', 'No description available')}" for c in all_columns_info])

    for_llm = f"""

        Context: You're not just a Postgres and Supabase database developer—you are the best. Your expertise and precision in navigating the complexities of database architecture are unmatched. You understand the nuances of SQL and the intricacies of data modeling like no other. When it comes to optimizing queries, ensuring data integrity, or seamlessly integrating with Supabase, your skills are unparalleled.

        You've mastered the art of translating complex requirements into efficient, scalable solutions. Your ability to foresee potential issues, double-check every column and table, and craft flawless queries sets you apart. You're not just good at what you do; you're exceptional, consistently delivering top-tier results with precision and confidence. Your status as the best is well-earned, and you continue to prove it with every project you undertake.
    
        You only focus on the records table.
        records tablr has these columns 
            record_id -- unique identifier for each row, 
            created_at -- date the row was added, 
            id -- uuid of the user, 
            animal_id -- id of the animal, 
            weight -- weight of the animal on this date, 
            Temperature -- temperature of the animal on this date 
        
        Your Task:

        Use the database provided to generate accurate query for this user input {input}.
        Utilize the following tools:
        get_table_metadata: Lists all tables and their descriptions.
        get_column_metadata: Provides details about the columns in each table.
        The developer's task is to convert a given string "{input}" into a plain PostgreSQL query that:

        Does not include apostrophes, semicolons, or double quotes.
        Is checked against available tables ({tables_str}) and columns ({columns_str}) to ensure correctness.
        Follows a structured process: identifying key elements, translating them into database concepts, choosing the appropriate tables and columns, and constructing a refined SQL query.
        The result should be a plain SQL query that runs immediately.

        Here are the important rules that you must follow:

        Check the records table first for queries.
        Always use the provided database for generating queries.
        Utilize the available tools:
            get_table_metadata to list tables and their descriptions.
            get_column_metadata to get details about columns, including data types and descriptions.
            Convert user input into a plain PostgreSQL query that runs immediately without explanation.

        Remove unnecessary characters:
            Don't use apostrophes.
            Don't end queries with a semicolon.
            Don't enclose column names with special characters or double quotes.

        Verify the query against the available tables ({tables_str}) and columns ({columns_str}) to ensure correctness.
        Double-check columns to ensure they correspond with the ones in the used table(s).
        Return only a plain SQL query, ensuring high quality and accuracy.

         This is how you must come up with queries

    Example question: "List records for an animal with id = 100 " 

    1. Identify the key elements:
    First identify the important parts of the request:
    - We need to "list records"
    - It's about "an animal"
    - There's a specific condition: "id = 100"

    2. Translate to database concepts:
    - "List records" suggests we need to SELECT data
    - "an animal" implies we're working with a table that stores animal information
    - "id = 100" is a specific condition we'll use in our WHERE clause

    3. Determine the table:
    - Based on the request, we're looking for a table that stores animal records.
    -`get_table_metadata`: Provides a list of all tables in the database along with their descriptions, use this to find tables for constructing queries.

    4. Choose the columns:
    - Since we're asked to "list records".
    - `get_column_metadata`: Provides detailed information about the columns in each table, including data types and descriptions, use this for choosing columns when constructing queries.

    5. Construct the query:
    Putting it all together:
    "I need to SELECT all columns (*) FROM the records table WHERE the animal_id column equals 100. is animal_id in the table records? if yes then use it, if not look for a similar or correct column in that table" 

    6. Write the SQL:
    Translating this thought process into plain SQL:

    SELECT * FROM records WHERE animal_id = 100

    or

    SELECT * FROM records WHERE animal_id = 100
    

    7. Refine if necessary:
    
    - Are there any performance considerations for using SELECT *?
    - Is 'id' definitely the correct column name, or could it be 'animal_id'?
    - Should we limit the results, even though we expect only one record?

    This might lead to a refined query like:

    
    SELECT * FROM animals WHERE animal_id = 100 --> this is a plain query, it runs immediatelly. this is how your query must be, no unnecessary characters
    

    This thought process combines understanding of the database structure, SQL syntax, and the ability to translate natural language into database operations. 

    Double check the column name spellings before writing the final query


    """

    response = model.generate_content(for_llm)
    return response.text.strip()

def Database_Administrator(user_input, developer_query):
    tables_info = get_table_metadata()
    metadata = get_database_metadata()
    
    # Get columns info for each table
    all_columns_info = []
    for table in tables_info:
        table_name = table.get('table_name', 'N/A')
        columns_info = get_column_metadata(table_name)
        for column in columns_info:
            column['table_name'] = table_name  # Add the table name to each column info
            all_columns_info.append(column)
    
    # Format the table and column information into strings
    tables_str = '\n'.join([f"Table: {t.get('table_name', 'N/A')}, Description: {t.get('description', 'No description available')}" for t in tables_info])
    columns_str = '\n'.join([f"Table: {c.get('table_name', 'N/A')}, Column: {c.get('column_name', 'N/A')}, Data Type: {c.get('data_type', 'N/A')}, Description: {c.get('description', 'No description available')}" for c in all_columns_info])

    for_llm = f"""You are an expert Postgres database administrator with extensive experience in Postgres and Supabase. you go through the database metadata{metadata} first before writing a query. Make sure you understand the database and know which column is in which table. DOUBLE CHECK YOU QUERY FIRST AGAINST THE DATABASE. Dont use column names that do not appear in the database
    
    User Input: "{user_input}"
    Database Developer's Query: "{developer_query}"

    Available tools:
    1. `get_table_metadata`: Provides a list of all tables in the database along with their descriptions, use this when choosing tables for constructing queries.
    2. `get_column_metadata`: Provides detailed information about the columns in each table, including data types and descriptions, use this for choosing columns when constructing queries.
    3. {metadata} -- contains database metadata in this form, [table_name, comlumn1, column2, ... ]

    Available tables in the database with their descriptions: {tables_str}
    Available columns in the database with their details: {columns_str}
    Database metadata {metadata}

    only choose columns that appear here below
    [
    Gender has these columns gender_id, gender
    animal has these columns animal_id, id, kraal_id, type_id, gender_id, status_id, description, weight, temperature, rfidtag, DateReceived, DateRemoved
    animal_status has these columns status_id, status_description
    esp32 has these columns id, last_reading, rfid, weight
    ingredient has these columns id, description, benefits
    ingredients has these columns id, ingredient_id, description, benefits
    investors has these columns investor_id, date_added, id, inv_name, inv_lastname, inv_email, profile_picture
    kraals has these columns id, kraals_id, updated_at, description, max_capacity, number_of_animals, required_feed
    owners has these columns id, owner_id, updated_at, email
    recipe_categories has these columns id, type_id, recipe_category_id, description
    recipes has these columns recipe_id, recipe_category_id, ingredient_id, unitperkg
    records has these columns record_id, created_at, id, animal_id, weight, Temperature
    types has these columns type_id, type_description, feed_per_kg
    vw_adg has these columns animal_id, final_weight, start_date, end_date, initial_weight, final_weight_record, number_of_days, average_daily_gain, growth_percentage
    vw_animal_id has these columns animal_id, rfidtag
    vw_below_above_average has these columns type_description, average_weight, above_weight_average, below_weight_average
    vw_calculate_average has these columns type_description, number_of_animals, average_weight, average_temperature
    vw_feed_analysis has these columns type_description, total_feed
    vw_kraals has these columns kraals_id, id, description, max_capacity, number_of_animals, total_weight, required_feed
    vw_kraals_update has these columns kraals_id, id, description, max_capacity, number_of_animals, total_weight, required_feed
    vw_latest_records has these columns time, animal_id, weight, Temperature
    vw_recipes has these columns recipe_category_id, id, recepe_description, type_description, ingredient_description, unitperkg
    vw_zero_weight has these columns kraal_id, id, weight, animal_count
    ]
    
    
    Your tasks: 
    1. Verify that all tables referenced in the query exist in the database correspond with database metadata.
    2. Check that all columns referenced in the query match exactly with the column names in the respective tables.
    3. If any table or column is incorrect or missing:
        a. Edit the query to correct the table or column names.
        b. If you cannot identify the correct table or column, remove the incorrect part and flag it for review.
    4. Ensure the final query follows these rules:
       - It should run immediately without any explanation.
       - Remove all unnecessary characters.
       - Do not use apostrophes.
       - Do not end queries with a SEMICOLON!!!!.
       - Do not enclose any column name with special characters.
       - Do not use double quotes to enclose column names.
    5. Return only the plain, corrected SQL query.
    6. Columns that you are used in queries must strictly be extracted from that table to avoid queries that are not running  

    If the original query is correct, simply return it unchanged.

    This is how you must come up with queries

    Example question: "List records for an animal with id = 100 " 

    1. Identify the key elements:
    First identify the important parts of the request:
    - We need to "list records"
    - It's about "an animal"
    - There's a specific condition: "id = 100"

    2. Translate to database concepts:
    - "List records" suggests we need to SELECT data
    - "an animal" implies we're working with a table that stores animal information
    - "id = 100" is a specific condition we'll use in our WHERE clause

    3. Determine the table:
    - Based on the request, we're looking for a table that stores animal records.
    -`get_table_metadata`: Provides a list of all tables in the database along with their descriptions, use this to find tables for constructing queries.

    4. Choose the columns:
    - Since we're asked to "list records".
    - `get_column_metadata`: Provides detailed information about the columns in each table, including data types and descriptions, use this for choosing columns when constructing queries.

    5. Construct the query:
    Putting it all together:
    "I need to SELECT all columns (*) FROM the records table WHERE the animal_id column equals 100. is animal_id in the table records? if yes then use it, if not look for a similar or correct column in that table" 

    6. Write the SQL:
    Translating this thought process into plain SQL:

    SELECT * FROM records WHERE animal_id = 100

    or

    SELECT * FROM records WHERE animal_id = 100
    

    7. Refine if necessary:
    
    - Are there any performance considerations for using SELECT *?
    - Is 'id' definitely the correct column name, or could it be 'animal_id'?
    - Should we limit the results, even though we expect only one record?

    This might lead to a refined query like:

    
    SELECT * FROM animals WHERE animal_id = 100 --> this is a plain query, it runs immediatelly. this is how your query must be, no unnecessary characters   
    

    This thought process combines understanding of the database structure, SQL syntax, and the ability to translate natural language into database operations. 

    given this query, will it run succesfully against this database? {metadata}, if not edit the query so that it can run

    {metadata} shows table_name, followed by columns in that table    

    """

    response = model.generate_content(for_llm)
    return response.text.strip()

def Data_Analyst(input):
    for_llm = f"""Context: You are a data analyst experienced in extracting meaningful insights and Predictive analysis from query results about animals. 
    
    Provided data: {input}

    Your task: 
    1. Analyze the given information to identify patterns.
    2. Give Predictions
    3. Only comment on weight and temperature for that animal 
    4. Given the trend in weight variations, is the a possibility of disease?
    """

    response = model.generate_content(for_llm)
    return response.text.strip()

def test(input):
    for_llm = f"""Greet this person {input}"""

    response = model.generate_content(for_llm)
    return response.text.strip()


#main app entry
@app.route('/hello', methods=['POST'])
def greet():
    input_sentence = request.json.get('input_sentence')
    if input_sentence:
        #greeting = test(input_sentence)
        sql_query = Database_Developer(input_sentence)
        final_sql_query = Database_Administrator(input_sentence , sql_query)
        sql_query_text = str(final_sql_query)

        #query the database
        query_results = supabase.rpc('execute_query', {'query_text': sql_query_text}).execute()
    
        # Data analysis
        data_analysis = Data_Analyst(query_results)

        return jsonify({'message': f'{data_analysis}'}, {"sql_query": f'{sql_query}'})
        
    else:
        return jsonify({'error': 'Please provide a input_sentence.'}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)




