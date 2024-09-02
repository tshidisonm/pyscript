from supabase import create_client, Client
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def get_database_metadata():
    # Query to retrieve table names and their columns
    query = """
    SELECT table_name, array_agg(column_name::text ORDER BY ordinal_position) as columns
    FROM information_schema.columns
    WHERE table_schema = 'public'
    GROUP BY table_name
    ORDER BY table_name
    """
    
    # Execute the query
    response = supabase.rpc('execute_query', {'query_text': query}).execute()

    # Print the raw response to debug
    #print("Raw Response:", response.data)
    
    if response.data:
        # Format the results
        formatted_metadata = []
        for entry in response.data:
            # Access the nested 'result' dictionary
            result = entry.get('result', {})
            
            table_name = result.get('table_name', 'N/A')  # Safely get table_name
            columns = ', '.join(result.get('columns', []))
            formatted_metadata.append(f"{table_name}, {columns}")
        
        return formatted_metadata
    else:
        return []

# Example usage:
#metadata = get_database_metadata()
#for table_info in metadata:
#    print(table_info)
    

