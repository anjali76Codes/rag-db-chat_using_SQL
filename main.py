import streamlit as st
import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import os
import google.generativeai as genai

# Load environment variables from .env file
load_dotenv()

# Get the database environment variables
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Get the Google API key
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# Configure Google Genai Key
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")


# Create a connection to the database using SQLAlchemy
def create_connection():
    try:
        engine = create_engine(
            f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        conn = engine.connect()
        return engine, conn
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None, None


# Function to execute SQL query and return the result
def execute_query(query):
    engine, conn = create_connection()
    if conn is not None:
        try:
            # Check if it's a SELECT query
            if query.strip().lower().startswith("select"):
                # Use Pandas to execute the query and return the result as a DataFrame for SELECT queries
                df = pd.read_sql(query, conn)
                conn.close()
                return df
            else:
                # For INSERT, UPDATE, DELETE use connection.execute() instead
                with conn.begin():  # Ensures transaction management (commit or rollback)
                    conn.execute(text(query))  # Execute the non-SELECT query safely
                conn.close()
                return None  # No rows are returned for non-SELECT queries
        except Exception as e:
            st.error(f"Error executing the query: {e}")
            return None
    return None


# Function to get SQL query suggestion from Gemini Pro
def get_sql_suggestion(user_input):
    if not user_input:
        return ""

    try:
        # Define the examples and the prompt for generating a SQL query suggestion
        prompt = f"""
        As a SQL expert, generate a PostgreSQL query based on the user's request: "{user_input}". 
        Ensure the query is syntactically correct and relevant to the request.
        If the request is unclear or cannot be translated to a SQL query, respond with an empty string.

        Here are some examples of queries based on a 'Students' table:

        Example 1:
        Request: "Show all students in the database."
        Query: "SELECT * FROM public.\"Students\";"

        Example 2:
        Request: "Show all students ordered by their ID."
        Query: "SELECT * FROM public.\"Students\" ORDER BY id ASC;"

        Example 3:
        Request: "List all male students."
        Query: "SELECT * FROM public.\"Students\" WHERE gender = 'Male';"

        Example 4:
        Request: "Find all students with GPA greater than 8."
        Query: "SELECT * FROM public.\"Students\" WHERE gpa > 8;"

        Example 5:
        Request: "Show students who joined after July 1st, 2022."
        Query: "SELECT * FROM public.\"Students\" WHERE join_date > '2022-07-01';"
        
        Now, based on the user's input, generate a similar SQL query.
        """

        response = model.generate_content(prompt)
        if hasattr(response, "text"):
            # Clean up the generated query by stripping out surrounding backticks, newlines, and extra spaces
            suggested_query = response.text.strip()
            # Remove the backticks (```) and make it a single line
            if suggested_query.startswith("```sql") and suggested_query.endswith("```"):
                suggested_query = suggested_query[
                    7:-3
                ].strip()  # Strip the triple backticks and extra spaces
            return suggested_query
        else:
            return ""
    except Exception as e:
        st.error(f"Error generating SQL suggestion: {e}")
        return ""


# Streamlit UI
def app():
    st.title("SQL Query Executor with Gemini Pro Suggestion")

    user_need = st.text_input("Describe your data need to get a SQL query suggestion:")

    # Get SQL suggestion from Gemini
    suggested_query = get_sql_suggestion(user_need)

    query = st.text_area(
        "Enter your SQL query here:", value=suggested_query, height=150
    )

    if st.button("Execute Query"):
        if query:
            result_df = execute_query(query)
            if result_df is not None and not result_df.empty:
                st.write("Query Results:")
                st.dataframe(result_df)  # Display the results as a table
            else:
                st.success("Query executed successfully, no result to display.")
        else:
            st.warning("Please enter a SQL query.")


if __name__ == "__main__":
    app()
