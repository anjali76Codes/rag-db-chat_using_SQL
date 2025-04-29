import streamlit as st
import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import os
import google.generativeai as genai
import requests

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

# Get the Hugging Face API key
HUGGING_FACE_API_KEY = os.getenv("HUGGING_FACE_API_KEY")

# Configure Google Genai Key
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

# Hugging Face API URL for text summarization
HF_URL = "https://api-inference.huggingface.co/models/facebook/bart-large-cnn"


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
        Query: "SELECT * FROM public.\"Students\" WHERE pointer > 8;"

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


def get_sql_response_explanation(df):
    # Convert the dataframe to a string for input to Hugging Face model
    df_str = df.to_string(index=False)

    # Define the prompt with an example and clear instructions
    prompt = f"""
    You are an assistant that explains SQL query results in a simple, structured way.

    Here is an example of how to explain a row from an SQL query result:

    Example: 
    Input Row: 1, "Anjali Gupta", "2004-04-15", "Female", "2022-07-01", 8.5, true
    Output: "Anjali Gupta is the student with id 1. She is female and was born on April 15, 2004. Her enrollment date is July 1, 2022, and she has scored a pointer of 8.5 in college."

    Now, explain the following SQL result in a similar format:

    {df_str}

    Please ensure your explanation is concise and to the point. Avoid jargon and unnecessary detail.
    """

    payload = {"inputs": prompt}
    headers = {"Authorization": f"Bearer {HUGGING_FACE_API_KEY}"}

    try:
        response = requests.post(HF_URL, headers=headers, json=payload)
        response.raise_for_status()  # This will raise an error for status codes >= 400
        response_json = response.json()

        # Check if the response is valid and contains the expected result
        if isinstance(response_json, list) and len(response_json) > 0:
            explanation = response_json[0].get("summary_text", "No summary provided.")
            return explanation
        else:
            st.error("Invalid response format from Hugging Face API.")
            return "Unable to generate an explanation."
    except requests.exceptions.RequestException as e:
        st.error(f"Error calling Hugging Face API: {e}")
        return "Error generating explanation."


# Streamlit UI
def app():
    st.title("SQL Query Executor with Gemini Pro Suggestion and Explanation")

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

                # Get explanation for SQL result using Hugging Face model
                explanation = get_sql_response_explanation(result_df)
                st.write("Explanation of the Results:")
                st.write(explanation)
            else:
                st.success("Query executed successfully, no result to display.")
        else:
            st.warning("Please enter a SQL query.")


if __name__ == "__main__":
    app()
