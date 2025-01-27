import streamlit as st
import json
import os
import sqlite3
import pandas as pd
import google.generativeai as genai
from google.ai.generativelanguage_v1beta.types import content
import json
import google.generativeai as genai

with open('config.json', 'r') as f:
    config = json.load(f)

genai.configure(api_key=config['gemini_api_key'])


# --- Model Configuration for Address Matching ---
generation_config_address = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 8192,
    "response_schema": content.Schema(
        type=content.Type.OBJECT,
        enum=[],
        required=[
            "match",
            "original_address",
            "cleaned_address",
            "matched_database_address",
            "confidence",
            "explanation",
        ],
        properties={
            "match": content.Schema(
                type=content.Type.BOOLEAN,
            ),
            "original_address": content.Schema(
                type=content.Type.STRING,
                description="The address inputted."
            ),
            "cleaned_address": content.Schema(
                type=content.Type.STRING,
                description="The cleaned up address that Gemini is using to compare. Empty string if no match"
            ),
             "matched_database_address": content.Schema(
                type=content.Type.STRING,
                 description="The address from the database that was the closest match. Empty string if no match"
            ),
            "confidence": content.Schema(
                type=content.Type.NUMBER,
                description="A score between 0 and 1 indicating the quality of the address match. 1 being a perfect match."
            ),
            "explanation": content.Schema(
                type=content.Type.STRING,
                description="Explains the matching result. For example, whether it was a perfect match, a partial match, or no match."
            ),
        },
    ),
    "response_mime_type": "application/json",
}

model_address = genai.GenerativeModel(
  model_name="gemini-2.0-flash-exp",
  generation_config=generation_config_address,
)

# --- Model Configuration for Summarization ---
generation_config_summary = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 8192,
    "response_mime_type": "text/plain",
}

model_summary = genai.GenerativeModel(
  model_name="gemini-2.0-flash-exp",
  generation_config=generation_config_summary,
)


def process_address(input_address, database_addresses):
    """
    Sends an address to Gemini for comparison and returns the parsed result.
    
    Args:
        input_address (str): The address to check against the database
        database_addresses (str): The addresses to check against

    Returns:
      dict: A dictionary containing the match details or None on error
    """

    chat_session = model_address.start_chat(
      history=[
          {
          "role": "user",
          "parts": [
            f"""Compare the inputted address to the data from the database. 
              Ultimately the street name and street number are important to determine a match. Be case insensitive.
              - If there's an exact match, then be 100% confident
              - If the name of the street is close and only a typo, then that is probably a match
              - If the address has a city or state or zip code, that is ok, you just need to care about street name and number
              - If there is a directional (eg 123 Fake St N) and the inputted address is 123 Fake St, then that is probably a match, though confidence can be lower
              - If the number is something like 123-456 Fake St and the inputted address is 123 Fake St, then it is probably a match, though confidence can be lower
              
              Here is data from the database: {database_addresses}"""
              ],
          }
        ]
    )
    response = chat_session.send_message(input_address)
    try:
        data = json.loads(response.text)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None
    return data

def summarize_violations(df):
    """
    Sends a DataFrame of code violations to Gemini for summarization.

    Args:
      df: (pd.DataFrame): A dataframe containing code violations.
      
    Returns:
      str: The paragraph summary or None on error.
    """
    if df.empty:
       return "No violations to summarize."

    # Construct a detailed prompt with all the violation details
    violation_summaries = []
    for index, row in df.iterrows():
        violation_summary = (
            f"  - Violation Number: {row['violation_number']}\n"
            f"    Violation Type: {row['complaint_type_name']}\n"
            f"    Violation: {row['violation']}\n"
            f"    Open Date: {row['open_date']}\n"
            f"    Violation Date: {row['violation_date']}\n"
            f"    Issued To: {row['issued_to']}\n"
           # Include more if needed
        )
        violation_summaries.append(violation_summary)
    
    all_violation_summaries = "\n".join(violation_summaries)
    
    prompt = f"""
    Here's information about code violations at an address:
    {all_violation_summaries}
    Please summarize the number of violations and provide a description of the violation types in a paragraph. 
    """
    
    response = model_summary.generate_content(prompt) # Pass the prompt into the function
    return response.text

database_name = "arcgis_data.db"
conn = sqlite3.connect(database_name)
prop_query = f"SELECT distinct PropertyAddress FROM rental_registry"
prop_df = pd.read_sql_query(prop_query, conn)
prop_address = prop_df['PropertyAddress'].str.cat(sep=', ')
comp_query = f"SELECT distinct complaint_address FROM code_violations"
comp_df = pd.read_sql_query(comp_query, conn)
complaint_add = comp_df['complaint_address'].str.cat(sep=', ')

# Example Data (replace with your actual data)
prop_add = f"{prop_address}"  # Replace with your actual data
complaint_addresses = f"{complaint_add}"  # Replace with your actual data


def main():
    st.title("Address Matching and Code Violation Summary")

    input_address = st.text_input("Enter an address:")

    if input_address:
        with st.spinner("Processing address..."):
            # Process Address Against Both Sets of Data
            data_prop = process_address(input_address, prop_add)
            data_complaint = process_address(input_address, complaint_addresses)


        st.subheader("Address Matching Results")
        col1, col2 = st.columns(2)


        with col1:
            if data_prop:
                st.write("**Property Address Results:**")
                st.write(f"  Cleaned Address: {data_prop['cleaned_address']}")
                st.write(f"  Confidence: {data_prop['confidence']}")
                st.write(f"  Explanation: {data_prop['explanation']}")
                st.write(f"  Match: {data_prop['match']}")
                st.write(f"  Matched Database Address: {data_prop['matched_database_address']}")
                st.write(f"  Original Address: {data_prop['original_address']}")

        with col2:
            if data_complaint:
                 st.write("**Code Violation Results:**")
                 st.write(f"  Cleaned Address: {data_complaint['cleaned_address']}")
                 st.write(f"  Confidence: {data_complaint['confidence']}")
                 st.write(f"  Explanation: {data_complaint['explanation']}")
                 st.write(f"  Match: {data_complaint['match']}")
                 st.write(f"  Matched Database Address: {data_complaint['matched_database_address']}")
                 st.write(f"  Original Address: {data_complaint['original_address']}")


        # Initialize an empty list to store dataframes
        dfs = []
        matched_addresses = []

        # Query for Prop Address if it exists
        if data_prop and data_prop["matched_database_address"]:
            matched_addresses.append(data_prop["matched_database_address"])
            database_name = "arcgis_data.db"
            conn = sqlite3.connect(database_name)
            query = f"SELECT * FROM code_violations WHERE complaint_address = '{data_prop['matched_database_address']}'"
            df_prop = pd.read_sql_query(query, conn)
            conn.close()
            dfs.append(df_prop)
        
        # Query for Complaint address if it exists
        if data_complaint and data_complaint["matched_database_address"]:
             if data_complaint["matched_database_address"] not in matched_addresses:
                matched_addresses.append(data_complaint["matched_database_address"])
                database_name = "arcgis_data.db"
                conn = sqlite3.connect(database_name)
                query = f"SELECT * FROM code_violations WHERE complaint_address = '{data_complaint['matched_database_address']}'"
                df_complaint = pd.read_sql_query(query, conn)
                conn.close()
                dfs.append(df_complaint)

        # Combine the dataframes and summarize
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            summary_text = summarize_violations(combined_df)

            if summary_text:
                st.subheader("Summary of Code Violations")
                st.write(summary_text)
            else:
                st.write("No summary was generated.")
        else:
           st.write("No matching addresses found to summarize violations.")


if __name__ == "__main__":
    main()