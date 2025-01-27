#import libraries
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
from urllib.parse import quote
import sqlite3
import logging

DATABASE_NAME = "arcgis_data.db"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def build_query_params(base_date_field, start_date, end_date, f="pjson"):
    """Builds query parameters with a date range"""
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    params = {
        "where": f"{base_date_field} BETWEEN '{start_date_str}' AND '{end_date_str}'",
        "objectIds": "",
        "geometry": "",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "",
        "spatialRel": "esriSpatialRelIntersects",
        "resultType": "none",
        "distance": "0.0",
        "units": "esriSRUnit_Meter",
        "relationParam": "",
        "returnGeodetic": "false",
        "outFields": "*",
        "returnGeometry": "true",
        "featureEncoding": "esriDefault",
        "multipatchOption": "xyFootprint",
        "maxAllowableOffset": "",
        "geometryPrecision": "",
        "outSR": "",
        "defaultSR": "",
        "datumTransformation": "",
        "applyVCSProjection": "false",
        "returnIdsOnly": "false",
        "returnUniqueIdsOnly": "false",
        "returnCountOnly": "false",
        "returnExtentOnly": "false",
        "returnQueryGeometry": "false",
        "returnDistinctValues": "false",
        "cacheHint": "false",
        "collation": "",
        "orderByFields": "",
        "groupByFieldsForStatistics": "",
        "outStatistics": "",
        "having": "",
        "resultOffset": "",
        "resultRecordCount": "",
        "returnZ": "false",
        "returnM": "false",
        "returnTrueCurves": "false",
        "returnExceededLimitFeatures": "true",
        "quantizationParameters": "",
        "sqlFormat": "none",
        "f": f,
        "token": ""
    }
    return params # Do not url encode


def fetch_data(base_url, params, part_type=None, paginate=False, max_records=None, table_name = None):
    """Fetches data from an ArcGIS FeatureServer URL, handles pagination, and returns a DataFrame."""
    all_records = []
    offset = 0
    record_limit = 1000
    
    try:
      while True:
         if paginate:
            params["resultOffset"] = offset
            params["resultRecordCount"] = record_limit

         full_url = base_url + "&".join([f"{key}={value}" for key,value in params.items()])
         response = requests.get(full_url)
         response.raise_for_status()
         data = response.json()
        
         if not data or 'features' not in data or not data['features']:
             if data and 'features' in data and not data['features']:
                 print(f"No more records found for {table_name or part_type}")
                 break
             else:
                print(f"Invalid response structure for {table_name or part_type}")
                break

         features = data['features']
         if not features:
            print(f"No more records found for {table_name or part_type}")
            break

          # Transform and flatten the data
         records = []
         for feature in features:
             properties = feature['attributes']
             geometry = feature.get('geometry', {})
             if geometry:
                 coordinates = geometry.get('coordinates', [])
                 if coordinates:
                    properties['longitude'] = coordinates[0]
                    properties['latitude'] = coordinates[1]
             if part_type:
                properties['part'] = part_type # Add part identifier
             records.append(properties)

         all_records.extend(records)
      
         if max_records and len(all_records) >= max_records:
            all_records = all_records[:max_records] # Only truncate if max_records is specified
            print(f"Max records of {max_records} reached, stopping pagination.")
            break

         if not paginate: # Only increment the offset if pagination is enabled.
            break
         offset += record_limit
    
    except requests.exceptions.RequestException as e:
       logging.error(f"Error during request for {table_name or part_type}: {e}")
       return None
    except json.JSONDecodeError as e:
      logging.error(f"Error decoding JSON for {table_name or part_type}: {e}")
      return None
    except KeyError as e:
       logging.error(f"Error accessing a key in JSON for {table_name or part_type}: {e}")
       return None
    except TypeError as e:
       logging.error(f"Error with data type for {table_name or part_type}: {e}")
       return None

    if not all_records:
       print(f"No records found for {table_name or part_type}")
       return None
    
    if part_type:
      print(f"Successfully fetched data for: {part_type} endpoint")
    else:
      print(f"Successfully fetched data for: {table_name} endpoint")
   
    df = pd.DataFrame(all_records)
   #Convert from ms to date
    date_fields = [col for col in df.columns if 'date' in col.lower()]
    for field in date_fields:
      if df[field].dtype == 'int64':
        df[field] = pd.to_datetime(df[field], unit='ms')

    # Specific date conversion for cityline_requests
    if table_name == 'cityline_requests':
      date_fields_cityline = ["Created_at_local", "Acknowledged_at_local", "Closed_at_local"]
      for field in date_fields_cityline:
        if field in df.columns:
           df[field] = pd.to_datetime(df[field], format='%m/%d/%Y - %I:%M%p', errors='coerce')
    return df



def load_dataframe_to_sqlite(df, table_name, conn, if_exists='replace'):
    """Loads a DataFrame into a SQLite database."""
    try:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        print(f"DataFrame successfully loaded into {table_name} table.")
    except Exception as e:
        logging.error(f"Error loading DataFrame into {table_name}: {e}")


def get_max_date_from_sqlite(conn, table_name, date_field):
    """Retrieves the maximum date from a SQLite table."""
    try:
        query = f"SELECT MAX({date_field}) FROM {table_name}"
        result = pd.read_sql_query(query, conn)
        if result.iloc[0, 0]:  # Check if there is a result
            # Attempt to convert the string to datetime if not None
            max_date_str = result.iloc[0, 0]
            
            if max_date_str:
                return pd.to_datetime(max_date_str)  # Convert string to datetime
            else:
              return None
        else:
          return None  # return None if the table does not exist or the table is empty
    except Exception as e:
        logging.error(f"Error retrieving max date from {table_name}: {e}")
        return None

def get_max_id_from_sqlite(conn, table_name, id_field):
    """Retrieves the maximum ID from a SQLite table."""
    try:
        query = f"SELECT MAX({id_field}) FROM {table_name}"
        result = pd.read_sql_query(query, conn)
        if result.iloc[0, 0]:  # Check if there is a result
            return result.iloc[0, 0]
        else:
          return 0  # return 0 if the table does not exist or the table is empty
    except Exception as e:
        logging.error(f"Error retrieving max ID from {table_name}: {e}")
        return 0 # Return 0 so we can load everything for first time


def fetch_and_load_data(key, config, conn):
    """Fetches data, loads into the database, and updates incrementally."""
    table_name = key
    date_field = config.get("date_field")
    id_field = config.get("id_field")
    
    if date_field: # Check if date_field is specified
        max_date_in_db = get_max_date_from_sqlite(conn, table_name, date_field)
        today = datetime.now()
        if max_date_in_db is None:
           print(f"No existing data found for {key}. Fetching all data.")
           start_date = today - timedelta(days=config["days_ago"])
           params = build_query_params(date_field, start_date, today)
           df = fetch_data(config["base_url"], params, config.get("part_type"), config.get("paginate"), config.get("max_records"), table_name)
           if df is not None:
               load_dataframe_to_sqlite(df, table_name, conn, if_exists='replace')
           return # Exit function as there is no data to increment
        else:
            print(f"Existing data found for {key}. Fetching incremental data since: {max_date_in_db}")
            start_date = max_date_in_db
            params = build_query_params(date_field, start_date, today)
            df = fetch_data(config["base_url"], params, config.get("part_type"), config.get("paginate"), config.get("max_records"), table_name)

            if df is not None and not df.empty:
                load_dataframe_to_sqlite(df, table_name, conn, if_exists='append')
                print(f"Incremental data loaded to table: {table_name}")
            else:
               print(f"No new records to load for {table_name}")
    elif id_field:
         max_id_in_db = get_max_id_from_sqlite(conn, table_name, id_field)
         print(f"Max ID in DB for {table_name}: {max_id_in_db}")
         params = {
            "where": f"{id_field} > {max_id_in_db if max_id_in_db else 0 }",
            "objectIds": "",
            "geometry": "",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "",
            "spatialRel": "esriSpatialRelIntersects",
            "resultType": "none",
            "distance": "0.0",
            "units": "esriSRUnit_Meter",
            "relationParam": "",
            "returnGeodetic": "false",
            "outFields": "*",
            "returnGeometry": "true",
            "featureEncoding": "esriDefault",
            "multipatchOption": "xyFootprint",
            "maxAllowableOffset": "",
            "geometryPrecision": "",
            "outSR": "",
            "defaultSR": "",
            "datumTransformation": "",
            "applyVCSProjection": "false",
            "returnIdsOnly": "false",
            "returnUniqueIdsOnly": "false",
            "returnCountOnly": "false",
            "returnExtentOnly": "false",
            "returnQueryGeometry": "false",
            "returnDistinctValues": "false",
            "cacheHint": "false",
            "collation": "",
            "orderByFields": "",
            "groupByFieldsForStatistics": "",
            "outStatistics": "",
            "having": "",
            "resultOffset": "",
            "resultRecordCount": "",
            "returnZ": "false",
            "returnM": "false",
            "returnTrueCurves": "false",
            "returnExceededLimitFeatures": "true",
            "quantizationParameters": "",
            "sqlFormat": "none",
            "f": "pjson",
            "token": ""
        }

         df = fetch_data(config["base_url"],params, config.get("part_type"), config.get("paginate"), config.get("max_records"), table_name)
         if df is not None and not df.empty:
           load_dataframe_to_sqlite(df, table_name, conn, if_exists='append')
           print(f"Incremental data loaded to table: {table_name}")
         else:
          print(f"No new records to load for {table_name}")
    else:
        print(f"Neither date_field or id_field is specified for {table_name}. Please review the config")

if __name__ == "__main__":
    endpoints = {
        "permits": {
            "base_url": "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Permit_Requests/FeatureServer/0/query?",
            "date_field": "Issue_Date",
            "days_ago": 365,
            "paginate": True,
            "part_type": None
        },
        "code_violations": {
            "base_url": "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Code_Violations_V2/FeatureServer/0/query?",
            "date_field": "violation_date",
            "days_ago": 365,
            "paginate": True,
            "part_type": None
        },
         "rental_registry":{
             "base_url": "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Syracuse_Rental_Registry/FeatureServer/0/query?",
            "date_field": "RR_app_received",
             "days_ago": 365,
              "paginate": True,
              "part_type": None
         },
         "parking_violations":{
             "base_url": "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Parking_Violations_2023_Present/FeatureServer/0/query?",
             "date_field": "issued_date",
             "days_ago": 365,
             "paginate": True,
             "part_type": None
         },
        "part_1_crimes": {
            "base_url": "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Crime_Data_2024_Part_1_Offenses_With_Lat_and_Long_Info/FeatureServer/0/query?",
            "date_field": "DATEEND",
             "days_ago": 365,
              "paginate": True,
            "part_type": "Part 1"
        },
        "part_2_crimes": {
            "base_url": "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/Crime_Data_2024_Part_2_Offenses_With_Lat_and_Long_Info/FeatureServer/0/query?",
            "date_field": "DATEEND",
            "days_ago": 365,
            "paginate": True,
             "part_type": "Part 2"
        },
        "cityline_requests": {
          "base_url": "https://services6.arcgis.com/bdPqSfflsdgFRVVM/arcgis/rest/services/SYRCityline_Requests_2021_Present/FeatureServer/0/query?",
          "id_field": "ObjectId",
            "paginate":True,
           "part_type": None
        }
    }

    #Initialize SQLite connection
    conn = sqlite3.connect(DATABASE_NAME)

    for key, config in endpoints.items():
        print(f"Processing: {key}")
        fetch_and_load_data(key, config, conn)
    
    conn.close()
    print("Completed successfully.")
