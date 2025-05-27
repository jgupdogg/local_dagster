# solana_pipeline/resources/google_sheets.py
import os
from typing import List, Dict, Any
import pandas as pd
import numpy as np
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dagster import resource, Field, StringSource

@resource(
    config_schema={
        "service_account_key_path": Field(
            StringSource,
            description="Path to Google Service Account credentials JSON file",
            is_required=False,
        ),
        "spreadsheet_id": Field(
            StringSource,
            description="Google Spreadsheet ID",
            is_required=False,
        ),
    }
)
def google_sheets_resource(context):
    """Resource for interacting with Google Sheets."""
    # Get config from parameters or environment variables
    key_path = context.resource_config.get("service_account_key_path") or os.getenv("GOOGLE_SHEETS_KEY_PATH")
    spreadsheet_id = context.resource_config.get("spreadsheet_id") or os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    
    if not key_path or not spreadsheet_id:
        raise ValueError("Google Sheets credentials or spreadsheet ID not provided")
    
    class GoogleSheetsClient:
        def __init__(self, key_path, spreadsheet_id):
            self.key_path = key_path
            self.spreadsheet_id = spreadsheet_id
            
        def get_service(self):
            """Create and return an authenticated Google Sheets service."""
            creds = Credentials.from_service_account_file(
                self.key_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            return build('sheets', 'v4', credentials=creds)
            
        def update_worksheet(self, df: pd.DataFrame, worksheet_name: str) -> Dict[str, Any]:
            """Update a worksheet with DataFrame data."""
            # Prepare DataFrame
            df = df.replace({np.nan: '', None: ''})
            
            # Convert boolean values to strings
            for col in df.select_dtypes(include=['bool']).columns:
                df[col] = df[col].apply(lambda x: 'Yes' if x == True else 'No' if x == False else '')
            
            # Convert all datetime columns to strings
            for col in df.select_dtypes(include=['datetime']).columns:
                df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if x and not pd.isna(x) else '')
            
            # Define the range to write to (starting at A1)
            range_label = f"{worksheet_name}!A1"
            
            # Prepare the data for writing
            headers = df.columns.tolist()
            data = df.values.tolist()
            
            # Sanitize the data
            sanitized_data = []
            for row in data:
                sanitized_data.append([str(cell) if cell is not None and cell != '' else '' for cell in row])
            
            values = [headers] + sanitized_data
            body = {'values': values}
            
            # Write to the sheet
            service = self.get_service()
            result = service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_label,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            return {
                'spreadsheet_id': self.spreadsheet_id,
                'worksheet_name': worksheet_name,
                'updated_range': result.get('updatedRange'),
                'updated_rows': result.get('updatedRows', 0),
                'updated_columns': result.get('updatedColumns', 0)
            }
    
    return GoogleSheetsClient(key_path, spreadsheet_id)