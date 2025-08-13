from notion_client import Client
import datetime

# Connect to Notion
notion = Client(auth="YOUR_INTEGRATION_TOKEN")

database_id = "YOUR_DATABASE_ID"

# Example row creation
notion.pages.create(
    parent={"database_id": database_id},
    properties={
        "Game Date": {"date": {"start": "2025-08-13"}},
        "Away Team": {"title": [{"text": {"content": "AAA"}}]},
        "Home Team": {"rich_text": [{"text": {"content": "BBB"}}]},
        "Prediction - Moneyline": {"select": {"name": "BBB ML"}},
        "Confidence (ML)": {"number": 7},
        # ... add the rest of your fields here
    }
)
