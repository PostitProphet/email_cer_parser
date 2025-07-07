import os
import json
import gspread
from flask import Flask, request
import google.generativeai as genai
from dateutil.parser import parse
from google.oauth2.service_account import Credentials

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Market Key Mapping ---
MARKET_KEY = {
    "1020": "Corporate", "1060": "IT&S", "5500": "East Texas", "5501": "East Texas",
    "5504": "East Texas", "5505": "East Texas", "5510": "East Texas", "5515": "East Texas",
    "5520": "East Texas", "5525": "East Texas", "5530": "East Texas", "5535": "East Texas",
    "5540": "East Texas", "5550": "East Texas", "5400": "Idaho", "5404": "Idaho",
    "5405": "Idaho", "5480": "Kansas", "5430": "New Jersey", "5440": "New Jersey",
    "1500": "New Mexico", "5050": "New Mexico", "5055": "New Mexico", "5061": "New Mexico",
    "5070": "New Mexico", "5082": "New Mexico", "5125": "New Mexico",
    "1600": "Oklahoma", "5200": "Oklahoma", "5201": "Oklahoma", "5210": "Oklahoma",
    "5230": "Oklahoma", "5251": "Oklahoma", "5260": "Oklahoma", "5265": "Oklahoma",
    "5270": "Oklahoma", "5275": "Oklahoma", "5280": "Oklahoma", "5300": "West Texas",
    "5301": "West Texas", "5310": "West Texas", "5311": "West Texas", "5316": "West Texas",
    "5320": "West Texas", "5410": "West Texas", "5415": "West Texas"
}

# --- Gemini API Interaction ---
class GeminiProcessor:
    def __init__(self):
        try:
            api_key = os.getenv("GEMINI_API_KEY")
            if not api_key:
                raise ValueError("GEMINI_API_KEY not found in environment variables.")
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel('gemini-1.5-flash-latest')
        except Exception as e:
            print(f"API Configuration Error: {e}")
            raise

    def clean_json_response(self, text):
        match = text[text.find('{'):text.rfind('}')+1]
        return match if match else "{}"

    def extract_data_from_email(self, email_body):
        prompt = """
        Analyze the email content. Extract the following fields:
        - "Forwarded Date": The 'Sent' date from the Tim Huffman email. Format it strictly as YYYY-MM-DD.
        - "Ardent CER#": The Capital Equipment Request number, often following "CER:".
        - "Notes": A concise project description (e.g., "ED Expansion", "OR Tables").
        - "Capital $": A number only, converting 'k' to thousands (e.g., $315k becomes 315000).
        - "ETA/Install": The estimated arrival or installation date/quarter.
        - "Mfg": Based on the notes and content, infer the Manufacturer.
        - "Model": Based on the notes and content, infer the specific Model.
        - "URL": The ERP link, which is a URL usually starting with "http".
        Return a single, minified JSON object. If a field is not found, use an empty string "".
        ---
        Email Content:
        """ + email_body
        try:
            response = self.model.generate_content(prompt)
            return json.loads(self.clean_json_response(response.text))
        except Exception as e:
            raise ValueError(f"Gemini API call or JSON parsing failed: {e}")

# --- Helper Functions ---
def get_market_from_cer(cer_number):
    if isinstance(cer_number, str) and len(cer_number) >= 4:
        return MARKET_KEY.get(cer_number[:4], "No Match")
    return "No Match"

def format_date_string(date_str):
    if not date_str:
        return ""
    try:
        dt_object = parse(date_str.strip(), fuzzy=True)
        return f"{dt_object.month}/{dt_object.day}/{dt_object.year}"
    except (ValueError, TypeError):
        return date_str

# --- Google Sheets Integration ---
def get_gspread_client():
    """Authenticates with Google Sheets and returns a client object."""
    creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable not set.")
    
    creds_dict = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file"
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client

def update_google_sheet(data):
    """Appends a new row to the appropriate market tab in Google Sheets."""
    try:
        print("-> Attempting to connect to Google Sheets...")
        client = get_gspread_client()
        print("-> Connection successful.")

        sheet_name = os.getenv("GOOGLE_SHEET_NAME", "CER_Report")
        print(f"-> Opening spreadsheet: '{sheet_name}'")
        spreadsheet = client.open(sheet_name)
        print("-> Spreadsheet opened successfully.")

        market = data.get("Market", "Unknown")
        print(f"-> Market determined as: '{market}'")

        # Get or create the worksheet for the market
        try:
            worksheet = spreadsheet.worksheet(market)
            print(f"-> Found existing worksheet tab: '{market}'")
        except gspread.WorksheetNotFound:
            print(f"-> Worksheet tab not found. Creating new tab: '{market}'")
            worksheet = spreadsheet.add_worksheet(title=market, rows="100", cols="20")
            header = ["Forwarded Date", "Market", "Ardent CER#", "Capital $", "Notes", "Mfg", "Model", "ETA/Install", "URL", "Source Email"]
            worksheet.append_row(header)
            print("-> Header row added to new worksheet.")

        # Prepare and append the data row
        row_to_add = [
            data.get("Forwarded Date", ""),
            market,
            data.get("Ardent CER#", ""),
            data.get("Capital $", ""),
            data.get("Notes", ""),
            data.get("Mfg", ""),
            data.get("Model", ""),
            data.get("ETA/Install", ""),
            data.get("URL", ""),
            data.get("Source Email", "")
        ]
        print(f"-> Preparing to add row: {row_to_add}")
        worksheet.append_row(row_to_add)
        print(f"✅ Successfully wrote data to tab: '{market}'")

    except Exception as e:
        # This will now print the exact type and message of the error
        print(f"❌ ERROR: Failed to update Google Sheet. Reason: {type(e).__name__} - {e}")

# --- Main Webhook Endpoint ---
@app.route("/webhook", methods=['POST'])
def handle_email():
    """This endpoint receives email data from SendGrid."""
    print("📧 Webhook received a new email.")
    
    # SendGrid sends email data as multipart/form-data
    email_body = request.form.get('text')
    email_from = request.form.get('from')
    
    if not email_body:
        return "Failed: No email body content.", 400

    try:
        gemini = GeminiProcessor()
        extracted_data = gemini.extract_data_from_email(email_body)
        
        cer_number = str(extracted_data.get("Ardent CER#", ""))
        if not cer_number:
            print("❌ Failed: Could not extract CER# from email.")
            return "Failed: Could not extract CER#.", 400

        extracted_data["Forwarded Date"] = format_date_string(extracted_data.get("Forwarded Date"))
        extracted_data["Market"] = get_market_from_cer(cer_number)
        extracted_data["Source Email"] = email_from
        
        # Update the shared Google Sheet
        update_google_sheet(extracted_data)
        
        return "✅ Success: Email processed and sheet updated.", 200

    except Exception as e:
        print(f"CRITICAL ERROR during email processing: {e}")
        return f"Internal server error: {e}", 500

# Health check endpoint
@app.route("/")
def index():
    return "CER Parser is running.", 200

if __name__ == "__main__":
    # This block is for local testing. Railway uses the Procfile to run the app.
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)