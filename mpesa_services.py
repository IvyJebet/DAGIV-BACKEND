import requests
import base64
from datetime import datetime
import json
import os

# --- M-PESA CONFIGURATION (SANDBOX DEFAULT) ---
# Your actual keys from developer.safaricom.co.ke
CONSUMER_KEY = os.getenv("MPESA_KEY", "M3Gi1qewRHJLR9o3uXxEViJ6TaVqNmAhYtSy0WF6GG7ayaBC") 
CONSUMER_SECRET = os.getenv("MPESA_SECRET", "AhIC9ItGvAv6ft51HlfQmBuawMECJLJN2sPdxAzwTj883G5MSfDzBRSZEekuAGBb") 
PASSKEY = os.getenv("MPESA_PASSKEY", "bfb279f9aa9bdbcf158e97dd71a467cd2e0c893059b10f78e6b72ada1ed2c919")
BUSINESS_SHORTCODE = "174379" # Sandbox Paybill
CALLBACK_URL = "https://your-domain.com/api/payments/mpesa/callback" # Must be HTTPS and public

class MpesaService:
    def __init__(self):
        self.auth_url = "https://sandbox.safaricom.co.ke/oauth/v1/generate?grant_type=client_credentials"
        self.stk_url = "https://sandbox.safaricom.co.ke/mpesa/stkpush/v1/processrequest"

    def get_access_token(self):
        # 1. Validation check
        if CONSUMER_KEY.startswith("YOUR_") or CONSUMER_SECRET.startswith("YOUR_"):
            print("❌ M-Pesa Auth Error: You are using dummy credentials! Please replace CONSUMER_KEY and CONSUMER_SECRET in mpesa_services.py with your real Daraja keys.")
            return None

        try:
            creds = f"{CONSUMER_KEY}:{CONSUMER_SECRET}"
            encoded = base64.b64encode(creds.encode()).decode()
            headers = {
                "Authorization": f"Basic {encoded}",
                "Cache-Control": "no-cache"
            }
            res = requests.get(self.auth_url, headers=headers)
            res.raise_for_status()
            return res.json()['access_token']
        except Exception as e:
            print(f"M-Pesa Auth Error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"M-Pesa Response Details: {e.response.text}")
            return None

    def stk_push(self, phone_number: str, amount: int, order_id: str):
        token = self.get_access_token()
        if not token:
            return {"status": "error", "message": "Failed to authenticate with M-Pesa. Check your Consumer Key & Secret."}

        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        password = base64.b64encode(f"{BUSINESS_SHORTCODE}{PASSKEY}{timestamp}".encode()).decode()

        # Sanitize Phone (Ensure format 2547XXXXXXXX)
        if phone_number.startswith("0"): phone_number = "254" + phone_number[1:]
        elif phone_number.startswith("+"): phone_number = phone_number[1:]

        payload = {
            "BusinessShortCode": BUSINESS_SHORTCODE,
            "Password": password,
            "Timestamp": timestamp,
            "TransactionType": "CustomerPayBillOnline",
            "Amount": amount,
            "PartyA": phone_number,
            "PartyB": BUSINESS_SHORTCODE,
            "PhoneNumber": phone_number,
            "CallBackURL": CALLBACK_URL,
            "AccountReference": order_id, # This links payment to order
            "TransactionDesc": f"Payment for {order_id}"
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        try:
            res = requests.post(self.stk_url, json=payload, headers=headers)
            res_data = res.json()
            if "errorMessage" in res_data:
                print(f"❌ M-Pesa STK Error: {res_data}")
            return res_data
        except Exception as e:
            return {"status": "error", "message": str(e)}