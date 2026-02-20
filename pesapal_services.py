import requests
import os
PESAPAL_CONSUMER_KEY = os.getenv("PESAPAL_KEY", "18ltrhhMAuNdqFWrrql6QKVyd5MGhCUX")
PESAPAL_CONSUMER_SECRET = os.getenv("PESAPAL_SECRET", "fW3mH0UjH2VFjI0HqwQemnH6OOs=")
PESAPAL_BASE_URL = "https://pay.pesapal.com/v3"

class PesapalService:
    def __init__(self):
        self.auth_url = f"{PESAPAL_BASE_URL}/api/Auth/RequestToken"
        self.ipn_url = f"{PESAPAL_BASE_URL}/api/URLSetup/RegisterIPN"
        self.order_url = f"{PESAPAL_BASE_URL}/api/Transactions/SubmitOrderRequest"
        
    def get_access_token(self):
        """Generates a temporary Bearer token required for all Pesapal requests."""
        payload = {
            "consumer_key": PESAPAL_CONSUMER_KEY,
            "consumer_secret": PESAPAL_CONSUMER_SECRET
        }
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        try:
            print("🔑 Requesting Pesapal Token...")
            res = requests.post(self.auth_url, json=payload, headers=headers)
            
            if res.status_code == 200:
                data = res.json()
                token = data.get('token')
                
                # 🛑 FIX: Ensure the token actually exists and isn't empty
                if token:
                    print("✅ Pesapal Token Received Successfully!")
                    return token
                else:
                    print(f"❌ Pesapal API returned 200, but NO TOKEN was found! Raw Response: {res.text}")
                    return None
                    
            print(f"❌ Pesapal Auth Error ({res.status_code}):", res.text)
            return None
        except Exception as e:
            print("❌ Pesapal Auth Request Failed:", e)
            return None
            
    def register_ipn(self, token):
        """Registers the webhook URL where Pesapal will send payment confirmations."""
        payload = {
            "url": "https://httpstat.us/200", 
            "ipn_notification_type": "GET"
        }
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        try:
            print("🌐 Registering Pesapal IPN...")
            res = requests.post(self.ipn_url, json=payload, headers=headers)
            if res.status_code == 200:
                print("✅ Pesapal IPN Registered!")
                return res.json().get('ipn_id')
            print(f"❌ Pesapal IPN Error ({res.status_code}):", res.text)
            return None
        except Exception as e:
             print("❌ Pesapal IPN Request Failed:", e)
             return None

    def submit_order(self, order_id, amount, phone, email, first_name, last_name):
        """Creates the secure payment session and returns the checkout URL."""
        token = self.get_access_token()
        if not token: 
            return {"status": "error", "message": "Pesapal Authentication failed"}
        
        ipn_id = self.register_ipn(token)
        if not ipn_id: 
            # Fallback IPN ID in case registration fails but auth succeeds
            print("⚠️ Warning: IPN Registration failed, attempting to proceed with a fallback ID...")
            ipn_id = "dummy-ipn-id-for-sandbox-123"
        
        payload = {
            "id": order_id,
            "currency": "KES",
            "amount": float(amount),
            "description": f"DAGIV Engineering Order {order_id}",
            "callback_url": f"http://localhost:5173/marketplace", # Where the iframe redirects after payment
            "notification_id": ipn_id,
            "billing_address": {
                "email_address": email,
                "phone_number": phone,
                "country_code": "KE",
                "first_name": first_name,
                "middle_name": "",
                "last_name": last_name,
                "line_1": "",
                "line_2": "",
                "city": "",
                "state": "",
                "postal_code": "",
                "zip_code": ""
            }
        }
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        try:
            print(f"🛒 Submitting Order to Pesapal (ID: {order_id}, Amount: {amount})...")
            res = requests.post(self.order_url, json=payload, headers=headers)
            
            if res.status_code == 200:
                # 🛑 FIX: Print the exact response from Pesapal so we can read the error
                print(f"✅ Pesapal Network 200 OK. RAW RESPONSE: {res.text}")
                return res.json()
            else:
                print(f"❌ Pesapal Submit Order Error ({res.status_code}):", res.text)
                return {"status": "error", "message": res.text}
        except Exception as e:
             print("❌ Pesapal Order Request Failed:", e)
             return {"status": "error", "message": str(e)}