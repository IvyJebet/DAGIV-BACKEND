import requests
import os

# --- PESAPAL V3 CONFIGURATION (SANDBOX) ---
PESAPAL_CONSUMER_KEY = os.getenv("PESAPAL_KEY", "18ltrhhMAuNdqFWrrql6QKVyd5MGhCUX")
PESAPAL_CONSUMER_SECRET = os.getenv("PESAPAL_SECRET", "fW3mH0UjH2VFjI0HqwQemnH6OOs=")
PESAPAL_BASE_URL = "https://cybqa.pesapal.com/pesapalv3"

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
            res = requests.post(self.auth_url, json=payload, headers=headers)
            if res.status_code == 200:
                return res.json().get('token')
            print("Pesapal Auth Error:", res.text)
            return None
        except Exception as e:
            print("Pesapal Request Error:", e)
            return None
            
    def register_ipn(self, token):
        """Registers the webhook URL where Pesapal will send payment confirmations."""
        payload = {
            # Use a dummy URL for local testing, update to your real domain in production
            "url": "https://dagiv-engineering.com/api/webhooks/pesapal", 
            "ipn_notification_type": "GET"
        }
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        res = requests.post(self.ipn_url, json=payload, headers=headers)
        if res.status_code == 200:
            return res.json().get('ipn_id')
        print("Pesapal IPN Error:", res.text)
        return None

    def submit_order(self, order_id, amount, phone, email, first_name, last_name):
        """Creates the secure payment session and returns the checkout URL."""
        token = self.get_access_token()
        if not token: 
            return {"status": "error", "message": "Pesapal Authentication failed"}
        
        ipn_id = self.register_ipn(token)
        if not ipn_id: 
            return {"status": "error", "message": "IPN Registration failed"}
        
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
            res = requests.post(self.order_url, json=payload, headers=headers)
            if res.status_code == 200:
                return res.json()
            else:
                print("Pesapal Submit Order Error:", res.text)
                return {"status": "error", "message": res.text}
        except Exception as e:
            return {"status": "error", "message": str(e)}