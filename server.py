import psycopg2
from psycopg2.extras import RealDictCursor
import hashlib
import smtplib
import traceback
import ssl
import uuid
import random
import string
import json
import os
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from fastapi import FastAPI, HTTPException, Depends, Request, status, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from jose import JWTError, jwt
import uvicorn
from google import genai
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from typing import Optional
import logging
import requests
from celery import Celery
from dotenv import load_dotenv
import os
from mpesa_services import MpesaService
from pesapal_services import PesapalService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dagiv_celery")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_EMAIL = "dagivengineering@gmail.com" 
SENDER_EMAIL = "dagivengineering@gmail.com"
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD") 
SECRET_KEY = "DAGIV_SUPER_SECRET_KEY_CHANGE_THIS_IN_PROD"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_CONF_URL = "https://accounts.google.com/.well-known/openid-configuration"
SMS_API_KEY = os.getenv("SMS_API_KEY")
SMS_USERNAME = os.getenv("SMS_USERNAME")
SMS_SENDER_ID = os.getenv("SMS_SENDER_ID")

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
celery_app = Celery("dagiv_tasks", broker=RABBITMQ_URL)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Africa/Nairobi",
    enable_utc=True,
)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") 
try:
    ai_client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print(f"⚠️ AI Client Init Error: {e}")
    ai_client = None

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")
class UserRegister(BaseModel):
    email: str
    phone: str
    password: str
    role: str = "BUYER"
    username: str
    business_name: Optional[str] = None

class OTPVerifyRequest(BaseModel):
    email: str
    otp_code: str

class GoogleAuthRequest(BaseModel):
    token: str

class LoginRequest(BaseModel):
    identifier: str 
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str
    user_id: str
    role: str
    username: str

class AIChatRequest(BaseModel):
    prompt: str

class InspectionRequest(BaseModel):
    machineType: str
    location: str
    contactPerson: str
    phone: str
    date: str

class OperatorLog(BaseModel):
    machineId: str
    operatorName: str
    date: str
    startTime: str
    endTime: str
    startOdometer: float
    endOdometer: float
    fuelAddedLiters: float
    location: str
    notes: str
    checklist: dict

class ConsultationRequest(BaseModel):
    name: str
    phone: str
    type: str
    details: str

class ServiceRequest(BaseModel):
    name: str
    phone: str
    email: str
    serviceType: str
    details: str
    company: str = "N/A"

class LeaseRequestKP(BaseModel):
    machineName: str
    machineId: str
    customerName: str
    phone: str
    duration: str

class MarketListing(BaseModel):
    listingType: str  
    sellerName: str
    phone: str
    location: str
    category: str
    subCategory: str
    brand: str
    model: str
    price: float
    currency: str
    specs: dict

class SellerCheck(BaseModel):
    phone: str

class SellerRegistration(BaseModel):
    name: str
    phone: str
    location: str
    email: str
    businessType: str
    regNumber: str
    doc_primary: str  
    doc_secondary: str
    doc_proof: str 
    password: str 

class OrderCreate(BaseModel):
    listing_id: str
    quantity: int
    payment_method: str 
    duration: int = 1 
    shipping_cost: float = 0.0

class OrderStatusUpdate(BaseModel):
    status: str 

class MpesaPaymentRequest(BaseModel):
    order_id: str
    phone_number: str
    amount: float

class CartAddRequest(BaseModel):
    listing_id: str
    quantity: int = 1

class CheckoutProcessRequest(BaseModel):
    payment_method: str 
    mpesa_phone: Optional[str] = None
    shipping_details: dict

class EscrowReleaseRequest(BaseModel):
    order_id: str

class Destination(BaseModel):
    address: str
    city: str

class CustomerContact(BaseModel):
    phone: str
    email: str

class ShipmentCreate(BaseModel):
    order_id: str
    destination: Destination
    customer_contact: CustomerContact

class ShipmentUpdate(BaseModel):
    status: str
    location: str
    notes: str

# --- 3. NOTIFICATION SYSTEM (SYNC & ASYNC) ---

def send_email_alert(category: str, details: str):
    """Legacy Sync Email Alert (Kept for backwards compatibility)"""
    if "REPLACE_THIS" in SENDER_PASSWORD:
        return
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = f"🔔 DAGIV ALERT: New {category}"
    html_content = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <div style="border: 1px solid #ddd; padding: 20px; border-radius: 8px; max-width: 600px;">
          <h2 style="color: #eab308; border-bottom: 2px solid #eab308; padding-bottom: 10px;">
            New {category} Received
          </h2>
          <p><strong>Time:</strong> {timestamp}</p>
          <div style="background-color: #f9fafb; padding: 15px; border-radius: 5px; border-left: 4px solid #333;">
            <pre style="font-family: monospace; white-space: pre-wrap;">{details}</pre>
          </div>
        </div>
      </body>
    </html>
    """
    try:
        msg = MIMEMultipart()
        msg['From'] = f"DAGIV System <{SENDER_EMAIL}>"
        msg['To'] = ADMIN_EMAIL
        msg['Subject'] = subject
        msg.attach(MIMEText(html_content, 'html'))
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, ADMIN_EMAIL, msg.as_string())
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

def send_otp_email(email: str, otp_code: str):
    """Sync OTP Email Sender"""
    subject = "DAGIV ENGINEERING - Your Verification Code"
    html_content = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <div style="border: 1px solid #ddd; padding: 20px; border-radius: 8px; max-width: 600px;">
          <h2 style="color: #eab308; border-bottom: 2px solid #eab308; padding-bottom: 10px;">
            Account Verification
          </h2>
          <p>Your OTP verification code is:</p>
          <h1 style="font-size: 32px; letter-spacing: 5px; color: #1e293b;">{otp_code}</h1>
          <p>This code will expire in 10 minutes.</p>
        </div>
      </body>
    </html>
    """
    try:
        msg = MIMEMultipart()
        msg['From'] = f"DAGIV System <{SENDER_EMAIL}>"
        msg['To'] = email
        msg['Subject'] = subject
        msg.attach(MIMEText(html_content, 'html'))
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, email, msg.as_string())
    except Exception as e:
        print(f"❌ Failed to send OTP email: {e}")

# CELERY ASYNC TASKS
@celery_app.task(bind=True, max_retries=3, name="server.send_email_async")
def send_email_async(self, to_email: str, subject: str, body: str):
    # Ensure variables are loaded in the worker context
    load_dotenv()
    s_email = os.getenv("SENDER_EMAIL") or SENDER_EMAIL
    s_pass = os.getenv("SENDER_PASSWORD") or SENDER_PASSWORD
    
    logger.info(f"📧 Attempting to send email to {to_email} via {s_email}...")

    if not s_pass or "REPLACE_THIS" in s_pass:
        logger.error("❌ SENDER_PASSWORD is not set or is still the default!")
        return "Failed: Missing Credentials"

    try:
        msg = MIMEMultipart()
        msg['From'] = f"DAGIV Engineering <{s_email}>"
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            logger.info("🔑 Logging into SMTP...")
            server.login(s_email, s_pass)
            server.sendmail(s_email, to_email, msg.as_string())
            
        logger.info(f"✅ Email successfully sent to {to_email}")
        return f"Sent to {to_email}"

    except smtplib.SMTPAuthenticationError:
        logger.error("❌ SMTP Authentication Failed. Check App Password.")
        raise self.retry(countdown=300) 
    except Exception as exc:
        logger.error(f"❌ smtplib error: {str(exc)}")
        logger.error(traceback.format_exc())
        raise self.retry(exc=exc, countdown=60)

@celery_app.task(bind=True, max_retries=3, name="server.send_sms_async")
def send_sms_async(self, phone_number: str, message: str):
    try:
        payload = {
            "username": SMS_USERNAME,
            "to": phone_number,
            "message": message,
            "from": SMS_SENDER_ID
        }
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "apiKey": SMS_API_KEY
        }
        response = requests.post(
            "https://api.africastalking.com/version1/messaging",
            data=payload,
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        return f"SMS successfully sent to {phone_number}"
    except Exception as exc:
        raise self.retry(exc=exc, countdown=60)

# 🛠️ FIX: Added explicit name="server.update_shipment_status_async"
@celery_app.task(bind=True, max_retries=3, name="server.update_shipment_status_async")
def update_shipment_status_async(self, tracking_number: str, new_status: str, location: str, notes: str):
    try:
        shipment = LOGISTICS_DB.get(tracking_number)
        if not shipment: return f"Error: Shipment {tracking_number} not found."
        
        shipment["status"] = new_status
        shipment["updates"].append({
            "status": new_status,
            "timestamp": datetime.utcnow().isoformat(),
            "location": location,
            "notes": notes
        })
        customer = shipment.get("customer_contact", {})
        sms_message = f"DAGIV Update: Shipment {tracking_number} is now {new_status.replace('_', ' ')}. Location: {location}."
        if customer.get("phone"):
            send_sms_async.delay(customer["phone"], sms_message)
        return f"Successfully updated {tracking_number} to {new_status}"
    except Exception as exc:
        raise self.retry(exc=exc, countdown=30)

# --- 4. HELPERS ---
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def hash_text(s: str) -> str:
    return hashlib.sha256((s or "").encode()).hexdigest()

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta if expires_delta else timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        role: str = payload.get("role")
        
        if user_id is None:
            raise credentials_exception
            
        return {"user_id": user_id, "role": role}
        
    except JWTError as e:
        raise credentials_exception

def require_role(required_role: str):
    def role_checker(user = Depends(get_current_user)):
        if user["role"] != required_role and user["role"] != "ADMIN":
            raise HTTPException(status_code=403, detail=f"Access denied. Requires {required_role}")
        return user
    return role_checker

# --- 5. STARTUP & MIGRATIONS ---

@app.on_event("startup")
def startup_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 0. Robust Users Table (from Phase 1)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT UNIQUE,
            email TEXT UNIQUE,
            phone TEXT UNIQUE,
            password_hash TEXT,
            role TEXT,
            is_verified BOOLEAN DEFAULT FALSE,
            google_id TEXT UNIQUE,
            otp_code TEXT,
            otp_expires_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 1. Orders Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            buyer_id TEXT,
            seller_phone TEXT,
            listing_id TEXT,
            total_amount REAL, 
            currency TEXT,
            status TEXT DEFAULT 'PENDING_PAYMENT',
            payment_method TEXT,
            shipping_details JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 1.5 Order Line Items Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS order_line_items (
            id SERIAL PRIMARY KEY,
            order_id TEXT,
            listing_id TEXT,
            quantity INT,
            unit_price REAL,
            seller_phone TEXT
        )
    """)
    
    # 2. Transactions Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            order_id TEXT,
            amount REAL,
            type TEXT, 
            status TEXT DEFAULT 'PENDING',
            checkout_request_id TEXT,  
            mpesa_receipt TEXT,        
            phone_number TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 3. Cart Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cart_items (
            id SERIAL PRIMARY KEY,
            user_id TEXT,
            listing_id TEXT,
            quantity INT DEFAULT 1,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, listing_id)
        )
    """)

    # 4. Logistics Tracking Table (from Phase 5)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logistics_tracking (
            id SERIAL PRIMARY KEY,
            order_id TEXT UNIQUE,
            provider TEXT,
            tracking_number TEXT,
            status TEXT DEFAULT 'PENDING',
            current_location TEXT,
            estimated_delivery TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 5. Migrations (Self-Healing)
    try:
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS listing_id TEXT")
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS seller_phone TEXT")
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_amount REAL") 
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS currency TEXT")
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_method TEXT")
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS shipping_details JSONB")
        cursor.execute("ALTER TABLE order_line_items ADD COLUMN IF NOT EXISTS unit_price REAL")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS type TEXT")          
        cursor.execute("ALTER TABLE transactions ALTER COLUMN type TYPE TEXT USING type::text")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING'") 
        cursor.execute("ALTER TABLE transactions ALTER COLUMN status TYPE TEXT USING status::text")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS checkout_request_id TEXT")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS mpesa_receipt TEXT")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS phone_number TEXT")
        
        # Add new Auth columns if table existed before Phase 1
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS google_id TEXT UNIQUE")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS otp_code TEXT")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS otp_expires_at TIMESTAMP")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_verified BOOLEAN DEFAULT FALSE")
        cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT UNIQUE")
        conn.commit()
    except Exception as e:
        print(f"Migration Info: {e}")
        conn.rollback()
        
    conn.commit()
    conn.close()

@app.get("/")
def read_root():
    return {"message": "DAGIV API (Secured) is Online"}

# --- 6. NEW AUTHENTICATION ROUTES ---

@app.post("/api/auth/register")
def register_user(user: UserRegister): 
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # 1. Validation - Strictly restrict to Buyers & Staff (Reject Sellers)
        if user.role == 'SELLER':
             raise HTTPException(status_code=400, detail="Sellers must use the dedicated Seller Registration portal.")
        elif user.role not in ['BUYER', 'COURIER', 'OPERATOR', 'MECHANIC']:
             raise HTTPException(status_code=400, detail=f"Invalid role selected: {user.role}")
             
        cursor.execute("SELECT id, is_verified FROM users WHERE email = %s OR phone = %s OR username = %s", (user.email, user.phone, user.username))
        existing_user = cursor.fetchone()

        if existing_user:
            if existing_user['is_verified']:
                raise HTTPException(status_code=400, detail="Email, Phone, or Username already registered and verified.")
            else:
                new_user_id = existing_user['id']
                hashed_pw = hash_text(user.password)
                
                # 🛠️ FIX: Added 'email = %s' to ensure the DB syncs with the frontend's cleaned email
                cursor.execute("""
                    UPDATE users 
                    SET password_hash = %s, phone = %s, username = %s, email = %s 
                    WHERE id = %s
                """, (hashed_pw, user.phone, user.username, user.email, new_user_id))
        else:
            new_user_id = str(uuid.uuid4())
            hashed_pw = hash_text(user.password)
            cursor.execute("""
                INSERT INTO users (id, email, phone, password_hash, role, is_verified, username)
                VALUES (%s, %s, %s, %s, %s, FALSE, %s)
            """, (new_user_id, user.email, user.phone, hashed_pw, user.role, user.username))
            
        # 2. Initialize Dependencies
        cursor.execute("""
            INSERT INTO wallets (user_id, balance_available, balance_pending, currency)
            VALUES (%s, 0.00, 0.00, 'KES')
            ON CONFLICT (user_id) DO NOTHING
        """, (new_user_id,))
        
        # Note: Seller profile creation block was intentionally removed.
        
        # 3. OTP Generation & Storage (Saving directly to users table)
        cursor.execute("DELETE FROM otps WHERE user_id = %s", (new_user_id,)) # Legacy cleanup
        
        otp_code = str(random.randint(100000, 999999))
        expires_at = datetime.utcnow() + timedelta(minutes=10)
        
        cursor.execute("""
            UPDATE users 
            SET otp_code = %s, otp_expires_at = %s 
            WHERE id = %s
        """, (otp_code, expires_at, new_user_id))
        
        # --- 🛑 CRITICAL FIX: Commit DB Transaction First ---
        conn.commit()
        
        # 4. Asynchronous Task Dispatch
        email_body = f"""
        <div style="font-family: sans-serif; max-width: 600px; border: 1px solid #e2e8f0; padding: 20px; border-radius: 12px;">
            <h2 style="color: #0f172a;">Verify Your DAGIV Account</h2>
            <p style="color: #64748b;">Welcome to DAGIV Engineering. Use the code below to complete your registration:</p>
            <div style="background: #f8fafc; padding: 20px; text-align: center; border-radius: 8px; margin: 20px 0;">
                <span style="font-size: 32px; font-weight: 900; letter-spacing: 5px; color: #eab308;">{otp_code}</span>
            </div>
            <p style="font-size: 12px; color: #94a3b8;">This code expires in 10 minutes.</p>
        </div>
        """
        
        try:
            logger.info(f"Queuing OTP email task for {user.email}")
            send_email_async.delay(user.email, "Verify Your DAGIV Account", email_body)
        except Exception as celery_err:
            logger.error(f"Celery Warning: Could not dispatch task -> {celery_err}")
            print(f"\n[DEBUG] CELERY UNAVAILABLE - OTP FOR {user.email}: {otp_code}\n")

        return {"requiresOtp": True, "user_id": new_user_id}

    except HTTPException as http_exc:
        conn.rollback()
        raise http_exc
    except Exception as e:
        conn.rollback()
        logger.error(f"Server Error during registration: {e}")
        raise HTTPException(status_code=500, detail="Database constraints failed. Ensure unique email/username.")
    finally:
        conn.close()

@app.post("/api/auth/verify-otp", response_model=Token)
def verify_otp(req: OTPVerifyRequest):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT id, role, username, otp_code, otp_expires_at FROM users WHERE email = %s", (req.email,))
        user = cursor.fetchone()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        if user['otp_code'] != req.otp_code:
            raise HTTPException(status_code=400, detail="Invalid OTP code")
        if user['otp_expires_at'] < datetime.utcnow():
            raise HTTPException(status_code=400, detail="OTP has expired")
            
        cursor.execute("UPDATE users SET is_verified = TRUE, otp_code = NULL, otp_expires_at = NULL WHERE id = %s", (user['id'],))
        conn.commit()
        
        access_token = create_access_token(data={"sub": str(user['id']), "role": user['role']})
        return {
            "access_token": access_token, 
            "token_type": "bearer", 
            "user_id": str(user['id']),
            "role": user['role'],
            "username": user['username']
        }
    finally:
        conn.close()

@app.post("/api/auth/google", response_model=Token)
def google_auth(req: GoogleAuthRequest):
    try:
        idinfo = id_token.verify_oauth2_token(req.token, google_requests.Request(), GOOGLE_CLIENT_ID)
        email = idinfo['email']
        google_id = idinfo['sub']
        name = idinfo.get('name', email.split('@')[0])
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("SELECT id, role, username FROM users WHERE email = %s OR google_id = %s", (email, google_id))
        user = cursor.fetchone()
        
        if user:
            # Update google_id if missing
            cursor.execute("UPDATE users SET google_id = %s, is_verified = TRUE WHERE id = %s", (google_id, user['id']))
            user_id = user['id']
            role = user['role']
            username = user['username']
        else:
            # Create new user
            user_id = str(uuid.uuid4())
            role = "BUYER"
            username = name.replace(" ", "").lower() + str(random.randint(100,999))
            cursor.execute("""
                INSERT INTO users (id, username, email, google_id, role, is_verified)
                VALUES (%s, %s, %s, %s, %s, TRUE)
            """, (user_id, username, email, google_id, role))
            cursor.execute("INSERT INTO wallets (user_id) VALUES (%s)", (user_id,))
            
        conn.commit()
        conn.close()
        
        access_token = create_access_token(data={"sub": str(user_id), "role": role})
        return {
            "access_token": access_token, 
            "token_type": "bearer", 
            "user_id": str(user_id),
            "role": role,
            "username": username
        }
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid Google token")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/api/login", response_model=Token)
def login(login_data: LoginRequest):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT id, username, password_hash, role, is_verified FROM users WHERE username=%s OR email=%s", (login_data.identifier, login_data.identifier))
    user = cursor.fetchone()
    conn.close()

    if not user or hash_text(login_data.password) != user['password_hash']:
        raise HTTPException(status_code=400, detail="Invalid credentials")
    if not user['is_verified']:
        raise HTTPException(status_code=403, detail="Account not verified. Please verify your OTP.")

    access_token = create_access_token(data={"sub": str(user['id']), "role": user['role']})
    return {
        "access_token": access_token, 
        "token_type": "bearer", 
        "user_id": str(user['id']),
        "role": user['role'],
        "username": user['username']
    }

# --- 7. YOUR EXISTING ROUTES (Untouched) ---

@app.post("/api/cart/add")
def add_to_cart(req: CartAddRequest, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT id FROM marketplace_listings WHERE id = %s", (req.listing_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Item not found")

        cursor.execute("""
            INSERT INTO cart_items (user_id, listing_id, quantity)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, listing_id) 
            DO UPDATE SET quantity = cart_items.quantity + EXCLUDED.quantity
        """, (user['user_id'], req.listing_id, req.quantity))
        
        conn.commit()
        return {"status": "success", "message": "Item added to cart"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail="Failed to add item to cart")
    finally:
        conn.close()

@app.get("/api/cart")
def get_cart(user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT c.listing_id, c.quantity, c.added_at, 
                   l.brand, l.model, l.price, l.currency, l.specs, l.listing_type, l.seller_name
            FROM cart_items c
            JOIN marketplace_listings l ON c.listing_id = l.id
            WHERE c.user_id = %s
            ORDER BY c.added_at DESC
        """, (user['user_id'],))
        items = cursor.fetchall()
        
        total_value = 0
        for item in items:
            if isinstance(item['specs'], str):
                item['specs'] = json.loads(item['specs'])
            
            if 'images' in item['specs'] and len(item['specs']['images']) > 0:
                item['image'] = item['specs']['images'][0]
            else:
                item['image'] = "https://via.placeholder.com/300?text=No+Image"
            
            del item['specs'] 
            total_value += (item['price'] * item['quantity'])

        return {
            "items": items,
            "summary": {
                "item_count": sum(i['quantity'] for i in items),
                "total_value": total_value,
                "currency": items[0]['currency'] if items else "KES"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.delete("/api/cart/remove/{listing_id}")
def remove_from_cart(listing_id: str, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM cart_items WHERE user_id = %s AND listing_id = %s", (user['user_id'], listing_id))
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/api/ai-consultant")
def ask_ai_engineer(req: AIChatRequest):
    if not ai_client:
        return {"status": "error", "response": "AI Client not initialized. Check API Key."}
    try:
        system_instruction = (
            "You are a Senior Mechanical Engineer at DAGIV ENGINEERING in Kenya. "
            "You provide technical, safety-conscious advice about heavy machinery. "
            "Keep answers concise (under 100 words). "
            "Be professional and authoritative."
        )
        full_prompt = f"{system_instruction}\n\nUSER QUESTION: {req.prompt}"
        response = ai_client.models.generate_content(
            model="gemini-1.5-flash-001", 
            contents=full_prompt
        )
        return {"status": "success", "response": response.text}
    except Exception as e:
        return {
            "status": "error", 
            "response": "Our AI Consultant is currently offline. Please contact our human engineers."
        }

@app.post("/api/book-inspection")
def book_inspection(request: InspectionRequest, background_tasks: BackgroundTasks):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspection_requests (
                id SERIAL PRIMARY KEY,
                machine_type TEXT,
                location TEXT,
                contact_person TEXT,
                phone TEXT,
                date TEXT,
                status TEXT DEFAULT 'Pending'
            )
        """)
        cursor.execute(
            "INSERT INTO inspection_requests (machine_type, location, contact_person, phone, date) VALUES (%s, %s, %s, %s, %s)",
            (request.machineType, request.location, request.contactPerson, request.phone, request.date)
        )
        conn.commit()
        conn.close()
        
        details = f"Machine: {request.machineType}\nLocation: {request.location}\nClient: {request.contactPerson}\nPhone: {request.phone}\nRequested Date: {request.date}"
        background_tasks.add_task(send_email_alert, "Inspection Booking", details)
        return {"status": "success", "message": "Inspection booked"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
    
@app.post("/api/operator-logs")
def submit_log(log: OperatorLog, background_tasks: BackgroundTasks, current_user: dict = Depends(get_current_user)):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            t_start = datetime.strptime(log.startTime, "%H:%M")
            t_end = datetime.strptime(log.endTime, "%H:%M")
            duration = (t_end - t_start).total_seconds() / 3600.0 
            if duration < 0: duration += 24 
        except:
            duration = 0.0

        usage_unit = "km" 
        clean_notes = log.notes
        if "[UNIT:" in log.notes:
            parts = log.notes.split("[UNIT:")
            clean_notes = parts[0].strip()
            usage_unit = parts[1].replace("]", "").strip()

        full_remarks = f"FUEL: {log.fuelAddedLiters}L | OP: {log.operatorName} | {clean_notes}"
        
        cursor.execute("""
            INSERT INTO service_logs 
            (vehicle, service_type, service_date, hours, mileage, remarks, usage_unit) 
            VALUES (%s, 'Operator Daily Log', %s, %s, %s, %s, %s)
        """, (log.machineId, log.date, round(duration, 2), log.startOdometer, full_remarks, usage_unit))
        
        conn.commit()
        conn.close()
        
        if clean_notes or log.fuelAddedLiters > 0:
            details = f"Vehicle: {log.machineId}\nOperator: {log.operatorName}\nWork Duration: {round(duration, 2)} hrs\nCurrent Reading: {log.startOdometer} {usage_unit}\nNotes: {clean_notes}"
            background_tasks.add_task(send_email_alert, "Daily Log Alert", details)
        return {"status": "success", "message": "Log submitted securely"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/api/consultation")
def book_consultation(req: ConsultationRequest, background_tasks: BackgroundTasks):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS consultation_requests (
                id SERIAL PRIMARY KEY,
                name TEXT,
                phone TEXT,
                type TEXT,
                details TEXT,
                status TEXT DEFAULT 'Pending',
                date_logged TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute(
            "INSERT INTO consultation_requests (name, phone, type, details) VALUES (%s, %s, %s, %s)",
            (req.name, req.phone, req.type, req.details)
        )
        conn.commit()
        conn.close()
        details = f"Client: {req.name}\nPhone: {req.phone}\nConsult Type: {req.type}\n\nProject Details:\n{req.details}"
        background_tasks.add_task(send_email_alert, "Consultation Request", details)
        return {"status": "success", "message": "Consultation booked"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/api/service-request")
def request_service(req: ServiceRequest, background_tasks: BackgroundTasks):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS service_inquiries (
                id SERIAL PRIMARY KEY,
                client_name TEXT,
                phone TEXT,
                email TEXT,
                request_type TEXT,
                details TEXT,
                company TEXT,
                category TEXT DEFAULT 'Service', 
                status TEXT DEFAULT 'Pending',
                date_logged TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute(
            "INSERT INTO service_inquiries (client_name, phone, email, request_type, details, company, category) VALUES (%s, %s, %s, %s, %s, %s, 'Service')",
            (req.name, req.phone, req.email, req.serviceType, req.details, req.company)
        )
        conn.commit()
        conn.close()
        details = f"Client: {req.name}\nCompany: {req.company}\nPhone: {req.phone}\nEmail: {req.email}\nService: {req.serviceType}\n\nRequirements:\n{req.details}"
        background_tasks.add_task(send_email_alert, "Service Request", details)
        return {"status": "success", "message": "Service request received"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/api/lease-request")
def request_lease(req: LeaseRequestKP, background_tasks: BackgroundTasks):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS service_inquiries (
                id SERIAL PRIMARY KEY,
                client_name TEXT,
                phone TEXT,
                email TEXT,
                request_type TEXT,
                details TEXT,
                company TEXT,
                category TEXT DEFAULT 'Service', 
                status TEXT DEFAULT 'Pending',
                date_logged TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        details_str = f"Interested in leasing {req.machineName} (ID: {req.machineId}) for {req.duration}"
        cursor.execute(
            "INSERT INTO service_inquiries (client_name, phone, request_type, details, category) VALUES (%s, %s, 'Lease Inquiry', %s, 'Lease')",
            (req.customerName, req.phone, details_str)
        )
        conn.commit()
        conn.close()
        details = f"Client: {req.customerName}\nPhone: {req.phone}\nTarget Machine: {req.machineName}\nDuration: {req.duration}"
        background_tasks.add_task(send_email_alert, "Lease Inquiry", details)
        return {"status": "success", "message": "Lease inquiry sent"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.post("/api/sellers/check")
def check_seller_status(check: SellerCheck):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sellers (
            id SERIAL PRIMARY KEY,
            name TEXT,
            phone TEXT UNIQUE,
            location TEXT,
            email TEXT,
            status TEXT DEFAULT 'PENDING',
            rating REAL DEFAULT 0.0,
            joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            business_type TEXT,
            reg_number TEXT,
            doc_primary TEXT,
            doc_secondary TEXT,
            doc_proof TEXT
        )
    """)
    try:
        cursor.execute("ALTER TABLE sellers ADD COLUMN doc_primary TEXT")
        cursor.execute("ALTER TABLE sellers ADD COLUMN doc_secondary TEXT")
        cursor.execute("ALTER TABLE sellers ADD COLUMN doc_proof TEXT")
    except:
        conn.rollback()
    else:
        conn.commit()
    cursor.execute("SELECT name, status, id FROM sellers WHERE phone = %s", (check.phone,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return {"exists": True, "name": result[0], "status": result[1], "sellerId": result[2]}
    else:
        return {"exists": False, "status": "UNKNOWN"}

@app.post("/api/sellers/register")
def register_seller(seller: SellerRegistration, background_tasks: BackgroundTasks):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM users WHERE email = %s OR phone = %s", (seller.email, seller.phone))
        existing_user = cursor.fetchone()
        if existing_user:
            final_user_id = existing_user[0]
        else:
            user_uuid = str(uuid.uuid4())
            hashed_pw = hash_text(seller.password)
            cursor.execute(
                """INSERT INTO users (id, username, email, phone, password_hash, role, is_verified)
                   VALUES (%s, %s, %s, %s, %s, 'SELLER', FALSE)
                   RETURNING id""",
                (user_uuid, seller.email.split('@')[0], seller.email, seller.phone, hashed_pw)
            )
            final_user_id = cursor.fetchone()[0]

        cursor.execute(
            """INSERT INTO sellers 
               (name, phone, location, email, business_type, reg_number, doc_primary, doc_secondary, doc_proof, status) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING') 
               ON CONFLICT (phone) DO UPDATE 
               SET name=EXCLUDED.name, location=EXCLUDED.location, email=EXCLUDED.email, 
                   doc_primary=EXCLUDED.doc_primary, doc_secondary=EXCLUDED.doc_secondary, doc_proof=EXCLUDED.doc_proof
               RETURNING id""",
            (seller.name, seller.phone, seller.location, seller.email, seller.businessType, seller.regNumber, 
             seller.doc_primary, seller.doc_secondary, seller.doc_proof)
        )
        seller_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO wallets (user_id, balance_available, balance_pending, currency)
            VALUES (%s, 0.00, 0.00, 'KES')
            ON CONFLICT (user_id) DO NOTHING
        """, (final_user_id,))
        conn.commit()
        details = f"New {seller.businessType} Registration\nName: {seller.name}\nPhone: {seller.phone}\nEmail: {seller.email}\nAction: LOGIN TO ADMIN PANEL TO VERIFY"
        background_tasks.add_task(send_email_alert, "Seller Registration", details)
        return {"status": "success", "message": "Registration received", "sellerId": seller_id}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "detail": str(e)}
    finally:
        conn.close()

@app.post("/api/marketplace/submit")
def submit_listing(item: MarketListing, background_tasks: BackgroundTasks, user: dict = Depends(get_current_user)):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT phone FROM users WHERE id = %s", (user['user_id'],))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(status_code=404, detail="User profile not found. Please re-login.")
        db_phone = user_row[0]
        item.phone = db_phone 
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS marketplace_listings (
                id TEXT PRIMARY KEY,
                listing_type TEXT,
                seller_name TEXT,
                phone TEXT,
                location TEXT,
                category TEXT,
                sub_category TEXT,
                brand TEXT,
                model TEXT,
                price REAL,
                currency TEXT,
                specs JSONB,
                status TEXT DEFAULT 'PENDING_REVIEW',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        import time
        listing_id = f"LST-{int(time.time())}"
        specs_json = json.dumps(item.specs)
        cursor.execute("""
            INSERT INTO marketplace_listings 
            (id, listing_type, seller_name, phone, location, category, sub_category, brand, model, price, currency, specs, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE')
        """, (listing_id, item.listingType, item.sellerName, item.phone, item.location, 
              item.category, item.subCategory, item.brand, item.model, 
              item.price, item.currency, specs_json))
        conn.commit()
        conn.close()
        background_tasks.add_task(send_email_alert, "New Listing (Live)", f"ID: {listing_id}\nSeller: {item.sellerName}\nPhone: {item.phone}\nItem: {item.brand} {item.model}\nStatus: LIVE")
        return {"status": "success", "listingId": listing_id, "message": "Listing is now LIVE"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/api/seller/dashboard")
def get_seller_dashboard(user: dict = Depends(require_role("SELLER"))):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT phone FROM users WHERE id = %s", (user['user_id'],))
        seller_row = cursor.fetchone()
        if not seller_row:
            raise HTTPException(status_code=404, detail="Seller profile not found")
        seller_phone = seller_row['phone']
        cursor.execute("SELECT balance_available, balance_pending, currency FROM wallets WHERE user_id = %s", (user['user_id'],))
        wallet = cursor.fetchone()
        cursor.execute("""
            SELECT 
                COUNT(*) as total_listings,
                COUNT(*) FILTER (WHERE status = 'ACTIVE') as active_listings
            FROM marketplace_listings 
            WHERE phone = %s
        """, (seller_phone,))
        inventory = cursor.fetchone()
        cursor.execute("""
            SELECT id, brand, model, price, currency, status, created_at 
            FROM marketplace_listings 
            WHERE phone = %s 
            ORDER BY created_at DESC 
            LIMIT 5
        """, (seller_phone,))
        recent_listings = cursor.fetchall()

        return {
            "wallet": wallet,
            "inventory": inventory,
            "listings": recent_listings,
            "performance": {"rating": 4.8, "orders_completed": 0} 
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to load dashboard")
    finally:
        conn.close()

@app.post("/api/checkout/process")
def process_checkout(req: CheckoutProcessRequest, background_tasks: BackgroundTasks, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT c.listing_id, c.quantity, l.price, l.seller_name, l.phone as seller_phone
            FROM cart_items c
            JOIN marketplace_listings l ON c.listing_id = l.id
            WHERE c.user_id = %s
        """, (user['user_id'],))
        items = cursor.fetchall()
        if not items: raise HTTPException(status_code=400, detail="Cart is empty")

        subtotal = sum(i['price'] * i['quantity'] for i in items)
        shipping_cost = 0.0 
        total = subtotal + shipping_cost

        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        cursor.execute("""
            INSERT INTO orders (id, buyer_id, total_amount, currency, status, payment_method, shipping_details)
            VALUES (%s, %s, %s, 'KES', 'PENDING_PAYMENT', %s, %s)
        """, (order_id, user['user_id'], total, req.payment_method, json.dumps(req.shipping_details)))

        for item in items:
            cursor.execute("""
                INSERT INTO order_line_items (order_id, listing_id, quantity, unit_price, seller_phone)
                VALUES (%s, %s, %s, %s, %s)
            """, (order_id, item['listing_id'], item['quantity'], item['price'], item['seller_phone']))

        cursor.execute("DELETE FROM cart_items WHERE user_id = %s", (user['user_id'],))
        
        payment_info = {}
        if req.payment_method == 'MPESA':
            mpesa = MpesaService()
            res = mpesa.stk_push(req.mpesa_phone, int(total), order_id)
            if "ResponseCode" in res and res["ResponseCode"] == "0":
                trx_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO transactions (id, order_id, amount, type, status, checkout_request_id, phone_number)
                    VALUES (%s, %s, %s, 'ESCROW_DEPOSIT', 'PROCESSING', %s, %s)
                """, (trx_id, order_id, total, res['CheckoutRequestID'], req.mpesa_phone))
                payment_info = {"type": "MPESA", "message": "STK Push sent to your phone. Please enter your PIN to secure the funds in Escrow."}
            else:
                payment_info = {"type": "ERROR", "message": res.get('errorMessage', "Failed to trigger M-Pesa. Order saved as Pending.")}
        elif req.payment_method == 'BANK':
            payment_info = {
                "type": "BANK",
                "message": "Please transfer funds to the DAGIV Escrow Account below.",
                "bank": "KCB Bank Kenya",
                "account_name": "DAGIV Engineering Ltd",
                "account_number": "1280877812",
                "branch": "Industrial Area",
                "reference": order_id
            }
        elif req.payment_method == 'CARD':
            pesapal = PesapalService()
            email = req.shipping_details.get('email', 'buyer@dagiv.com')
            phone = req.shipping_details.get('phone', '0700000000')
            first_name = req.shipping_details.get('firstName', 'Guest')
            last_name = req.shipping_details.get('lastName', 'User')
            res = pesapal.submit_order(order_id, total, phone, email, first_name, last_name)
            if res and "redirect_url" in res:
                payment_info = {"type": "CARD", "message": "Secure gateway initialized.", "url": res["redirect_url"]}
            else:
                error_msg = res.get('message', 'Unknown Error') if isinstance(res, dict) else 'Check IPN URL'
                raise HTTPException(status_code=400, detail=f"Failed to initialize Pesapal gateway: {error_msg}")
        
        conn.commit()
        background_tasks.add_task(send_email_alert, "New Order via Checkout", f"Order ID: {order_id}\nTotal: KES {total}\nMethod: {req.payment_method}")
        return {"status": "success", "order_id": order_id, "payment_info": payment_info}
    except HTTPException as he:
        raise he 
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/api/orders/{order_id}/release-escrow")
def release_escrow(order_id: str, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT status FROM orders WHERE id = %s AND buyer_id = %s", (order_id, user['user_id']))
        order = cursor.fetchone()
        if not order: raise HTTPException(status_code=404, detail="Order not found")
        if order['status'] != 'FUNDS_SECURED' and order['status'] != 'IN_TRANSIT':
            raise HTTPException(status_code=400, detail="Order is not in a releasable state")

        cursor.execute("UPDATE orders SET status = 'RELEASED' WHERE id = %s", (order_id,))
        cursor.execute("SELECT seller_phone, unit_price, quantity FROM order_line_items WHERE order_id = %s", (order_id,))
        items = cursor.fetchall()
        for item in items:
            item_total = item['unit_price'] * item['quantity']
            cursor.execute("SELECT id FROM users WHERE phone = %s", (item['seller_phone'],))
            seller = cursor.fetchone()
            if seller:
                cursor.execute("""
                    UPDATE wallets 
                    SET balance_pending = balance_pending - %s, balance_available = balance_available + %s 
                    WHERE user_id = %s
                """, (item_total, item_total, seller['id']))
        conn.commit()
        return {"status": "success", "message": "Funds have been released to the seller."}
    finally: conn.close()

@app.post("/api/payments/mpesa/callback")
async def mpesa_callback(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
        callback_data = data.get("Body", {}).get("stkCallback", {})
        result_code = callback_data.get("ResultCode")
        checkout_request_id = callback_data.get("CheckoutRequestID")

        if result_code == 0:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("SELECT order_id FROM transactions WHERE checkout_request_id = %s", (checkout_request_id,))
            txn = cursor.fetchone()
            if txn:
                order_id = txn['order_id']
                cursor.execute("UPDATE orders SET status = 'FUNDS_SECURED' WHERE id = %s", (order_id,))
                cursor.execute("UPDATE transactions SET status = 'COMPLETED' WHERE checkout_request_id = %s", (checkout_request_id,))
                cursor.execute("SELECT seller_phone, unit_price, quantity FROM order_line_items WHERE order_id = %s", (order_id,))
                items = cursor.fetchall()
                for item in items:
                    item_total = item['unit_price'] * item['quantity']
                    cursor.execute("SELECT id FROM users WHERE phone = %s", (item['seller_phone'],))
                    seller = cursor.fetchone()
                    if seller:
                        cursor.execute("UPDATE wallets SET balance_pending = balance_pending + %s WHERE user_id = %s", (item_total, seller['id']))
                conn.commit()
                background_tasks.add_task(send_email_alert, "Escrow Secured (M-Pesa)", f"Order {order_id} has been fully funded.")
            conn.close()
        return {"ResultCode": 0, "ResultDesc": "Accepted"}
    except Exception as e:
        return {"ResultCode": 1, "ResultDesc": "Rejected"}

@app.get("/api/marketplace/listings")
def get_public_listings():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("""
            SELECT id, listing_type as "listingType", seller_name as "sellerName", phone, location, category, sub_category as "subCategory", brand, model, price, currency, specs, status, created_at 
            FROM marketplace_listings WHERE status = 'ACTIVE' ORDER BY created_at DESC
        """)
        listings = cursor.fetchall()
        for item in listings:
            if isinstance(item['specs'], str):
                item['specs'] = json.loads(item['specs'])
                if 'images' in item['specs'] and len(item['specs']['images']) > 0:
                    item['images'] = item['specs']['images']
                else:
                    item['images'] = ["https://via.placeholder.com/300?text=No+Image"]
                item['verifiedByDagiv'] = True
        return listings
    except Exception as e:
        return []
    finally:
        conn.close()

# --- 8. NEW ASYNC LOGISTICS TRACKING (PHASE 5) ---

LOGISTICS_DB = {} # Memory mock for logistics

def create_shipment(order_id: str, destination: dict, customer_contact: dict) -> dict:
    tracking_number = f"DAGIV-TRK-{uuid.uuid4().hex[:8].upper()}"
    shipment = {
        "tracking_number": tracking_number,
        "order_id": order_id,
        "status": "DISPATCHED",
        "destination": destination,
        "customer_contact": customer_contact,
        "updates": [{
            "status": "DISPATCHED",
            "timestamp": datetime.utcnow().isoformat(),
            "location": "DAGIV Central Warehouse, Nairobi",
            "notes": "Shipment has been dispatched and handed over to logistics partner."
        }]
    }
    LOGISTICS_DB[tracking_number] = shipment
    
    sms_message = f"Your DAGIV order {order_id} has been dispatched. Tracking Number: {tracking_number}."
    if customer_contact.get("phone"):
        send_sms_async.delay(customer_contact["phone"], sms_message)
    return shipment

@app.post("/api/logistics/shipments", tags=["Logistics"])
def dispatch_order(shipment_data: ShipmentCreate):
    shipment = create_shipment(
        order_id=shipment_data.order_id,
        destination=shipment_data.destination.model_dump(),
        customer_contact=shipment_data.customer_contact.model_dump()
    )
    return {"message": "Shipment created successfully", "shipment": shipment}

@app.post("/api/logistics/shipments/{tracking_number}/update", tags=["Logistics"])
def update_shipment(tracking_number: str, update_data: ShipmentUpdate):
    update_shipment_status_async.delay(
        tracking_number=tracking_number,
        new_status=update_data.status,
        location=update_data.location,
        notes=update_data.notes
    )
    return {"message": "Shipment update queued successfully"}

@app.get("/api/logistics/track/{tracking_number}", tags=["Logistics"])
def track_shipment(tracking_number: str):
    info = LOGISTICS_DB.get(tracking_number)
    if not info: raise HTTPException(status_code=404, detail="Tracking number not found")
    return info

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)