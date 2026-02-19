import psycopg2
from psycopg2.extras import RealDictCursor
import hashlib
import smtplib
import ssl
import uuid
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from jose import JWTError, jwt
import uvicorn
import os
import json
from google import genai
from typing import Optional

# --- NEW IMPORT ---
from mpesa_services import MpesaService

# --- 1. CONFIGURATION ---
DATABASE_URL = "postgresql://postgres.fzmydgefyoaglnroenae:IvyEngineering2026@aws-1-eu-west-2.pooler.supabase.com:6543/postgres?sslmode=require"
ADMIN_EMAIL = "dagivengineering@gmail.com" 
SENDER_EMAIL = "dagivengineering@gmail.com"
SENDER_PASSWORD = "rlcn kqim otgr kgcd" 
SECRET_KEY = "DAGIV_SUPER_SECRET_KEY_CHANGE_THIS_IN_PROD"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 

GEMINI_API_KEY = "AIzaSyBsDgJjYoXFQCLHRYkdeabEklIFMGvihoQ" 
try:
    ai_client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print(f"⚠️ AI Client Init Error: {e}")
    ai_client = None

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    # Add 5173 (Vite's default port) to this list
    allow_origins=[
        "http://localhost:30001", "http://127.0.0.1:30001",
        "http://localhost:5173", "http://127.0.0.1:5173",
        "http://localhost:3000", "http://127.0.0.1:3000"
    ], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")

# --- 2. DATA MODELS ---

class UserRegister(BaseModel):
    email: str
    phone: str
    password: str
    role: str = "BUYER"
    business_name: str = None

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
    payment_method: str # 'MPESA', 'BANK'
    duration: int = 1 # For rentals (days)
    shipping_cost: float = 0.0

class OrderStatusUpdate(BaseModel):
    status: str 

# --- NEW MODEL FOR MPESA ---
class MpesaPaymentRequest(BaseModel):
    order_id: str
    phone_number: str
    amount: float

# --- 3. NOTIFICATION SYSTEM ---

def send_email_alert(category: str, details: str):
    if "REPLACE_THIS" in SENDER_PASSWORD:
        print("❌ Email skipped: Password not configured.")
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
        print(f"✅ Email Alert sent to {ADMIN_EMAIL}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# --- 4. HELPERS ---
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def hash_text(s: str) -> str:
    return hashlib.sha256((s or "").encode()).hexdigest()

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# Update imports to include logging
import logging

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # PRINT THE TOKEN (For debugging only - remove in production!)
        print(f"🔍 Received Token: {token[:10]}...{token[-10:]}") 

        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        role: str = payload.get("role")
        
        print(f"✅ Token Decoded: User={user_id}, Role={role}") 

        if user_id is None:
            print("❌ Error: Token missing 'sub' (User ID)")
            raise credentials_exception
            
        return {"user_id": user_id, "role": role}
        
    except JWTError as e:
        print(f"❌ Token Validation Failed: {str(e)}") 
        raise credentials_exception

def require_role(required_role: str):
    def role_checker(user = Depends(get_current_user)):
        # Allow ADMIN to access everything, otherwise check role
        if user["role"] != required_role and user["role"] != "ADMIN":
            raise HTTPException(status_code=403, detail=f"Access denied. Requires {required_role}")
        return user
    return role_checker

# --- 5. ROUTES ---

@app.on_event("startup")
def startup_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 2. Transactions Table
    # FIX: We use TEXT for id since some databases strictly enforce UUID formats.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            order_id TEXT,
            amount REAL,
            type TEXT, 
            status TEXT DEFAULT 'PENDING',
            checkout_request_id TEXT,  -- For M-Pesa Tracking
            mpesa_receipt TEXT,        -- Confirmed Receipt No
            phone_number TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 3. Migration: Force add ALL new columns if they are missing
    try:
        # Update orders table
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS listing_id TEXT")
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS seller_phone TEXT")
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_amount REAL") 
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS currency TEXT")
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_method TEXT")
        
        # Update transactions table
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS type TEXT")           
        # FIX FOR ENUM ERROR: Force 'type' and 'status' columns to be standard TEXT
        cursor.execute("ALTER TABLE transactions ALTER COLUMN type TYPE TEXT USING type::text")
        
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING'") 
        cursor.execute("ALTER TABLE transactions ALTER COLUMN status TYPE TEXT USING status::text")
        
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS checkout_request_id TEXT")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS mpesa_receipt TEXT")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS phone_number TEXT")
        
        conn.commit()
        print("✅ Database Migration: Converted ENUM to TEXT and added missing columns to schema.")
    except Exception as e:
        print(f"⚠️ Migration warning (Safe to ignore if columns already exist/converted): {e}")
        conn.rollback()
        
    conn.commit()
    conn.close()

@app.get("/")
def read_root():
    return {"message": "DAGIV API (Secured) is Online"}

@app.post("/api/auth/register", response_model=Token)
def register_user(user: UserRegister):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        if user.role not in ['BUYER', 'SELLER', 'COURIER', 'OPERATOR', 'MECHANIC']:
             raise HTTPException(status_code=400, detail=f"Invalid role selected: {user.role}")
        cursor.execute("SELECT id FROM users WHERE email = %s OR phone = %s", (user.email, user.phone))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Email or Phone already registered")
            
        hashed_pw = hash_text(user.password)
        new_uuid = str(uuid.uuid4())
        
        cursor.execute("""
            INSERT INTO users (id, email, phone, password_hash, role, is_verified, username)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, role
        """, (new_uuid, user.email, user.phone, hashed_pw, user.role, False, user.email.split('@')[0]))
        
        new_user = cursor.fetchone()
        new_user_id = new_user['id']
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                id SERIAL PRIMARY KEY,
                user_id TEXT UNIQUE,
                balance_available REAL DEFAULT 0.00,
                balance_pending REAL DEFAULT 0.00,
                currency TEXT DEFAULT 'KES'
            )
        """)
        cursor.execute("""
            INSERT INTO wallets (user_id, balance_available, balance_pending, currency)
            VALUES (%s, 0.00, 0.00, 'KES')
        """, (new_user_id,))
        
        if user.role == 'SELLER':
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sellers (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    phone TEXT UNIQUE,
                    email TEXT,
                    status TEXT DEFAULT 'PENDING'
                )
            """)
            cursor.execute("INSERT INTO sellers (name, phone, email, status) VALUES (%s, %s, %s, 'PENDING')", 
                           (user.business_name or "New Seller", user.phone, user.email))

        conn.commit()
        access_token = create_access_token(
            data={"sub": str(new_user_id), "role": user.role}
        )
        return {
            "access_token": access_token, 
            "token_type": "bearer", 
            "user_id": str(new_user_id),
            "role": user.role,
            "username": user.email.split('@')[0]
        }

    except Exception as e:
        conn.rollback()
        print(f"Registration Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/api/auth/login", response_model=Token)
@app.post("/api/login", response_model=Token)
def login(login_data: LoginRequest):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT id, username, password_hash, role FROM users WHERE username=%s OR email=%s", (login_data.identifier, login_data.identifier))
    user = cursor.fetchone()
    conn.close()

    if not user:
        raise HTTPException(status_code=400, detail="Invalid credentials")
    if hash_text(login_data.password) != user['password_hash']:
        raise HTTPException(status_code=400, detail="Invalid credentials")


    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": str(user['id']), "role": user['role']}, 
        expires_delta=access_token_expires
    )
    
    return {
        "access_token": access_token, 
        "token_type": "bearer", 
        "user_id": str(user['id']),
        "role": user['role'],
        "username": user['username'] or "User"
    }

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
        print(f"❌ Gemini AI Error: {str(e)}")
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
        print(f"Server Error: {e}")
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

# --- FIXED: SELLER REGISTRATION WITH LOGIN CREATION ---
@app.post("/api/sellers/register")
def register_seller(seller: SellerRegistration, background_tasks: BackgroundTasks):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Create User Account (Login Credentials)
        # Check if user already exists
        cursor.execute("SELECT id FROM users WHERE email = %s OR phone = %s", (seller.email, seller.phone))
        existing_user = cursor.fetchone()
        
        if existing_user:
            # User exists, maybe upgrading? We will proceed to create seller profile
            final_user_id = existing_user[0]
        else:
            # Create new user
            user_uuid = str(uuid.uuid4())
            hashed_pw = hash_text(seller.password)
            cursor.execute(
                """INSERT INTO users (id, username, email, phone, password_hash, role, is_verified)
                   VALUES (%s, %s, %s, %s, %s, 'SELLER', FALSE)
                   RETURNING id""",
                (user_uuid, seller.email.split('@')[0], seller.email, seller.phone, hashed_pw)
            )
            final_user_id = cursor.fetchone()[0]

        # 2. Create/Update Seller Profile (KYC Data)
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

        # 3. Create Wallet (if not exists)
        cursor.execute("""
            INSERT INTO wallets (user_id, balance_available, balance_pending, currency)
            VALUES (%s, 0.00, 0.00, 'KES')
            ON CONFLICT (user_id) DO NOTHING
        """, (final_user_id,))
        
        conn.commit()
        
        # 4. Notify Admin
        details = f"New {seller.businessType} Registration\nName: {seller.name}\nPhone: {seller.phone}\nEmail: {seller.email}\nAction: LOGIN TO ADMIN PANEL TO VERIFY"
        background_tasks.add_task(send_email_alert, "Seller Registration", details)
        
        return {"status": "success", "message": "Registration received", "sellerId": seller_id}
        
    except Exception as e:
        conn.rollback()
        print(f"Registration Error: {e}")
        return {"status": "error", "detail": str(e)}
    finally:
        conn.close()

# --- MARKETPLACE SUBMIT (UPDATED) ---
@app.post("/api/marketplace/submit")
def submit_listing(item: MarketListing, background_tasks: BackgroundTasks, user: dict = Depends(get_current_user)):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. SECURITY: Fetch the actual phone number from the User Profile using the token ID
        cursor.execute("SELECT phone FROM users WHERE id = %s", (user['user_id'],))
        user_row = cursor.fetchone()
        
        if not user_row:
            raise HTTPException(status_code=404, detail="User profile not found. Please re-login.")
            
        # 2. ENFORCEMENT: Override the payload phone with the verified database phone
        db_phone = user_row[0]
        item.phone = db_phone # <--- This ensures exact match with profile for Dashboard visibility

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
        
        # 3. LOGIC CHANGE: Force status to ACTIVE immediately (Bypassing Review)
        initial_status = 'ACTIVE' 
        
        import time
        import json
        listing_id = f"LST-{int(time.time())}"
        specs_json = json.dumps(item.specs)
        
        cursor.execute("""
            INSERT INTO marketplace_listings 
            (id, listing_type, seller_name, phone, location, category, sub_category, brand, model, price, currency, specs, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (listing_id, item.listingType, item.sellerName, item.phone, item.location, 
              item.category, item.subCategory, item.brand, item.model, 
              item.price, item.currency, specs_json, initial_status))
        
        conn.commit()
        conn.close()
        
        # Notify Admin (Just for info, since it's already live)
        background_tasks.add_task(send_email_alert, "New Listing (Live)", f"ID: {listing_id}\nSeller: {item.sellerName}\nPhone: {item.phone}\nItem: {item.brand} {item.model}\nStatus: LIVE")
        
        return {"status": "success", "listingId": listing_id, "message": "Listing is now LIVE"}
        
    except Exception as e:
        print(f"Marketplace Error: {e}")
        return {"status": "error", "detail": str(e)}

@app.get("/api/seller/dashboard")
def get_seller_dashboard(user: dict = Depends(require_role("SELLER"))):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Get phone from user profile to find their listings
        cursor.execute("SELECT phone FROM users WHERE id = %s", (user['user_id'],))
        seller_row = cursor.fetchone()
        if not seller_row:
            raise HTTPException(status_code=404, detail="Seller profile not found")
        seller_phone = seller_row['phone']
        
        # Wallet
        cursor.execute("SELECT balance_available, balance_pending, currency FROM wallets WHERE user_id = %s", (user['user_id'],))
        wallet = cursor.fetchone()

        # Stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total_listings,
                COUNT(*) FILTER (WHERE status = 'ACTIVE') as active_listings
            FROM marketplace_listings 
            WHERE phone = %s
        """, (seller_phone,))
        inventory = cursor.fetchone()
        
        # Listings
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
        print(f"Dashboard Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to load dashboard")
    finally:
        conn.close()

# --- 6. ORDER & TRANSACTION ROUTES ---

@app.post("/api/orders/create")
def create_order(order: OrderCreate, background_tasks: BackgroundTasks, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # 1. Get Listing Details
        cursor.execute("SELECT price, currency, seller_name, phone, listing_type FROM marketplace_listings WHERE id = %s", (order.listing_id,))
        listing = cursor.fetchone()
        if not listing:
            raise HTTPException(status_code=404, detail="Listing not found")
            
        # 2. Calculate Total
        base_price = listing['price']
        total = (base_price * order.quantity * order.duration) + order.shipping_cost
        
        # 3. Create Order ID
        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        
        # 4. Save Order
        cursor.execute("""
            INSERT INTO orders (id, buyer_id, seller_phone, listing_id, total_amount, currency, status, payment_method)
            VALUES (%s, %s, %s, %s, %s, %s, 'PENDING_PAYMENT', %s)
        """, (order_id, user['user_id'], listing['phone'], order.listing_id, total, listing['currency'], order.payment_method))
        
        # 5. Log Transaction (Ledger)
        trans_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO transactions (id, order_id, amount, type, status)
            VALUES (%s, %s, %s, 'ESCROW_DEPOSIT', 'PENDING')
        """, (trans_id, order_id, total))
        
        conn.commit()
        
        # 6. Notify
        details = f"Order: {order_id}\nItem: {listing['listing_type']}\nAmount: {listing['currency']} {total}\nBuyer ID: {user['user_id']}"
        background_tasks.add_task(send_email_alert, "New Order Created", details)
        
        return {"status": "success", "orderId": order_id, "amount": total, "currency": listing['currency']}
        
    except Exception as e:
        conn.rollback()
        print(f"Order Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/api/seller/orders")
def get_seller_orders(user: dict = Depends(require_role("SELLER"))):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Get seller's phone to identify their orders
        cursor.execute("SELECT phone FROM users WHERE id = %s", (user['user_id'],))
        seller = cursor.fetchone()
        if not seller: raise HTTPException(status_code=404, detail="Seller not found")
        
        # Fetch Orders where this user is the seller
        cursor.execute("""
            SELECT 
                o.id, o.total_amount as amount, o.currency, o.status, o.created_at,
                o.payment_method, u.email as buyer_contact,
                l.brand, l.model, l.listing_type
            FROM orders o
            JOIN marketplace_listings l ON o.listing_id = l.id
            JOIN users u ON o.buyer_id = u.id
            WHERE o.seller_phone = %s
            ORDER BY o.created_at DESC
        """, (seller['phone'],))
        
        orders = cursor.fetchall()
        return orders
    except Exception as e:
        print(f"Order Fetch Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/api/orders/{order_id}/update-status")
def update_order_status(order_id: str, update: OrderStatusUpdate, background_tasks: BackgroundTasks, user: dict = Depends(require_role("SELLER"))):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # 1. Verify this order belongs to the seller
        cursor.execute("SELECT phone FROM users WHERE id = %s", (user['user_id'],))
        seller_phone = cursor.fetchone()['phone']
        
        cursor.execute("SELECT id, status, total_amount, buyer_id FROM orders WHERE id = %s AND seller_phone = %s", (order_id, seller_phone))
        order = cursor.fetchone()
        
        if not order:
            raise HTTPException(status_code=404, detail="Order not found or access denied")
            
        # 2. Update Status
        cursor.execute("UPDATE orders SET status = %s WHERE id = %s", (update.status, order_id))
        if update.status == 'COMPLETED':
            cursor.execute("""
                UPDATE wallets 
                SET balance_pending = balance_pending - %s, 
                    balance_available = balance_available + %s 
                WHERE user_id = %s
            """, (order['total_amount'], order['total_amount'], user['user_id']))
            
        conn.commit()
        
        # 4. Notify
        background_tasks.add_task(send_email_alert, f"Order {update.status}", f"Order {order_id} marked as {update.status} by Seller.")
        
        return {"status": "success", "new_state": update.status}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# --- MARKETPLACE LISTING FETCH (CRITICAL ADDITION) ---
@app.get("/api/marketplace/listings")
def get_public_listings():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Fetch only ACTIVE listings
        cursor.execute("""
            SELECT 
                id, 
                listing_type as "listingType", 
                seller_name as "sellerName", 
                phone, 
                location, 
                category, 
                sub_category as "subCategory", 
                brand, 
                model, 
                price, 
                currency, 
                specs, 
                status,
                created_at 
            FROM marketplace_listings 
            WHERE status = 'ACTIVE'
            ORDER BY created_at DESC
        """)
        listings = cursor.fetchall()
        
        # Parse JSON specs
        for item in listings:
            if isinstance(item['specs'], str):
                import json
                item['specs'] = json.loads(item['specs'])
                
                if 'images' in item['specs'] and len(item['specs']['images']) > 0:
                    item['images'] = item['specs']['images']
                else:
                    item['images'] = ["https://via.placeholder.com/300?text=No+Image"]

                item['verifiedByDagiv'] = True

        return listings
    except Exception as e:
        print(f"Market Fetch Error: {e}")
        return []
    finally:
        conn.close()

# --- NEW PAYMENT ENDPOINT ---
@app.post("/api/payments/mpesa/pay")
def trigger_mpesa_payment(req: MpesaPaymentRequest, background_tasks: BackgroundTasks):
    mpesa = MpesaService()
    
    # 1. Trigger STK Push
    # Note: Amount must be INT for M-Pesa
    response = mpesa.stk_push(req.phone_number, int(req.amount), req.order_id)
    
    if "ResponseCode" in response and response["ResponseCode"] == "0":
        # 2. Log Transaction state
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # FIX: Generate true UUID for transaction ID
        trx_id = str(uuid.uuid4())
        
        cursor.execute("""
            INSERT INTO transactions (id, order_id, amount, type, status, checkout_request_id, phone_number)
            VALUES (%s, %s, %s, 'PAYMENT_INITIATED', 'PROCESSING', %s, %s)
        """, (trx_id, req.order_id, req.amount, response['CheckoutRequestID'], req.phone_number))
        
        conn.commit()
        conn.close()
        
        return {"status": "success", "message": "STK Push Sent", "checkoutRequestId": response['CheckoutRequestID']}
    else:
        return {"status": "error", "detail": response.get("errorMessage", "Failed to initiate M-Pesa")}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)