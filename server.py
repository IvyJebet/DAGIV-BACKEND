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
import logging

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
    # Kept your original Vite and React ports for dev
    allow_origins=[
        "http://localhost:30001", "http://127.0.0.1:30001",
        "http://localhost:5173", "http://127.0.0.1:5173",
        "http://localhost:3000", "http://127.0.0.1:3000",
        "*" # Added wildcard as suggested, but keep specific ones above if you lock this down later
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

class MpesaPaymentRequest(BaseModel):
    order_id: str
    phone_number: str
    amount: float

class CartAddRequest(BaseModel):
    listing_id: str
    quantity: int = 1

# --- NEW CHECKOUT & ESCROW MODELS ---
class CheckoutProcessRequest(BaseModel):
    payment_method: str # 'MPESA', 'BANK', 'CARD'
    mpesa_phone: Optional[str] = None
    shipping_details: dict

class EscrowReleaseRequest(BaseModel):
    order_id: str

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
            shipping_details JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 1.5 Order Items Table (For Multi-Item Checkout)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            id SERIAL PRIMARY KEY,
            order_id TEXT,
            listing_id TEXT,
            quantity INT,
            unit_price REAL, -- <--- PERMANENT FIX
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
            checkout_request_id TEXT,  -- For M-Pesa Tracking
            mpesa_receipt TEXT,        -- Confirmed Receipt No
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
    
    # 4. Migrations (Self-Healing)
    try:
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS listing_id TEXT")
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS seller_phone TEXT")
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_amount REAL") 
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS currency TEXT")
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_method TEXT")
        cursor.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS shipping_details JSONB")
        
        # PERMANENT FIX for order_items table mismatch
        cursor.execute("ALTER TABLE order_items ADD COLUMN IF NOT EXISTS unit_price REAL")
        
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS type TEXT")           
        cursor.execute("ALTER TABLE transactions ALTER COLUMN type TYPE TEXT USING type::text")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING'") 
        cursor.execute("ALTER TABLE transactions ALTER COLUMN status TYPE TEXT USING status::text")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS checkout_request_id TEXT")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS mpesa_receipt TEXT")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS phone_number TEXT")
        
        conn.commit()
    except Exception as e:
        print(f"Migration Info: {e}")
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
@app.post("/api/cart/add")
def add_to_cart(req: CartAddRequest, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Verify item exists
        cursor.execute("SELECT id FROM marketplace_listings WHERE id = %s", (req.listing_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Item not found")

        # Upsert (Insert or Update if exists)
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
        # Log the error but don't crash
        print(f"Cart Add Error: {e}")
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
            
            # Format image
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

@app.post("/api/sellers/register")
def register_seller(seller: SellerRegistration, background_tasks: BackgroundTasks):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Create User Account (Login Credentials)
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

        # 2. Create/Update Seller Profile
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

        # 3. Create Wallet
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


# --- 7. NEW ESCROW & CHECKOUT ROUTES ---

@app.post("/api/checkout/process")
def process_checkout(req: CheckoutProcessRequest, background_tasks: BackgroundTasks, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # 1. Fetch Cart Items
        cursor.execute("""
            SELECT c.listing_id, c.quantity, l.price, l.seller_name, l.phone as seller_phone
            FROM cart_items c
            JOIN marketplace_listings l ON c.listing_id = l.id
            WHERE c.user_id = %s
        """, (user['user_id'],))
        items = cursor.fetchall()
        
        if not items: raise HTTPException(status_code=400, detail="Cart is empty")

        # 2. Calculate Total
        subtotal = sum(i['price'] * i['quantity'] for i in items)
        shipping_cost = 15000.0 # Standard flat rate heavy haulage
        total = subtotal + shipping_cost

        # 3. Create Main Order
        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        cursor.execute("""
            INSERT INTO orders (id, buyer_id, total_amount, currency, status, payment_method, shipping_details)
            VALUES (%s, %s, %s, 'KES', 'PENDING_PAYMENT', %s, %s)
        """, (order_id, user['user_id'], total, req.payment_method, json.dumps(req.shipping_details)))

        # 4. Create Order Items (PERMANENT FIX: using unit_price)
        for item in items:
            cursor.execute("""
                INSERT INTO order_items (order_id, listing_id, quantity, unit_price, seller_phone)
                VALUES (%s, %s, %s, %s, %s)
            """, (order_id, item['listing_id'], item['quantity'], item['price'], item['seller_phone']))

        # 5. Clear Cart
        cursor.execute("DELETE FROM cart_items WHERE user_id = %s", (user['user_id'],))
        
        # 6. Payment Routing
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
                "bank": "Equity Bank Kenya",
                "account_name": "DAGIV Escrow Trust",
                "account_number": "012345678910",
                "branch": "Industrial Area",
                "reference": order_id
            }
            
        elif req.payment_method == 'CARD':
            payment_info = {
                "type": "CARD",
                "message": "Redirecting to secure card gateway...",
                "url": f"https://sandbox.pesapal.com/pay/{order_id}" # Mock URL
            }

        conn.commit()
        background_tasks.add_task(send_email_alert, "New Order via Checkout", f"Order ID: {order_id}\nTotal: KES {total}\nMethod: {req.payment_method}")
        return {"status": "success", "order_id": order_id, "payment_info": payment_info}
        
    except Exception as e:
        conn.rollback()
        print(f"Checkout Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/api/test/mock-payment-success/{order_id}")
def mock_payment_success(order_id: str):
    """Developer endpoint to simulate Safaricom confirming the STK push"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Update order to FUNDS_SECURED
        cursor.execute("UPDATE orders SET status = 'FUNDS_SECURED' WHERE id = %s RETURNING id", (order_id,))
        if not cursor.fetchone(): raise HTTPException(status_code=404, detail="Order not found")
        
        # Credit the pending balance of the sellers involved
        cursor.execute("SELECT seller_phone, unit_price, quantity FROM order_items WHERE order_id = %s", (order_id,))
        items = cursor.fetchall()
        for item in items:
            item_total = item['unit_price'] * item['quantity']
            # Find seller's user_id
            cursor.execute("SELECT id FROM users WHERE phone = %s", (item['seller_phone'],))
            seller = cursor.fetchone()
            if seller:
                cursor.execute("UPDATE wallets SET balance_pending = balance_pending + %s WHERE user_id = %s", (item_total, seller['id']))
        
        cursor.execute("UPDATE transactions SET status = 'COMPLETED' WHERE order_id = %s", (order_id,))
        conn.commit()
        return {"status": "success", "message": f"Order {order_id} is now FUNDS_SECURED. Seller sees funds as pending."}
    finally: conn.close()

@app.post("/api/orders/{order_id}/release-escrow")
def release_escrow(order_id: str, user: dict = Depends(get_current_user)):
    """Buyer endpoint to confirm receipt and release funds to the seller"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT status FROM orders WHERE id = %s AND buyer_id = %s", (order_id, user['user_id']))
        order = cursor.fetchone()
        if not order: raise HTTPException(status_code=404, detail="Order not found")
        if order['status'] != 'FUNDS_SECURED' and order['status'] != 'IN_TRANSIT':
            raise HTTPException(status_code=400, detail="Order is not in a releasable state")

        # Update order status
        cursor.execute("UPDATE orders SET status = 'RELEASED' WHERE id = %s", (order_id,))
        
        # Move pending funds to available funds for the seller
        cursor.execute("SELECT seller_phone, unit_price, quantity FROM order_items WHERE order_id = %s", (order_id,))
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

@app.get("/api/marketplace/listings")
def get_public_listings():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
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
        return []
    finally:
        conn.close()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)