# server.py
import psycopg2
import hashlib
import smtplib
import ssl
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

# --- NEW: Import the modern Google GenAI Client ---
from google import genai 

# --- 1. CONFIGURATION ---
DATABASE_URL = "postgresql://postgres.fzmydgefyoaglnroenae:sB7FRUojV1IyiGxj@aws-1-eu-west-2.pooler.supabase.com:6543/postgres?sslmode=require"

# ADMIN CONTACT DETAILS
ADMIN_EMAIL = "jebetivy388@gmail.com" 

# EMAIL SENDER CONFIGURATION
SENDER_EMAIL = "jebetivy388@gmail.com"
SENDER_PASSWORD = "eupb xbce wbwa espe" 

# SECURITY CONFIG
SECRET_KEY = "DAGIV_SUPER_SECRET_KEY_CHANGE_THIS_IN_PROD"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 

GEMINI_API_KEY = "AIzaSyBsDgJjYoXFQCLHRYkdeabEklIFMGvihoQ" 

# Initialize the new Client
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

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")

# --- 2. DATA MODELS ---

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

class LoginRequest(BaseModel):
    username: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str
    role: str

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

# --- 3. NOTIFICATION SYSTEM (EMAIL ONLY) ---

def send_email_alert(category: str, details: str):
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
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return username

# --- 5. ROUTES ---

@app.get("/")
def read_root():
    return {"message": "DAGIV API (Secured) is Online"}

# --- AI CONSULTANT ROUTE (FIXED MODEL NAME) ---
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
        
        # FIX: Using specific model version 'gemini-1.5-flash-001'
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

@app.post("/api/login", response_model=Token)
def login(login_data: LoginRequest):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT username, password, role FROM users WHERE username=%s", (login_data.username,))
    user = cursor.fetchone()
    conn.close()

    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    db_username, db_password_hash, db_role = user
    if hash_text(login_data.password) != db_password_hash:
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": db_username, "role": db_role}, 
        expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer", "role": db_role}

# --- PROTECTED ROUTES WITH BACKGROUND NOTIFICATIONS ---

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
def submit_log(log: OperatorLog, background_tasks: BackgroundTasks, current_user: str = Depends(get_current_user)):
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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)