# server.py
import psycopg2
import hashlib
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from jose import JWTError, jwt
import uvicorn
import os

# --- 1. CONFIGURATION ---
DATABASE_URL = "postgresql://postgres.fzmydgefyoaglnroenae:sB7FRUojV1IyiGxj@aws-1-eu-west-2.pooler.supabase.com:6543/postgres?sslmode=require"

# SECURITY CONFIG
SECRET_KEY = "DAGIV_SUPER_SECRET_KEY_CHANGE_THIS_IN_PROD"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 # 24 Hours

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# This tells FastAPI that the token comes from the "Authorization: Bearer" header
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")

# --- 2. DATA MODELS ---
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

# --- 3. HELPERS ---
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def hash_text(s: str) -> str:
    """Matches the SHA256 hashing used in the Tkinter Admin App"""
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
    """Validates the token sent by the Frontend"""
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

# --- 4. ROUTES ---

@app.get("/")
def read_root():
    return {"message": "DAGIV API (Secured) is Online"}

# --- AUTH ROUTE ---
@app.post("/api/login", response_model=Token)
def login(login_data: LoginRequest):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Verify user against the DB (Same table the Admin App writes to)
    cursor.execute("SELECT username, password, role FROM users WHERE username=%s", (login_data.username,))
    user = cursor.fetchone()
    conn.close()

    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    
    db_username, db_password_hash, db_role = user
    
    # Check Password (SHA256 Match)
    if hash_text(login_data.password) != db_password_hash:
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    # Create JWT
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": db_username, "role": db_role}, 
        expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer", "role": db_role}

# --- PROTECTED ROUTES ---

@app.post("/api/book-inspection")
def book_inspection(request: InspectionRequest):
    # Public endpoint (Clients don't login)
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
        return {"status": "success", "message": "Inspection booked"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/api/operator-logs")
def submit_log(log: OperatorLog, current_user: str = Depends(get_current_user)):
    # PROTECTED: Only logged-in users with a valid token can hit this
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Append the logged-in username to the record for audit trails
        full_remarks = f"{log.notes} | Loc: {log.location} | Op: {log.operatorName} (Auth: {current_user})"
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS service_logs (
                id SERIAL PRIMARY KEY,
                vehicle TEXT,
                service_type TEXT,
                service_date TEXT,
                cost REAL,
                remarks TEXT,
                mileage REAL,
                hours REAL,
                next_service REAL,
                group_id TEXT,
                currency TEXT,
                usage_unit TEXT
            )
        """)

        cursor.execute("""
            INSERT INTO service_logs 
            (vehicle, service_type, service_date, hours, mileage, remarks, usage_unit) 
            VALUES (%s, 'Operator Daily Log', %s, %s, %s, %s, 'Hours')
        """, (log.machineId, log.date, log.endTime, log.startOdometer, full_remarks))
        
        conn.commit()
        conn.close()
        return {"status": "success", "message": "Log submitted securely"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)