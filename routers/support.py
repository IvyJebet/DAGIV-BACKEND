import os
import uuid
import json
import asyncio
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends, status, WebSocket, WebSocketDisconnect
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from dotenv import load_dotenv

# --- SQLAlchemy 2.0 & AsyncPG ---
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Boolean, DateTime, select, func, update
from sqlalchemy.dialects.postgresql import JSONB, UUID
import redis.asyncio as aioredis

load_dotenv()

# =====================================================================
# --- 0. DB & REDIS CONFIGURATION ---
# =====================================================================
SECRET_KEY = "DAGIV_SUPER_SECRET_KEY_CHANGE_THIS_IN_PROD"
ALGORITHM = "HS256"

# Setup Async SQLAlchemy Engine
# Dynamically and safely clean the URL so psycopg2 in server.py doesn't crash
raw_db_url = os.getenv("DATABASE_URL", "").strip().strip('"').strip("'")

# Fallback: Catch accidental duplications from the .env file
if raw_db_url.startswith("DATABASE_URL="):
    raw_db_url = raw_db_url.replace("DATABASE_URL=", "", 1).strip().strip('"').strip("'")

# Inject the async driver
if raw_db_url.startswith("postgresql://"):
    async_db_url = raw_db_url.replace("postgresql://", "postgresql+asyncpg://", 1)
elif raw_db_url.startswith("postgres://"):
    async_db_url = raw_db_url.replace("postgres://", "postgresql+asyncpg://", 1)
else:
    async_db_url = raw_db_url

# Remove the psycopg2-specific 'sslmode' argument that crashes asyncpg
async_db_url = async_db_url.replace("?sslmode=require", "").replace("&sslmode=require", "")

engine = create_async_engine(
    async_db_url, 
    echo=False, 
    connect_args={"statement_cache_size": 0} # Disables cache to bypass PgBouncer limitations
)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Setup Redis for WebSockets
redis_client = aioredis.from_url(os.getenv("REDIS_URL"))

# Setup S3 Client pointing to Supabase Storage
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('SUPABASE_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('SUPABASE_SECRET_ACCESS_KEY'),
    region_name=os.getenv('SUPABASE_S3_REGION'),
    endpoint_url=os.getenv('SUPABASE_S3_ENDPOINT')
)
BUCKET_NAME = os.getenv('SUPABASE_S3_BUCKET_NAME')

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")

async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401)
        return {"user_id": user_id, "role": payload.get("role")}
    except JWTError:
        raise HTTPException(status_code=401)

# =====================================================================
# --- 1. SQLALCHEMY ORM MODELS ---
# =====================================================================
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(UUID(as_uuid=False), primary_key=True)
    username = Column(String)
    role = Column(String)

class SupportTicket(Base):
    __tablename__ = 'support_tickets'
    id = Column(String, primary_key=True)
    buyer_id = Column(UUID(as_uuid=False))
    assigned_agent_id = Column(UUID(as_uuid=False), nullable=True)
    subject = Column(String)
    category = Column(String)
    priority = Column(String)
    status = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class TicketMessage(Base):
    __tablename__ = 'ticket_messages'
    id = Column(UUID(as_uuid=False), primary_key=True)
    ticket_id = Column(String)
    sender_id = Column(UUID(as_uuid=False))
    message = Column(String)
    is_internal_note = Column(Boolean, default=False)
    is_read = Column(Boolean, default=False)
    attachments = Column(JSONB, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)

# =====================================================================
# --- 2. REDIS PUB/SUB WEBSOCKET MANAGER ---
# =====================================================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, ticket_id: str):
        await websocket.accept()
        if ticket_id not in self.active_connections:
            self.active_connections[ticket_id] = []
        self.active_connections[ticket_id].append(websocket)

    def disconnect(self, websocket: WebSocket, ticket_id: str):
        if ticket_id in self.active_connections:
            self.active_connections[ticket_id].remove(websocket)

    async def broadcast(self, ticket_id: str, message: dict):
        await redis_client.publish(f"ticket_{ticket_id}", json.dumps(message))

manager = ConnectionManager()

# Background task to listen to Redis channels
async def redis_listener():
    pubsub = redis_client.pubsub()
    await pubsub.psubscribe("ticket_*")
    async for message in pubsub.listen():
        if message["type"] == "pmessage":
            channel = message["channel"].decode()
            ticket_id = channel.split("_")[1]
            data = json.loads(message["data"].decode())
            
            if ticket_id in manager.active_connections:
                for connection in manager.active_connections[ticket_id]:
                    try:
                        await connection.send_json(data)
                    except Exception:
                        pass

router = APIRouter()

@router.on_event("startup")
async def startup_event():
    asyncio.create_task(redis_listener())

# =====================================================================
# --- 3. PYDANTIC SCHEMAS ---
# =====================================================================
class TicketCreate(BaseModel):
    subject: str = Field(min_length=5, max_length=150)
    category: str
    priority: str = "MEDIUM"
    initial_message: str = Field(min_length=10)
    attachments: List[str] = []

class MessageCreate(BaseModel):
    message: str
    is_internal_note: bool = False
    attachments: List[str] = []

class TicketUpdate(BaseModel):
    status: Optional[str] = None
    assigned_agent_id: Optional[str] = None

class TypingUpdate(BaseModel):
    is_typing: bool

# =====================================================================
# --- 4. ROUTES ---
# =====================================================================

@router.get("/api/support/upload-url")
async def get_upload_url(file_name: str, file_type: str, user: dict = Depends(get_current_user)):
    """Generate an S3 Pre-signed URL targeted at Supabase Storage"""
    file_key = f"support/{user['user_id']}/{uuid.uuid4().hex[:8]}_{file_name}"
    try:
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': BUCKET_NAME, 'Key': file_key, 'ContentType': file_type},
            ExpiresIn=3600
        )
        # Convert the S3 endpoint to a Supabase Public URL for direct image viewing
        base_url = os.getenv('SUPABASE_S3_ENDPOINT').replace('/s3', '/object/public')
        file_url = f"{base_url}/{BUCKET_NAME}/{file_key}"
        
        return {"upload_url": presigned_url, "file_url": file_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.websocket("/api/support/tickets/{ticket_id}/ws")
async def websocket_endpoint(websocket: WebSocket, ticket_id: str, token: str, db: AsyncSession = Depends(get_db)):
    try:
        user = await get_current_user(token)
        await manager.connect(websocket, ticket_id)
        while True:
            data = await websocket.receive_json()
            if data.get("type") == "TYPING":
                await manager.broadcast(ticket_id, {
                    "type": "TYPING",
                    "user_id": user['user_id'],
                    "is_typing": data.get("is_typing", False),
                    "sender_name": "Support Team" if user.get('role', '').upper() in ['ADMIN', 'SUPPORT'] else "Buyer"
                })
            # FIX: Real-time Read Receipts
            elif data.get("type") == "MARK_READ":
                # 1. Update DB instantly
                await db.execute(
                    update(TicketMessage).where(
                        TicketMessage.ticket_id == ticket_id, 
                        TicketMessage.sender_id != user['user_id'], 
                        TicketMessage.is_read == False
                    ).values(is_read=True)
                )
                await db.commit()
                # 2. Tell the other person's browser to turn the ticks blue!
                await manager.broadcast(ticket_id, {
                    "type": "READ_RECEIPT",
                    "user_id": user['user_id']
                })
    except WebSocketDisconnect:
        manager.disconnect(websocket, ticket_id)
    except Exception:
        await websocket.close()

@router.post("/api/support/tickets")
async def create_ticket(req: TicketCreate, user: dict = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    ticket_id = f"TKT-{uuid.uuid4().hex[:8].upper()}"
    msg_id = str(uuid.uuid4())
    
    new_ticket = SupportTicket(
        id=ticket_id, buyer_id=user['user_id'], subject=req.subject, 
        category=req.category, priority=req.priority, status='OPEN'
    )
    new_message = TicketMessage(
        id=msg_id, ticket_id=ticket_id, sender_id=user['user_id'], 
        message=req.initial_message, attachments=req.attachments
    )
    
    db.add(new_ticket)
    db.add(new_message)
    await db.commit()
    return {"status": "success", "ticket_id": ticket_id}

@router.get("/api/support/tickets")
async def get_tickets(page: int = 1, limit: int = 10, status: Optional[str] = None, user: dict = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    offset = (page - 1) * limit
    user_role = str(user.get('role', '')).upper()
    
    # Subquery to calculate unread counts dynamically
    unread_subq = select(func.count()).where(
        TicketMessage.ticket_id == SupportTicket.id,
        TicketMessage.is_read == False,
        TicketMessage.sender_id != user['user_id']
    ).scalar_subquery()

    query = select(SupportTicket, User.username.label("buyer_name"), unread_subq.label("unread_count"))\
        .join(User, SupportTicket.buyer_id == User.id)

    if user_role not in ['ADMIN', 'SUPPORT']:
        query = query.where(SupportTicket.buyer_id == user['user_id'])
    if status and status != 'ALL':
        query = query.where(SupportTicket.status == status)

    # Absolute Top Sorting
    query = query.order_by(unread_subq.desc(), SupportTicket.updated_at.desc()).offset(offset).limit(limit)
    
    result = await db.execute(query)
    rows = result.fetchall()
    
    # Count Total Pagination
    count_q = select(func.count()).select_from(SupportTicket)
    if user_role not in ['ADMIN', 'SUPPORT']:
        count_q = count_q.where(SupportTicket.buyer_id == user['user_id'])
    total = (await db.execute(count_q)).scalar()

    tickets = [{
        **t.SupportTicket.__dict__, 
        "buyer_name": t.buyer_name, 
        "unread_count": t.unread_count
    } for t in rows]
    
    return {"total": total, "page": page, "limit": limit, "tickets": tickets}

@router.get("/api/support/tickets/{ticket_id}")
async def get_ticket_details(ticket_id: str, user: dict = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    user_role = str(user.get('role', '')).upper()
    
    t_query = select(SupportTicket, User.username.label("buyer_name")).join(User, SupportTicket.buyer_id == User.id).where(SupportTicket.id == ticket_id)
    t_result = (await db.execute(t_query)).first()
    
    if not t_result:
        raise HTTPException(status_code=404, detail="Ticket not found")
    
    ticket_data = {**t_result.SupportTicket.__dict__, "buyer_name": t_result.buyer_name}
    
    if user_role not in ['ADMIN', 'SUPPORT'] and ticket_data['buyer_id'] != user['user_id']:
        raise HTTPException(status_code=403, detail="Unauthorized")

    # Auto-mark as read
    await db.execute(
        update(TicketMessage).where(TicketMessage.ticket_id == ticket_id, TicketMessage.sender_id != user['user_id'], TicketMessage.is_read == False)
        .values(is_read=True)
    )
    await db.commit()

    # Fetch Messages
    m_query = select(TicketMessage, User.username.label("sender_name"), User.role.label("sender_role")).join(User, TicketMessage.sender_id == User.id).where(TicketMessage.ticket_id == ticket_id).order_by(TicketMessage.created_at.asc())
    
    if user_role not in ['ADMIN', 'SUPPORT']:
        m_query = m_query.where(TicketMessage.is_internal_note == False)
        
    m_result = await db.execute(m_query)
    messages = []
    
    for m in m_result.fetchall():
        msg_dict = {
            "id": m.TicketMessage.id,
            "ticket_id": m.TicketMessage.ticket_id,
            "sender_id": m.TicketMessage.sender_id,
            "message": m.TicketMessage.message,
            "is_internal_note": m.TicketMessage.is_internal_note,
            "is_read": m.TicketMessage.is_read,
            "attachments": m.TicketMessage.attachments or [],
            "created_at": m.TicketMessage.created_at.isoformat() + "Z",
        }
        # Enforce Professional Naming Override
        msg_dict['sender_name'] = 'Support Team' if str(m.sender_role).upper() in ['ADMIN', 'SUPPORT'] else m.sender_name
        messages.append(msg_dict)
        
    ticket_data['messages'] = messages
    return ticket_data

@router.post("/api/support/tickets/{ticket_id}/messages")
async def add_ticket_message(ticket_id: str, req: MessageCreate, user: dict = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    user_role = str(user.get('role', '')).upper()
    
    ticket = (await db.execute(select(SupportTicket).where(SupportTicket.id == ticket_id))).scalar_one_or_none()
    if not ticket:
        raise HTTPException(status_code=404)
    if ticket.status == 'CLOSED':
        raise HTTPException(status_code=400, detail="Ticket closed.")

    new_status = ticket.status if req.is_internal_note else ('OPEN' if user_role not in ['ADMIN', 'SUPPORT'] else 'WAITING_ON_CUSTOMER')
    msg_id = str(uuid.uuid4())
    
    new_message = TicketMessage(
        id=msg_id, ticket_id=ticket_id, sender_id=user['user_id'], 
        message=req.message, is_internal_note=req.is_internal_note, attachments=req.attachments
    )
    ticket.status = new_status
    ticket.updated_at = datetime.utcnow()
    
    db.add(new_message)
    await db.commit()

    # Broadcast directly through Redis
    sender_name = "Support Team" if user_role in ['ADMIN', 'SUPPORT'] else "Buyer"
    await manager.broadcast(ticket_id, {
        "type": "NEW_MESSAGE",
        "message": {
            "id": msg_id,
            "ticket_id": ticket_id,
            "sender_id": user['user_id'],
            "sender_name": sender_name,
            "sender_role": user_role,
            "message": req.message,
            "is_internal_note": req.is_internal_note,
            "is_read": False,
            "attachments": req.attachments,
            "created_at": datetime.utcnow().isoformat() + "Z"
        }
    })
    return {"status": "success"}

# Fallback HTTP Polling
@router.post("/api/support/tickets/{ticket_id}/typing")
async def update_typing(ticket_id: str, req: TypingUpdate, user: dict = Depends(get_current_user)):
    user_role = str(user.get('role', '')).upper()
    sender_name = "Support Team" if user_role in ['ADMIN', 'SUPPORT'] else "Buyer"
    
    await manager.broadcast(ticket_id, {
        "type": "TYPING",
        "user_id": user['user_id'],
        "is_typing": req.is_typing,
        "sender_name": sender_name
    })
    return {"status": "ok"}

@router.patch("/api/support/tickets/{ticket_id}")
async def update_ticket(ticket_id: str, req: TicketUpdate, user: dict = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    user_role = str(user.get('role', '')).upper()
    
    ticket = (await db.execute(select(SupportTicket).where(SupportTicket.id == ticket_id))).scalar_one_or_none()
    if not ticket:
        raise HTTPException(status_code=404)

    if user_role not in ['ADMIN', 'SUPPORT']:
        if str(ticket.buyer_id) != str(user['user_id']):
            raise HTTPException(status_code=403, detail="Unauthorized")
        if req.status and req.status != 'CLOSED':
            raise HTTPException(status_code=403, detail="Buyers can only close tickets.")
        if req.assigned_agent_id:
            raise HTTPException(status_code=403, detail="Buyers cannot assign agents.")

    if req.status: ticket.status = req.status
    if req.assigned_agent_id: 
        ticket.assigned_agent_id = req.assigned_agent_id
        if ticket.status == 'OPEN': ticket.status = 'IN_PROGRESS'
        
    ticket.updated_at = datetime.utcnow()
    
    if req.status == 'RESOLVED':
        db.add(TicketMessage(
            id=str(uuid.uuid4()), ticket_id=ticket_id, sender_id=user['user_id'], 
            message="System: Ticket marked as RESOLVED.", is_internal_note=True, attachments=[]
        ))
        
    await db.commit()
    return {"status": "success"}