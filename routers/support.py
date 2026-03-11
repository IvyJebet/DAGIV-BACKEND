import os
import uuid
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel, Field
from typing import Optional, List
from dotenv import load_dotenv

load_dotenv()

# =====================================================================
# --- 0. STANDALONE DEPENDENCIES ---
# =====================================================================
DATABASE_URL = os.getenv("DATABASE_URL")
SECRET_KEY = "DAGIV_SUPER_SECRET_KEY_CHANGE_THIS_IN_PROD"
ALGORITHM = "HS256"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/auth/login")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

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
        
    except JWTError:
        raise credentials_exception

router = APIRouter()

# Global memory cache for short-polling typing indicators
# Structure: { ticket_id: { user_id: timestamp } }
typing_cache = {}

# =====================================================================
# --- 1. PYDANTIC VALIDATION MODELS ---
# =====================================================================

class TicketCreate(BaseModel):
    subject: str = Field(min_length=5, max_length=150)
    category: str
    priority: str = "MEDIUM"
    initial_message: str = Field(min_length=10)

class MessageCreate(BaseModel):
    message: str = Field(min_length=2)
    is_internal_note: bool = False

class TicketStatusUpdate(BaseModel):
    status: str

class TicketUpdate(BaseModel):
    status: Optional[str] = None
    assigned_agent_id: Optional[str] = None

class TypingUpdate(BaseModel):
    is_typing: bool

# =====================================================================
# --- 2. API ROUTES ---
# =====================================================================

@router.post("/api/support/tickets")
def create_ticket(req: TicketCreate, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        ticket_id = f"TKT-{uuid.uuid4().hex[:8].upper()}"
        msg_id = str(uuid.uuid4())
        
        cursor.execute("""
            INSERT INTO support_tickets (id, buyer_id, subject, category, priority, status)
            VALUES (%s, %s, %s, %s, %s, 'OPEN')
        """, (ticket_id, user['user_id'], req.subject, req.category, req.priority))
        
        cursor.execute("""
            INSERT INTO ticket_messages (id, ticket_id, sender_id, message)
            VALUES (%s, %s, %s, %s)
        """, (msg_id, ticket_id, user['user_id'], req.initial_message))
        
        conn.commit()
        return {"status": "success", "ticket_id": ticket_id, "message": "Ticket created successfully."}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@router.get("/api/support/tickets")
def get_tickets(page: int = 1, limit: int = 10, status: Optional[str] = None, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        offset = (page - 1) * limit
        user_role = str(user.get('role', '')).upper()
        
        # Base queries
        count_query = "SELECT COUNT(*) as total FROM support_tickets t WHERE 1=1"
        data_query = """
            SELECT t.id, t.subject, t.category, t.status, t.priority, t.created_at, t.updated_at, u.username as buyer_name,
            (SELECT COUNT(*) FROM ticket_messages tm WHERE tm.ticket_id = t.id AND tm.is_read = FALSE AND tm.sender_id != %s) as unread_count
            FROM support_tickets t
            JOIN users u ON t.buyer_id = u.id
            WHERE 1=1
        """
        
        # Params for the data_query start with the user_id for the unread_count subquery
        data_params = [user['user_id']]
        count_params = []

        # Role-based scoping
        if user_role not in ['ADMIN', 'SUPPORT']:
            count_query += " AND t.buyer_id = %s"
            data_query += " AND t.buyer_id = %s"
            count_params.append(user['user_id'])
            data_params.append(user['user_id'])
            
        # Status filtering
        if status and status != 'ALL':
            count_query += " AND t.status = %s"
            data_query += " AND t.status = %s"
            count_params.append(status)
            data_params.append(status)

        # Absolute Top Sorting: Unread first, then mostly recently updated
        data_query += " ORDER BY unread_count DESC, t.updated_at DESC LIMIT %s OFFSET %s"
        data_params.extend([limit, offset])
        
        cursor.execute(count_query, tuple(count_params) if count_params else None)
        total_row = cursor.fetchone()
        total = total_row['total'] if total_row else 0
        
        cursor.execute(data_query, tuple(data_params))
        tickets = cursor.fetchall()
        
        return {"total": total, "page": page, "limit": limit, "tickets": tickets}
    finally:
        cursor.close()
        conn.close()


@router.get("/api/support/tickets/{ticket_id}")
def get_ticket_details(ticket_id: str, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        user_role = str(user.get('role', '')).upper()
        
        cursor.execute("""
            SELECT t.*, u.username as buyer_name 
            FROM support_tickets t
            JOIN users u ON t.buyer_id = u.id
            WHERE t.id = %s
        """, (ticket_id,))
        ticket = cursor.fetchone()
        
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found")
            
        if user_role not in ['ADMIN', 'SUPPORT'] and str(ticket['buyer_id']) != str(user['user_id']):
            raise HTTPException(status_code=403, detail="Unauthorized access to this ticket")

        # Automatically mark all messages from the OTHER party as read when viewing the thread
        cursor.execute("""
            UPDATE ticket_messages 
            SET is_read = TRUE 
            WHERE ticket_id = %s AND sender_id != %s AND is_read = FALSE
        """, (ticket_id, user['user_id']))
        conn.commit()
            
        msg_query = """
            SELECT m.id, m.message, m.is_internal_note, m.created_at, m.is_read, m.sender_id, u.username as sender_name, u.role as sender_role
            FROM ticket_messages m
            JOIN users u ON m.sender_id = u.id
            WHERE m.ticket_id = %s
        """
        
        if user_role not in ['ADMIN', 'SUPPORT']:
            msg_query += " AND m.is_internal_note = FALSE"
            
        msg_query += " ORDER BY m.created_at ASC"
        cursor.execute(msg_query, (ticket_id,))
        ticket['messages'] = cursor.fetchall()

        # Check Typing Cache for real-time polling
        is_typing = False
        if ticket_id in typing_cache:
            now = datetime.now()
            for uid, tstamp in list(typing_cache[ticket_id].items()):
                if str(uid) != str(user['user_id']) and now - tstamp < timedelta(seconds=4):
                    is_typing = True
                    
        ticket['other_party_typing'] = is_typing
        return ticket
    finally:
        cursor.close()
        conn.close()


@router.post("/api/support/tickets/{ticket_id}/messages")
def add_ticket_message(ticket_id: str, req: MessageCreate, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        user_role = str(user.get('role', '')).upper()
        
        cursor.execute("SELECT buyer_id, status FROM support_tickets WHERE id = %s", (ticket_id,))
        ticket = cursor.fetchone()
        
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found")
        if user_role not in ['ADMIN', 'SUPPORT'] and str(ticket['buyer_id']) != str(user['user_id']):
            raise HTTPException(status_code=403, detail="Unauthorized")
        if ticket['status'] == 'CLOSED':
            raise HTTPException(status_code=400, detail="Cannot reply to a closed ticket.")

        new_status = 'OPEN' if user_role not in ['ADMIN', 'SUPPORT'] else 'WAITING_ON_CUSTOMER'
        if req.is_internal_note:
            new_status = ticket['status'] 
            if user_role not in ['ADMIN', 'SUPPORT']:
                raise HTTPException(status_code=403, detail="Buyers cannot post internal notes")

        cursor.execute("""
            INSERT INTO ticket_messages (id, ticket_id, sender_id, message, is_internal_note, is_read)
            VALUES (%s, %s, %s, %s, %s, FALSE)
        """, (str(uuid.uuid4()), ticket_id, user['user_id'], req.message, req.is_internal_note))
        
        cursor.execute("UPDATE support_tickets SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s", (new_status, ticket_id))
        
        # Clear typing indicator immediately after sending
        if ticket_id in typing_cache and user['user_id'] in typing_cache[ticket_id]:
            typing_cache[ticket_id].pop(user['user_id'], None)

        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# Polling endpoint for typing indicator
@router.post("/api/support/tickets/{ticket_id}/typing")
def update_typing(ticket_id: str, req: TypingUpdate, user: dict = Depends(get_current_user)):
    if ticket_id not in typing_cache:
        typing_cache[ticket_id] = {}
        
    if req.is_typing:
        typing_cache[ticket_id][user['user_id']] = datetime.now()
    else:
        typing_cache[ticket_id].pop(user['user_id'], None)
        
    return {"status": "ok"}

@router.patch("/api/support/tickets/{ticket_id}")
def update_ticket(ticket_id: str, req: TicketUpdate, user: dict = Depends(get_current_user)):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        user_role = str(user.get('role', '')).upper()
        
        cursor.execute("SELECT buyer_id, status FROM support_tickets WHERE id = %s", (ticket_id,))
        ticket = cursor.fetchone()
        
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found")

        if user_role not in ['ADMIN', 'SUPPORT']:
            if str(ticket['buyer_id']) != str(user['user_id']):
                raise HTTPException(status_code=403, detail="Unauthorized")
            if req.status and req.status != 'CLOSED':
                raise HTTPException(status_code=403, detail="Buyers can only close tickets.")
            if req.assigned_agent_id:
                raise HTTPException(status_code=403, detail="Buyers cannot assign agents.")

        update_fields = []
        params = []
        if req.status:
            update_fields.append("status = %s")
            params.append(req.status)
        if req.assigned_agent_id:
            update_fields.append("assigned_agent_id = %s")
            params.append(req.assigned_agent_id)
            if ticket['status'] == 'OPEN':
                update_fields.append("status = 'IN_PROGRESS'")

        if not update_fields:
            return {"status": "success", "message": "No changes requested"}

        update_fields.append("updated_at = CURRENT_TIMESTAMP")
        params.append(ticket_id)
        
        query = f"UPDATE support_tickets SET {', '.join(update_fields)} WHERE id = %s"
        cursor.execute(query, tuple(params))
        
        if req.status == 'RESOLVED':
            msg = "System: Ticket marked as RESOLVED."
            cursor.execute("INSERT INTO ticket_messages (id, ticket_id, sender_id, message, is_internal_note) VALUES (%s, %s, %s, %s, TRUE)", 
                          (str(uuid.uuid4()), ticket_id, user['user_id'], msg))
                          
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()