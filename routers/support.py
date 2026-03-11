import os
import uuid
from datetime import datetime
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
        print("CREATE TICKET ERROR:", str(e))
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
        params = []
        
        # Normalize role to uppercase to completely avoid case-sensitivity bugs
        user_role = str(user.get('role', '')).upper()
        
        count_query = "SELECT COUNT(*) as total FROM support_tickets t WHERE 1=1"
        data_query = """
            SELECT t.id, t.subject, t.category, t.status, t.priority, t.created_at, t.updated_at, u.username as buyer_name
            FROM support_tickets t
            JOIN users u ON t.buyer_id = u.id
            WHERE 1=1
        """

        # Role-based scoping using the safely normalized role
        if user_role not in ['ADMIN', 'SUPPORT']:
            count_query += " AND t.buyer_id = %s"
            data_query += " AND t.buyer_id = %s"
            params.append(user['user_id'])
            
        # Status filtering
        if status and status != 'ALL':
            count_query += " AND t.status = %s"
            data_query += " AND t.status = %s"
            params.append(status)

        data_query += " ORDER BY t.updated_at DESC LIMIT %s OFFSET %s"
        
        cursor.execute(count_query, tuple(params) if params else None)
        total_row = cursor.fetchone()
        total = total_row['total'] if total_row else 0
        
        cursor.execute(data_query, tuple(params + [limit, offset]))
        tickets = cursor.fetchall()
        
        return {"total": total, "page": page, "limit": limit, "tickets": tickets}
    except Exception as e:
        print("GET TICKETS ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))
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
            
        msg_query = """
            SELECT m.id, m.message, m.is_internal_note, m.created_at, u.username as sender_name, u.role as sender_role
            FROM ticket_messages m
            JOIN users u ON m.sender_id = u.id
            WHERE m.ticket_id = %s
        """
        
        if user_role not in ['ADMIN', 'SUPPORT']:
            msg_query += " AND m.is_internal_note = FALSE"
            
        msg_query += " ORDER BY m.created_at ASC"
        cursor.execute(msg_query, (ticket_id,))
        ticket['messages'] = cursor.fetchall()
        
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
            INSERT INTO ticket_messages (id, ticket_id, sender_id, message, is_internal_note)
            VALUES (%s, %s, %s, %s, %s)
        """, (str(uuid.uuid4()), ticket_id, user['user_id'], req.message, req.is_internal_note))
        
        cursor.execute("UPDATE support_tickets SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s", (new_status, ticket_id))
        
        conn.commit()
        return {"status": "success"}
    except Exception as e:
        conn.rollback()
        print("ADD MSG ERROR:", str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


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

        # Buyers can ONLY close their own tickets. Agents can do anything.
        if user_role not in ['ADMIN', 'SUPPORT']:
            if str(ticket['buyer_id']) != str(user['user_id']):
                raise HTTPException(status_code=403, detail="Unauthorized")
            if req.status and req.status != 'CLOSED':
                raise HTTPException(status_code=403, detail="Buyers can only close tickets.")
            if req.assigned_agent_id:
                raise HTTPException(status_code=403, detail="Buyers cannot assign agents.")

        # Build dynamic update query
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
        
        # Log the action in the thread for audit history
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