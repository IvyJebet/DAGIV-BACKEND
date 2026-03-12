import os
import psycopg2
from celery import Celery
from celery.schedules import crontab
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Connect Celery to Redis broker
celery_app = Celery("sla_engine", broker="redis://localhost:6379/0")

# Schedule task to run every hour
celery_app.conf.beat_schedule = {
    "check-sla-violations-every-hour": {
        "task": "celery_worker.check_sla",
        "schedule": crontab(minute=0, hour='*'),
    },
}

@celery_app.task
def check_sla():
    """Scans for tickets OPEN for > 48 hours and escalates them."""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    try:
        forty_eight_hours_ago = datetime.utcnow() - timedelta(hours=48)
        
        # Find violations
        cursor.execute("""
            SELECT id FROM support_tickets 
            WHERE status = 'OPEN' AND updated_at < %s
        """, (forty_eight_hours_ago,))
        
        violating_tickets = cursor.fetchall()
        
        if violating_tickets:
            ticket_ids = [t[0] for t in violating_tickets]
            
            # Update status to ESCALATED
            cursor.execute("""
                UPDATE support_tickets 
                SET status = 'ESCALATED', priority = 'CRITICAL', updated_at = CURRENT_TIMESTAMP 
                WHERE id = ANY(%s)
            """, (ticket_ids,))
            
            # Auto-inject a system message (triggers real-time update on next fetch)
            for tid in ticket_ids:
                cursor.execute("""
                    INSERT INTO ticket_messages (id, ticket_id, sender_id, message, is_internal_note)
                    VALUES (gen_random_uuid(), %s, 'SYSTEM', 'SLA Violation: Ticket escalated due to 48h inactivity.', TRUE)
                """, (tid,))
            
            conn.commit()
            print(f"SLA Engine escalated {len(ticket_ids)} tickets.")
            
            # TODO: Trigger Email to Management here
            
    except Exception as e:
        conn.rollback()
        print(f"SLA Engine Error: {e}")
    finally:
        cursor.close()
        conn.close()