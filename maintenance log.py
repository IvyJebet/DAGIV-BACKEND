import psycopg2
import time
import sys
import csv
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog
from datetime import datetime, timedelta
import hashlib
import uuid
import random
import string
import threading

# --- CONFIGURATION: CLOUD DATABASE ---
# Using Connection Pooler (Port 6543)
DATABASE_URL = "postgresql://postgres.fzmydgefyoaglnroenae:sB7FRUojV1IyiGxj@aws-1-eu-west-2.pooler.supabase.com:6543/postgres?sslmode=require"

# --- 1. SMART IMPORTS (KEPT EXACTLY AS IS) ---
try:
    from tkcalendar import DateEntry, Calendar
    HAS_CALENDAR = True
except ImportError:
    HAS_CALENDAR = False
    # Fallback for DateEntry
    class DateEntry(ttk.Entry):
        def __init__(self, master, **kwargs):
            kwargs.pop('date_pattern', None)
            kwargs.pop('background', None)
            kwargs.pop('foreground', None)
            kwargs.pop('borderwidth', None)
            super().__init__(master, **kwargs)
            self.insert(0, datetime.now().strftime("%Y-%m-%d"))
        def get_date(self):
            try:
                return datetime.strptime(self.get(), "%Y-%m-%d").date()
            except ValueError:
                return datetime.now().date()
        def set_date(self, date_obj):
            self.delete(0, tk.END)
            self.insert(0, date_obj.strftime("%Y-%m-%d"))
            
    # Fallback for Calendar
    class Calendar(tk.Frame):
        def __init__(self, master, **kwargs):
            super().__init__(master)
            tk.Label(self, text="Install 'tkcalendar' for visual planner").pack()
        def calevent_remove(self, *args): pass
        def calevent_create(self, *args): pass
        def tag_config(self, *args): pass

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# --- 2. CONSTANTS & CONFIG (KEPT EXACTLY AS IS) ---
CURRENCY_LIST = [
    "KES - Kenyan Shilling", "USD - US Dollar", "EUR - Euro", "GBP - British Pound", 
    "AED - UAE Dirham", "AUD - Australian Dollar", "CAD - Canadian Dollar", 
    "CNY - Chinese Yuan", "JPY - Japanese Yen", "INR - Indian Rupee", 
    "TZS - Tanzanian Shilling", "UGX - Ugandan Shilling", "RWF - Rwandan Franc",
    "ZAR - South African Rand", "NGN - Nigerian Naira", "GHS - Ghanaian Cedi",
    "ETB - Ethiopian Birr", "SAR - Saudi Riyal", "QAR - Qatari Riyal",
    "CHF - Swiss Franc", "SEK - Swedish Krona", "NOK - Norwegian Krone",
    "DKK - Danish Krone", "SGD - Singapore Dollar", "HKD - Hong Kong Dollar",
    "NZD - New Zealand Dollar", "THB - Thai Baht", "MYR - Malaysian Ringgit",
    "IDR - Indonesian Rupiah", "PHP - Philippine Peso", "KRW - South Korean Won",
    "BRL - Brazilian Real", "RUB - Russian Ruble", "TRY - Turkish Lira",
    "MXN - Mexican Peso", "PLN - Polish Zloty", "EGP - Egyptian Pound"
]

USAGE_UNITS = ["Kilometers (km)", "Miles (mi)", "Hours (hrs)"]

SERVICE_CATEGORIES = {
    "ENGINE BAY SERVICE": [
        "Oil Filter", "Fuel Filter", "Cabin filter", "Air filter Primary/secondary", "Air filter", "Water separator",
        "Power steering pump", "Spark plugs", "Alternator", "Turbo", "Turbo seals", "Intercooler", "Tensioner", "Flywheel", "Crankshaft",
        "Radiator", "Radiator fan", "Timing belt", "Serpentine belt", "Valve cover gasket", "Oil pan gasket", "Oil pump", "Water pump",
        "Thermostat", "EGR valve", "Fuel injectors", "Fuel pump", "Glow plugs"
    ],
    "TRANSMISSION SERVICE": [
        "Gear oil #90", "Gear oil #140", "Clutch plate", "Pressure plate", "Release bearing", "Clutch booster",
        "Universal joint", "Differential seal", "Transmission mount", "Torque converter", "Clutch master cylinder", "Clutch slave cylinder"
    ],
    "CHASSIS/SUSPENSION/STEERING/BODY": [
        "Front shocks", "Rear shocks", "Coil springs", "Leaf springs", "Control arm bushes", "Ball joints", "Tie rod ends",
        "Stabilizer link", "Steering box", "Steering pump", "Body mounts", "Wiper motor", "Brake lights", "Headlights"
    ],
    "WHEELS": [
        "Wheel alignment", "Wheel balancing", "Brake disc", "Brake pads", "Brake linings", "Tires", "Rims",
        "Wheel bearings", "CV joints", "Brake calipers", "ABS sensors"
    ],
    "PNEUMATIC/HYDRAULIC/PLANT": [
        "Compressor Service", "Air dryer", "Hydraulic oil change", "Cylinder seal kits", "Hydraulic pump", "Hydraulic hoses",
        "Bucket teeth", "Track guide", "Travel motor"
    ],
    "LUBRICANTS/FLUIDS": [
        "Engine oil(10w-40)", "Engine oil(15w-40)", "Coolant", "Hydraulic oil", "Brake fluid", "Grease", "Transmission fluid"
    ]
}

MACHINERY_TYPES = ["Tipper Truck", "Excavator (Cat)", "Backhoe Loader", "Grader", "Roller / Compactor", "Bulldozer", "Crane Truck", "Water Bowser"]

# --- 4. HELPERS (KEPT EXACTLY AS IS) ---

def hash_text(s: str) -> str:
    return hashlib.sha256((s or "").encode()).hexdigest()

def gen_code(n=6) -> str:
    return "".join(random.choices(string.digits, k=n))

def send_verification_code(receiver_email: str, receiver_phone: str, code: str) -> bool:
    return False 

def send_bill_sms_threaded(phone, message, callback=None):
    def task():
        time.sleep(1.5) 
        print(f"\n[SMS SENT (Threaded)]\nTo: {phone}\nMessage: {message}\n-------------------")
        if callback: callback()
    threading.Thread(target=task, daemon=True).start()

def calculate_daily_usage(vehicle_reg):
    # Updated to use Postgres Connection
    try:
        conn = connect_db()
        if not conn: return 50.0
        cur = conn.cursor()
        # SQL: %s instead of ?
        cur.execute("SELECT service_date, mileage FROM service_logs WHERE vehicle=%s ORDER BY service_date DESC LIMIT 3", (vehicle_reg,))
        rows = cur.fetchall()
        conn.close()

        if len(rows) < 2: return 50.0 

        total_days = 0
        total_val = 0
        
        for i in range(len(rows) - 1):
            try:
                d1 = datetime.strptime(rows[i][0], "%Y-%m-%d")
                m1 = float(rows[i][1])
                d2 = datetime.strptime(rows[i+1][0], "%Y-%m-%d")
                m2 = float(rows[i+1][1])
                days = (d1 - d2).days
                val = m1 - m2
                if days > 0 and val > 0:
                    total_days += days; total_val += val
            except: continue
        if total_days == 0: return 50.0
        return max(total_val / total_days, 5.0)
    except: return 50.0

# --- 5. DATABASE UTILS (UPDATED FOR SUPABASE) ---

def connect_db():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"DB Error: {e}")
        return None

def initialize_db():
    conn = connect_db()
    if not conn:
        messagebox.showerror("Error", "No Internet Connection to Cloud DB")
        sys.exit(1)
        
    cursor = conn.cursor()

    # Tables updated for Postgres (SERIAL instead of AUTOINCREMENT)
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
        CREATE TABLE IF NOT EXISTS expiry_alerts (
            vehicle TEXT PRIMARY KEY,
            insurance_expiry TEXT,
            inspection_expiry TEXT,
            speed_governor_expiry TEXT,
            last_alert_date TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT,
            role TEXT,
            email TEXT,
            phone TEXT,
            security_question TEXT,
            security_answer_hash TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS parts_inventory (
            id SERIAL PRIMARY KEY,
            part_name TEXT,
            serial_number TEXT UNIQUE,
            supplier TEXT,
            date_added TEXT,
            status TEXT DEFAULT 'In Stock'
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logistics_log (
            id SERIAL PRIMARY KEY,
            client_name TEXT,
            client_phone TEXT,
            machine_type TEXT,
            reg_number TEXT,
            site_location TEXT,
            driver_name TEXT,
            start_date TEXT,
            rate_type TEXT, 
            rate_amount REAL,
            status TEXT DEFAULT 'Active',
            end_date TEXT,
            total_usage REAL,
            total_cost REAL,
            currency TEXT
        )
    """)
    
    # NEW TABLE FOR WEB REQUESTS
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
    
    # Default Admin
    cursor.execute("SELECT COUNT(*) FROM users")
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
            INSERT INTO users (username, password, role, email, phone, security_question, security_answer_hash) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, ("admin", hash_text("admin"), "admin", "admin@empire.co.ke", "0700000000", "Default Question", hash_text("admin")))
    
    conn.commit()
    conn.close()

# --- 6. MAIN APPLICATION GUI ---

def run_main_app(username, role):
    app = tk.Tk()
    app.title(f"DAGIV ENGINEERING ERP - {username} ({role})")
    app.state('zoomed')

    style = ttk.Style()
    style.theme_use('clam')
    style.configure("Treeview", rowheight=25, font=('Arial', 10))
    style.configure("Treeview.Heading", font=('Arial', 10, 'bold'))
    style.configure("TButton", font=('Arial', 10, 'bold'))

    notebook = ttk.Notebook(app)
    notebook.pack(fill="both", expand=True, padx=5, pady=5)

    tab_planner = ttk.Frame(notebook)
    notebook.add(tab_planner, text="📅 Smart Alerts")

    # --- NEW: WEB REQUESTS TAB ---
    tab_requests = ttk.Frame(notebook)
    notebook.add(tab_requests, text="🌍 Web Requests")

    tab_service = ttk.Frame(notebook)
    tab_logistics = ttk.Frame(notebook)
    tab_parts = ttk.Frame(notebook) 
    tab_expiry = ttk.Frame(notebook)
    tab_account = ttk.Frame(notebook)

    notebook.add(tab_service, text="🔧 Service Logs")
    notebook.add(tab_logistics, text="🚚 Logistics & Leasing")
    notebook.add(tab_parts, text="⚙️ Genuine Parts Store")
    notebook.add(tab_expiry, text="📅 Compliance & Expiry")
    notebook.add(tab_account, text="👤 Account & Users")

    # ==========================================
    # TAB: WEB REQUESTS LOGIC (THREADED)
    # ==========================================
    req_cols = ("ID", "Type", "Contact", "Phone", "Date", "Status")
    req_tree = ttk.Treeview(tab_requests, columns=req_cols, show="headings")
    for c in req_cols: 
        req_tree.heading(c, text=c)
        req_tree.column(c, width=100)
    req_tree.pack(fill="both", expand=True, padx=10, pady=10)

    def mark_request_done():
        sel = req_tree.selection()
        if not sel: return
        item_id = req_tree.item(sel[0])['values'][0]
        try:
            conn = connect_db(); c = conn.cursor()
            c.execute("UPDATE inspection_requests SET status='Completed' WHERE id=%s", (item_id,))
            conn.commit(); conn.close()
            # Force immediate refresh
            threading.Thread(target=fetch_web_data, daemon=True).start()
        except Exception as e:
            messagebox.showerror("Error", str(e))

    req_btn_frame = ttk.Frame(tab_requests)
    req_btn_frame.pack(fill="x", padx=10, pady=5)
    tk.Button(req_btn_frame, text="✅ Mark as Contacted/Done", bg="green", fg="white", command=mark_request_done).pack(side="left")

    def fetch_web_data():
        """Runs in background thread to prevent buffering"""
        try:
            conn = connect_db()
            if not conn: return
            cur = conn.cursor()
            cur.execute("SELECT id, machine_type, contact_person, phone, date, status FROM inspection_requests WHERE status='Pending'")
            rows = cur.fetchall()
            conn.close()
            app.after(0, update_web_ui, rows)
        except: pass
        
    def update_web_ui(rows):
        for i in req_tree.get_children(): req_tree.delete(i)
        for row in rows: req_tree.insert("", "end", values=row)
        app.after(5000, lambda: threading.Thread(target=fetch_web_data, daemon=True).start())

    # Start loop
    threading.Thread(target=fetch_web_data, daemon=True).start()

    # ==========================================
    # TAB 0: SMART PLANNER (INTERACTIVE)
    # ==========================================
    planner_frame = ttk.Frame(tab_planner)
    planner_frame.pack(fill="both", expand=True, padx=10, pady=10)

    # 1. CONTROL BAR
    ctrl_frame = ttk.Frame(planner_frame); ctrl_frame.pack(fill="x", pady=5)
    view_filter_var = tk.StringVar(value="All")
    def apply_filter(): refresh_smart_planner()
    
    ttk.Label(ctrl_frame, text="FILTER:", font=("Arial", 10, "bold")).pack(side="left")
    for txt, val in [("🌍 All", "All"), ("🔧 Mechanical", "Mechanical"), ("📄 Compliance", "Compliance")]:
        ttk.Radiobutton(ctrl_frame, text=txt, variable=view_filter_var, value=val, command=apply_filter).pack(side="left", padx=10)
    tk.Button(ctrl_frame, text="🔄 Refresh", command=apply_filter).pack(side="right")

    # 2. SPLIT VIEW
    paned = ttk.PanedWindow(planner_frame, orient="horizontal"); paned.pack(fill="both", expand=True)
    
    def show_vehicle_details(vehicle_reg):
        conn = connect_db(); cur = conn.cursor()
        cur.execute("SELECT service_date, mileage, next_service, remarks FROM service_logs WHERE vehicle=%s ORDER BY id DESC LIMIT 1", (vehicle_reg,))
        mech_row = cur.fetchone()
        cur.execute("SELECT insurance_expiry, inspection_expiry, speed_governor_expiry FROM expiry_alerts WHERE vehicle=%s", (vehicle_reg,))
        comp_row = cur.fetchone()
        conn.close()

        pop = tk.Toplevel(app)
        pop.title(f"Vehicle Card: {vehicle_reg}")
        pop.geometry("400x350")
        ttk.Label(pop, text=f"Vehicle: {vehicle_reg}", font=("Arial", 14, "bold")).pack(pady=10)
        f = ttk.Frame(pop, padding=15); f.pack(fill="both", expand=True)
        ttk.Label(f, text="🔧 Mechanical Status", font=("Arial", 10, "bold")).grid(row=0, column=0, sticky="w", pady=(0,5))
        if mech_row:
            ttk.Label(f, text=f"Last Service Date:").grid(row=1, column=0, sticky="w")
            ttk.Label(f, text=mech_row[0], foreground="blue").grid(row=1, column=1, sticky="w")
            ttk.Label(f, text=f"Next Service Due:").grid(row=3, column=0, sticky="w")
            ttk.Label(f, text=str(mech_row[2]), foreground="red").grid(row=3, column=1, sticky="w")
            ttk.Label(f, text=f"Last Remarks:").grid(row=4, column=0, sticky="w")
            ttk.Label(f, text=mech_row[3]).grid(row=4, column=1, sticky="w")
        else:
            ttk.Label(f, text="No service history recorded.").grid(row=1, column=0, columnspan=2, sticky="w")
        ttk.Separator(f, orient="horizontal").grid(row=5, column=0, columnspan=2, sticky="ew", pady=15)
        ttk.Label(f, text="📄 Compliance Status", font=("Arial", 10, "bold")).grid(row=6, column=0, sticky="w", pady=(0,5))
        if comp_row:
            labels = ["Insurance Expiry", "Inspection Expiry", "Speed Gov Expiry"]
            for i, val in enumerate(comp_row):
                ttk.Label(f, text=f"{labels[i]}:").grid(row=7+i, column=0, sticky="w")
                fg = "green"
                if val:
                    try:
                        if datetime.strptime(val, "%Y-%m-%d").date() < datetime.now().date(): fg = "red"
                    except: pass
                ttk.Label(f, text=val or "N/A", foreground=fg).grid(row=7+i, column=1, sticky="w")
        else:
            ttk.Label(f, text="No compliance data recorded.").grid(row=7, column=0, columnspan=2, sticky="w")
        tk.Button(pop, text="Close", command=pop.destroy).pack(pady=10)

    p_left = ttk.LabelFrame(paned, text="Calendar (Click dates)", padding=5); paned.add(p_left, weight=1)
    
    def on_calendar_select(event):
        if not HAS_CALENDAR: return
        selected_date = planner_cal.selection_get()
        event_ids = planner_cal.get_calevents(date=selected_date)
        urgent_vehicles = []
        for ev_id in event_ids:
            tags = planner_cal.calevent_cget(ev_id, "tags")
            if "red" in tags or "yellow" in tags:
                text_content = planner_cal.calevent_cget(ev_id, "text")
                veh_reg = text_content.split(" - ")[0] 
                urgent_vehicles.append(veh_reg)
        if not urgent_vehicles: messagebox.showinfo("Smart Planner", "Nothing to show for this date.")
        else: show_vehicle_details(urgent_vehicles[0])

    if HAS_CALENDAR:
        planner_cal = Calendar(p_left, selectmode='day', date_pattern='yyyy-mm-dd')
        planner_cal.pack(fill="both", expand=True)
        planner_cal.tag_config('red', background='red', foreground='white')
        planner_cal.tag_config('yellow', background='orange', foreground='black')
        planner_cal.tag_config('green', background='green', foreground='white')
        planner_cal.bind("<<CalendarSelected>>", on_calendar_select)
    else:
        tk.Label(p_left, text="Install 'tkcalendar'").pack()
    
    p_right = ttk.LabelFrame(paned, text="Action Items List", padding=5); paned.add(p_right, weight=1)
    cols_plan = ("Vehicle", "Category", "Urgency", "Action Item", "Due Date")
    tree_plan = ttk.Treeview(p_right, columns=cols_plan, show="headings")
    for c in cols_plan: tree_plan.heading(c, text=c)
    tree_plan.column("Vehicle", width=90); tree_plan.column("Category", width=80)
    tree_plan.column("Urgency", width=60); tree_plan.column("Action Item", width=200)
    tree_plan.pack(fill="both", expand=True)
    tree_plan.tag_configure('RED', background='#ffcccc')
    tree_plan.tag_configure('YELLOW', background='#fff3cd')
    tree_plan.tag_configure('GREEN', background='#d4edda')

    def on_list_double_click(event):
        sel = tree_plan.selection()
        if sel:
            item = tree_plan.item(sel[0])['values']
            if item: show_vehicle_details(item[0])
    tree_plan.bind("<Double-1>", on_list_double_click)

    def refresh_smart_planner():
        for i in tree_plan.get_children(): tree_plan.delete(i)
        if HAS_CALENDAR: planner_cal.calevent_remove('all')
        
        def task():
            conn = connect_db(); cur = conn.cursor()
            results = []
            today = datetime.now().date()
            f_mode = view_filter_var.get()

            if f_mode in ["All", "Mechanical"]:
                # Fetch all logs (Simplified for Postgres Migration)
                cur.execute("SELECT vehicle, next_service, mileage, usage_unit FROM service_logs")
                all_rows = cur.fetchall()
                # Sort in python to get latest per vehicle
                latest_logs = {} 
                for r in all_rows: 
                    # Assuming higher ID is later, or we process all. 
                    # For stability: just process every unique vehicle found
                    latest_logs[r[0]] = r 
                
                # Better: Query Distinct Vehicles
                cur.execute("SELECT DISTINCT vehicle FROM service_logs")
                vehs = cur.fetchall()
                for v_tuple in vehs:
                    veh = v_tuple[0]
                    cur.execute("SELECT next_service, mileage, usage_unit FROM service_logs WHERE vehicle=%s ORDER BY id DESC LIMIT 1", (veh,))
                    last = cur.fetchone()
                    if last:
                        next_v, curr_v, unit = last
                        daily = calculate_daily_usage(veh)
                        if not next_v: next_v = (curr_v or 0) + 5000
                        rem = next_v - (curr_v or 0)
                        try: days = int(rem/daily)
                        except: days = 30
                        pred_date = today + timedelta(days=days)
                        urg, msg = "GREEN", "Healthy"
                        unit_s = unit if unit else "km"
                        if rem < 500: urg, msg = "RED", f"Service Due ({int(rem)}{unit_s})"
                        elif rem < 2000: urg, msg = "YELLOW", f"Upcoming ({int(rem)}{unit_s})"
                        results.append({"v": veh, "c": "🔧 Mech", "u": urg, "a": msg, "d": pred_date})

            if f_mode in ["All", "Compliance"]:
                cur.execute("SELECT vehicle, insurance_expiry, inspection_expiry, speed_governor_expiry FROM expiry_alerts")
                for row in cur.fetchall():
                    veh, ins, insp, spd = row
                    checks = [("Ins", ins, "Insurance"), ("Insp", insp, "Inspection"), ("Spd", spd, "Speed Gov")]
                    for tag, d_str, full in checks:
                        if d_str:
                            try:
                                d_obj = datetime.strptime(d_str, "%Y-%m-%d").date()
                                days = (d_obj - today).days
                                urg, msg = "GREEN", f"{tag} OK"
                                if days < 0: urg, msg = "RED", f"⚠️ {full} EXPIRED"
                                elif days < 14: urg, msg = "RED", f"{full} Expiring ({days}d)"
                                elif days < 45: urg, msg = "YELLOW", f"{full} Renewal Soon"
                                if urg != "GREEN" or f_mode == "Compliance":
                                    results.append({"v": veh, "c": "📄 Doc", "u": urg, "a": msg, "d": d_obj})
                            except: pass
            conn.close()

            def update():
                results.sort(key=lambda x: x['d'])
                for r in results:
                    tree_plan.insert("", "end", values=(r['v'], r['c'], r['u'], r['a'], r['d'].strftime("%Y-%m-%d")), tags=(r['u'],))
                    if HAS_CALENDAR and r['u'] in ['RED', 'YELLOW']:
                        planner_cal.calevent_create(r['d'], f"{r['v']} - {r['a']}", r['u'].lower())
            app.after(0, update)
        threading.Thread(target=task, daemon=True).start()
    app.after(1000, refresh_smart_planner)

    # ==========================================
    # TAB 1: SERVICE LOGS
    # ==========================================
    input_frame = ttk.LabelFrame(tab_service, text="Log New Service / Edit", padding=10)
    input_frame.pack(side="left", fill="y", padx=10, pady=10)
    edit_id_var = tk.StringVar()

    ttk.Label(input_frame, text="Vehicle Reg:").grid(row=0, column=0, sticky="w", pady=5)
    vehicle_ent = ttk.Entry(input_frame, width=30); vehicle_ent.grid(row=0, column=1, pady=5)

    ttk.Label(input_frame, text="Service Category:").grid(row=1, column=0, sticky="w", pady=5)
    cat_combo = ttk.Combobox(input_frame, values=list(SERVICE_CATEGORIES.keys()), state="readonly", width=28)
    cat_combo.grid(row=1, column=1, pady=5)

    ttk.Label(input_frame, text="Service Item:").grid(row=2, column=0, sticky="w", pady=5)
    item_combo = ttk.Combobox(input_frame, state="readonly", width=28)
    item_combo.grid(row=2, column=1, pady=5)

    def update_items(event):
        selected_cat = cat_combo.get()
        new_values = SERVICE_CATEGORIES.get(selected_cat, [])
        item_combo.config(values=new_values)
        item_combo.set('')
    cat_combo.bind('<<ComboboxSelected>>', update_items)

    ttk.Label(input_frame, text="Date:").grid(row=3, column=0, sticky="w", pady=5)
    date_ent = DateEntry(input_frame, width=28, date_pattern="yyyy-mm-dd"); date_ent.grid(row=3, column=1, pady=5)

    # --- UPDATED COST & CURRENCY ---
    ttk.Label(input_frame, text="Cost:").grid(row=4, column=0, sticky="w", pady=5)
    cost_frame = ttk.Frame(input_frame)
    cost_frame.grid(row=4, column=1, pady=5, sticky="w")
    cost_ent = ttk.Entry(cost_frame, width=15); cost_ent.pack(side="left")
    currency_combo = ttk.Combobox(cost_frame, values=CURRENCY_LIST, state="readonly", width=12)
    currency_combo.set("KES - Kenyan Shilling")
    currency_combo.pack(side="left", padx=5)

    # --- UPDATED USAGE UNITS ---
    ttk.Label(input_frame, text="Current Usage:").grid(row=5, column=0, sticky="w", pady=5)
    usage_frame = ttk.Frame(input_frame)
    usage_frame.grid(row=5, column=1, pady=5, sticky="w")
    mileage_ent = ttk.Entry(usage_frame, width=15); mileage_ent.pack(side="left")
    unit_combo = ttk.Combobox(usage_frame, values=USAGE_UNITS, state="readonly", width=12)
    unit_combo.set("Kilometers (km)")
    unit_combo.pack(side="left", padx=5)

    ttk.Label(input_frame, text="Next Service:").grid(row=6, column=0, sticky="w", pady=5)
    next_frame = ttk.Frame(input_frame)
    next_frame.grid(row=6, column=1, pady=5, sticky="w")
    next_ent = ttk.Entry(next_frame, width=15); next_ent.pack(side="left")
    lbl_next_unit = ttk.Label(next_frame, text=" (Unit matches above)"); lbl_next_unit.pack(side="left")

    ttk.Label(input_frame, text="Remarks:").grid(row=7, column=0, sticky="w", pady=5)
    remarks_ent = ttk.Entry(input_frame, width=30); remarks_ent.grid(row=7, column=1, pady=5)

    # Expiry
    ttk.Separator(input_frame, orient="horizontal").grid(row=8, column=0, columnspan=2, sticky="ew", pady=10)
    ttk.Label(input_frame, text="Update Compliance Dates", font=("Arial", 9, "bold")).grid(row=9, column=0, columnspan=2)
    ins_var = tk.BooleanVar(); insp_var = tk.BooleanVar(); spd_var = tk.BooleanVar()
    ttk.Checkbutton(input_frame, text="Insurance Expiry", variable=ins_var).grid(row=10, column=0, sticky="w")
    ins_date = DateEntry(input_frame, width=15, date_pattern="yyyy-mm-dd"); ins_date.grid(row=10, column=1, sticky="w")
    ttk.Checkbutton(input_frame, text="Inspection Expiry", variable=insp_var).grid(row=11, column=0, sticky="w")
    insp_date = DateEntry(input_frame, width=15, date_pattern="yyyy-mm-dd"); insp_date.grid(row=11, column=1, sticky="w")
    ttk.Checkbutton(input_frame, text="Speed Governor Expiry", variable=spd_var).grid(row=12, column=0, sticky="w")
    spd_date = DateEntry(input_frame, width=15, date_pattern="yyyy-mm-dd"); spd_date.grid(row=12, column=1, sticky="w")

    tree_frame = ttk.Frame(tab_service)
    tree_frame.pack(side="right", fill="both", expand=True, padx=10, pady=10)
    cols = ("ID", "Vehicle", "Service", "Date", "Cost", "Curr", "Cost", "Usage", "Unit", "Next Due")
    tree = ttk.Treeview(tree_frame, columns=cols, show="headings")
    tree.heading("ID", text="ID"); tree.column("ID", width=30)
    tree.heading("Vehicle", text="Vehicle"); tree.column("Vehicle", width=80)
    tree.heading("Service", text="Service"); tree.column("Service", width=120)
    tree.heading("Date", text="Date"); tree.column("Date", width=80)
    tree.heading("Cost", text="Cost"); tree.column("Cost", width=60)
    tree.heading("Curr", text="Curr"); tree.column("Curr", width=40)
    tree.heading("Usage", text="Usage"); tree.column("Usage", width=70)
    tree.heading("Unit", text="Unit"); tree.column("Unit", width=60)
    tree.heading("Next Due", text="Next Due"); tree.column("Next Due", width=70)
    tree.pack(fill="both", expand=True)

    def clear_form():
        vehicle_ent.delete(0, tk.END); cat_combo.set(''); item_combo.set(''); cost_ent.delete(0, tk.END)
        mileage_ent.delete(0, tk.END); next_ent.delete(0, tk.END); remarks_ent.delete(0, tk.END)
        ins_var.set(False); insp_var.set(False); spd_var.set(False); edit_id_var.set("")
        item_combo.config(values=[]); btn_save.config(text="Save Entry")

    def refresh_tree():
        def fetch():
            try:
                conn = connect_db()
                if not conn: return
                cur = conn.cursor()
                cur.execute("SELECT id, vehicle, service_type, service_date, cost, currency, mileage, usage_unit, next_service FROM service_logs ORDER BY id DESC")
                rows = cur.fetchall()
                conn.close()
                app.after(0, update_ui, rows)
            except: pass
        
        def update_ui(rows):
            for i in tree.get_children(): tree.delete(i)
            for row in rows: tree.insert("", "end", values=row)
            app.after(10000, lambda: threading.Thread(target=fetch, daemon=True).start()) # Auto refresh logs every 10s

        threading.Thread(target=fetch, daemon=True).start()

    # Start log refresh
    refresh_tree()

    def save_entry():
        veh = vehicle_ent.get(); svc = item_combo.get()
        if not veh or not svc: messagebox.showerror("Error", "Vehicle and Service required"); return
        
        conn = connect_db(); cur = conn.cursor()
        curr = currency_combo.get().split(" ")[0]; unit = unit_combo.get()
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        try:
            if edit_id_var.get():
                cur.execute("UPDATE service_logs SET vehicle=%s, service_type=%s, service_date=%s, cost=%s, currency=%s, mileage=%s, usage_unit=%s, next_service=%s, remarks=%s WHERE id=%s",
                           (veh, svc, date_ent.get(), cost_ent.get(), curr, mileage_ent.get(), unit, next_ent.get(), remarks_ent.get(), edit_id_var.get()))
                msg = "Updated"
            else:
                cur.execute("INSERT INTO service_logs (vehicle, service_type, service_date, cost, currency, mileage, usage_unit, next_service, remarks) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                           (veh, svc, date_ent.get(), cost_ent.get(), curr, mileage_ent.get(), unit, next_ent.get(), remarks_ent.get()))
                msg = "Saved"
            
            if ins_var.get() or insp_var.get() or spd_var.get():
                cur.execute("INSERT INTO expiry_alerts (vehicle) VALUES (%s) ON CONFLICT (vehicle) DO NOTHING", (veh,))
                if ins_var.get(): cur.execute("UPDATE expiry_alerts SET insurance_expiry=%s, last_alert_date=%s WHERE vehicle=%s", (ins_date.get(), today_str, veh))
                if insp_var.get(): cur.execute("UPDATE expiry_alerts SET inspection_expiry=%s, last_alert_date=%s WHERE vehicle=%s", (insp_date.get(), today_str, veh))
                if spd_var.get(): cur.execute("UPDATE expiry_alerts SET speed_governor_expiry=%s, last_alert_date=%s WHERE vehicle=%s", (spd_date.get(), today_str, veh))
            
            conn.commit()
            messagebox.showinfo("Success", msg)
            clear_form()
            # Force immediate fetch
            threading.Thread(target=refresh_smart_planner, daemon=True).start()
        except Exception as e: messagebox.showerror("Database Error", str(e))
        finally: conn.close()

    def load_for_edit():
        sel = tree.selection()
        if not sel: return
        item = tree.item(sel[0])['values']
        clear_form(); edit_id_var.set(item[0])
        vehicle_ent.insert(0, item[1])
        try:
            svc_text = item[2]; item_combo.set(svc_text)
            date_ent.set_date(datetime.strptime(item[3], "%Y-%m-%d").date())
            cost_ent.insert(0, item[4]); mileage_ent.insert(0, item[6]); next_ent.insert(0, item[8])
        except: pass
        btn_save.config(text="Update Record")

    def delete_entry():
        if role != "admin": messagebox.showerror("Denied", "Admin only"); return
        sel = tree.selection()
        if sel and messagebox.askyesno("Confirm", "Delete?"):
            conn = connect_db(); conn.execute("DELETE FROM service_logs WHERE id=%s", (tree.item(sel[0])['values'][0],))
            conn.commit(); conn.close(); clear_form()

    def export_to_excel():
        conn = connect_db()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM service_logs ORDER BY id DESC")
            rows = cursor.fetchall()
            col_names = ["ID", "Vehicle", "Service", "Date", "Cost", "Remarks", "Mileage", "Hours", "Next Service", "Group ID", "Currency", "Unit"]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_name = f"ServiceLogs_{timestamp}"
            
            if HAS_PANDAS:
                file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", initialfile=default_name, filetypes=[("Excel files", "*.xlsx")])
                if file_path:
                    df = pd.DataFrame(rows, columns=col_names)
                    df.to_excel(file_path, index=False)
                    messagebox.showinfo("Success", f"Exported to {file_path}")
            else:
                file_path = filedialog.asksaveasfilename(defaultextension=".csv", initialfile=default_name, filetypes=[("CSV files", "*.csv")])
                if file_path:
                    with open(file_path, "w", newline="") as f:
                        writer = csv.writer(f); writer.writerow(col_names); writer.writerows(rows)
                    messagebox.showinfo("Success", f"Exported to {file_path}")
        except Exception as e: messagebox.showerror("Export Error", str(e))
        finally: conn.close()

    def verify_part_real():
        code = simpledialog.askstring("Verify", "Scan Serial Number:")
        if not code: return
        conn = connect_db(); cur = conn.cursor()
        cur.execute("SELECT status, part_name, supplier FROM parts_inventory WHERE serial_number=%s", (code,))
        res = cur.fetchone(); conn.close()
        if res:
            if res[0] == "In Stock":
                if messagebox.askyesno("Verified", f"✅ GENUINE: {res[1]}\nInstall now?"):
                    c2 = connect_db(); c2.execute("UPDATE parts_inventory SET status='Installed' WHERE serial_number=%s", (code,)); c2.commit(); c2.close()
                    remarks_ent.insert(tk.END, f" [Installed {res[1]} #{code}]"); refresh_inventory()
            else: messagebox.showwarning("Used", "Part already installed!")
        else: messagebox.showerror("Counterfeit", "❌ Serial not found!")

    btn_frame = ttk.Frame(input_frame); btn_frame.grid(row=13, column=0, columnspan=2, pady=15)
    btn_save = tk.Button(btn_frame, text="Save Entry", bg="#28a745", fg="white", command=save_entry, width=15); btn_save.pack(side="left", padx=2)
    tk.Button(btn_frame, text="Edit Entry", bg="#ffc107", command=load_for_edit, width=8).pack(side="left", padx=2)
    tk.Button(btn_frame, text="Delete ", bg="#dc3545", fg="white", command=delete_entry, width=8).pack(side="left", padx=2)
    tk.Button(btn_frame, text="📊 Export", bg="#17a2b8", fg="white", command=export_to_excel, width=12).pack(side="left", padx=2)
    tk.Button(input_frame, text="🛡️ Verify Genuine Part", bg="#007bff", fg="white", command=verify_part_real).grid(row=14, column=0, columnspan=2, sticky="ew", pady=5)

    # ==========================================
    # TAB 2: LOGISTICS & LEASING
    # ==========================================
    log_left = ttk.LabelFrame(tab_logistics, text="🚜 Dispatch Machine", padding=10)
    log_left.pack(side="left", fill="y", padx=10, pady=10)
    log_right_container = ttk.Frame(tab_logistics)
    log_right_container.pack(side="right", fill="both", expand=True, padx=10, pady=10)
    filter_frame = ttk.Frame(log_right_container); filter_frame.pack(fill="x", pady=5)
    log_view_var = tk.StringVar(value="Active")
    def toggle_log_view(): refresh_logistics()
    ttk.Radiobutton(filter_frame, text="Active Leases", variable=log_view_var, value="Active", command=toggle_log_view).pack(side="left", padx=10)
    ttk.Radiobutton(filter_frame, text="Billing History", variable=log_view_var, value="History", command=toggle_log_view).pack(side="left", padx=10)
    log_right = ttk.LabelFrame(log_right_container, text="📋 Fleet Management", padding=10); log_right.pack(fill="both", expand=True)

    ttk.Label(log_left, text="Client Name:").grid(row=0, column=0, sticky="w", pady=5)
    l_client = ttk.Entry(log_left, width=30); l_client.grid(row=0, column=1, pady=5)
    ttk.Label(log_left, text="Phone:").grid(row=1, column=0, sticky="w", pady=5)
    l_phone = ttk.Entry(log_left, width=30); l_phone.grid(row=1, column=1, pady=5)
    ttk.Label(log_left, text="Machine Type:").grid(row=2, column=0, sticky="w", pady=5)
    l_mach = ttk.Combobox(log_left, values=MACHINERY_TYPES, state="readonly", width=28); l_mach.grid(row=2, column=1, pady=5)
    ttk.Label(log_left, text="Reg/Fleet No:").grid(row=3, column=0, sticky="w", pady=5)
    l_reg = ttk.Entry(log_left, width=30); l_reg.grid(row=3, column=1, pady=5)
    ttk.Label(log_left, text="Site Location:").grid(row=4, column=0, sticky="w", pady=5)
    l_site = ttk.Entry(log_left, width=30); l_site.grid(row=4, column=1, pady=5)
    ttk.Label(log_left, text="Operator/Driver:").grid(row=5, column=0, sticky="w", pady=5)
    l_driver = ttk.Entry(log_left, width=30); l_driver.grid(row=5, column=1, pady=5)
    ttk.Label(log_left, text="Start Date:").grid(row=6, column=0, sticky="w", pady=5)
    l_start = DateEntry(log_left, width=28, date_pattern="yyyy-mm-dd"); l_start.grid(row=6, column=1, pady=5)
    ttk.Separator(log_left, orient="horizontal").grid(row=7, column=0, columnspan=2, sticky="ew", pady=10)
    ttk.Label(log_left, text="Rate Type:").grid(row=8, column=0, sticky="w", pady=5)
    l_rate_type = ttk.Combobox(log_left, values=["Per Day (Dry)", "Per Day (Wet)", "Per Hour"], state="readonly", width=28); l_rate_type.grid(row=8, column=1, pady=5)
    l_rate_type.set("Per Day (Dry)")
    ttk.Label(log_left, text="Rate Amount:").grid(row=9, column=0, sticky="w", pady=5)
    l_rate_frame = ttk.Frame(log_left); l_rate_frame.grid(row=9, column=1, pady=5, sticky="w")
    l_amount = ttk.Entry(l_rate_frame, width=15); l_amount.pack(side="left")
    l_curr_combo = ttk.Combobox(l_rate_frame, values=CURRENCY_LIST, state="readonly", width=12); l_curr_combo.set("KES - Kenyan Shilling"); l_curr_combo.pack(side="left", padx=5)

    def refresh_logistics():
        for i in log_tree.get_children(): log_tree.delete(i)
        conn = connect_db(); cur = conn.cursor()
        if log_view_var.get() == "Active":
            cur.execute("SELECT id, client_name, machine_type, reg_number, site_location, start_date, status, '', '', currency FROM logistics_log WHERE status='Active'")
        else:
            cur.execute("SELECT id, client_name, machine_type, reg_number, site_location, start_date, status, end_date, total_cost, currency FROM logistics_log WHERE status='Returned' ORDER BY id DESC")
        for row in cur.fetchall(): 
            vals = list(row); curr = vals.pop(); vals.append(curr) 
            log_tree.insert("", "end", values=vals)
        conn.close()

    def dispatch_machine():
        c_name = l_client.get(); c_mach = l_mach.get(); c_reg = l_reg.get()
        curr = l_curr_combo.get().split(" ")[0]
        if not c_name or not c_mach or not c_reg: messagebox.showerror("Error", "Required fields missing."); return
        try: rate = float(l_amount.get())
        except: messagebox.showerror("Error", "Invalid Rate"); return
        conn = connect_db(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO logistics_log (client_name, client_phone, machine_type, reg_number, site_location, driver_name, start_date, rate_type, rate_amount, status, currency)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'Active', %s)
        """, (c_name, l_phone.get(), c_mach, c_reg, l_site.get(), l_driver.get(), l_start.get(), l_rate_type.get(), rate, curr))
        conn.commit(); conn.close(); messagebox.showinfo("Success", f"{c_mach} Dispatched"); refresh_logistics()
        l_client.delete(0, tk.END); l_reg.delete(0, tk.END); l_site.delete(0, tk.END); l_amount.delete(0, tk.END)

    def return_machine():
        sel = log_tree.selection()
        if not sel: messagebox.showwarning("Select", "Select a machine."); return
        item = log_tree.item(sel[0])['values']; log_id = item[0]
        if item[6] == 'Returned': messagebox.showinfo("Info", "Already returned."); return
        start_str = item[5]
        ret_window = tk.Toplevel(app); ret_window.title("Return Machine")
        tk.Label(ret_window, text="End Date:").grid(row=0, column=0, padx=10, pady=10)
        ret_date = DateEntry(ret_window, date_pattern="yyyy-mm-dd"); ret_date.grid(row=0, column=1, padx=10)
        tk.Label(ret_window, text="Usage (Days/Hrs):").grid(row=1, column=0, padx=10, pady=10)
        usage_ent = tk.Entry(ret_window); usage_ent.grid(row=1, column=1, padx=10)
        try:
            d1 = datetime.strptime(start_str, "%Y-%m-%d").date(); d2 = datetime.now().date(); delta = (d2 - d1).days
            if delta == 0: delta = 1
            usage_ent.insert(0, str(delta))
        except: pass
        def confirm_return():
            try:
                usage = float(usage_ent.get()); end_d = ret_date.get()
                conn = connect_db(); cur = conn.cursor()
                cur.execute("SELECT rate_amount, client_name, machine_type, client_phone, reg_number, currency FROM logistics_log WHERE id=%s", (log_id,))
                res = cur.fetchone()
                rate = res[0]; client_n = res[1]; mach_t = res[2]; client_p = res[3]; reg_n = res[4]; curr = res[5]
                total = rate * usage
                msg = f"CLIENT: {client_n}\nMACHINE: {mach_t}\nTotal: {curr} {total:,.2f}\n\nConfirm?"
                if messagebox.askyesno("Confirm Bill", msg):
                    cur.execute("UPDATE logistics_log SET status='Returned', end_date=%s, total_usage=%s, total_cost=%s WHERE id=%s", (end_d, usage, total, log_id))
                    conn.commit(); conn.close()
                    sms_msg = f"Dear {client_n}, lease for {mach_t} ({reg_n}) ended. Usage: {usage}. Bill: {curr} {total:,.2f}. Thank you."
                    send_bill_sms_threaded(client_p, sms_msg, callback=lambda: messagebox.showinfo("Success", "SMS Sent"))
                    ret_window.destroy(); refresh_logistics()
            except Exception as e: messagebox.showerror("Error", str(e))
        tk.Button(ret_window, text="Calculate & Close", bg="green", fg="white", command=confirm_return).grid(row=2, column=0, columnspan=2, pady=20)

    tk.Button(log_left, text="🚀 Dispatch Machine", bg="#007bff", fg="white", command=dispatch_machine).grid(row=10, column=0, columnspan=2, pady=20, sticky="ew")
    log_cols = ("ID", "Client", "Machine", "Reg No", "Site", "Start", "Status", "End", "Cost", "Curr")
    log_tree = ttk.Treeview(log_right, columns=log_cols, show="headings")
    for c in log_cols: log_tree.heading(c, text=c); log_tree.column(c, width=80)
    log_tree.pack(fill="both", expand=True)
    tk.Button(log_right, text="🔄 Return & Bill", bg="#ffc107", command=return_machine).pack(pady=10)

    # ==========================================
    # TAB 3: GENUINE PARTS (Inventory)
    # ==========================================
    inv_left = ttk.LabelFrame(tab_parts, text="📥 Add Stock", padding=10); inv_left.pack(side="left", fill="y", padx=10, pady=10)
    inv_right = ttk.LabelFrame(tab_parts, text="📋 Inventory", padding=10); inv_right.pack(side="right", fill="both", expand=True, padx=10, pady=10)
    
    ttk.Label(inv_left, text="Part Name:").grid(row=0, column=0, sticky="w", pady=5)
    part_name_ent = ttk.Entry(inv_left, width=30); part_name_ent.grid(row=0, column=1, pady=5)
    ttk.Label(inv_left, text="Serial/Barcode:").grid(row=1, column=0, sticky="w", pady=5)
    part_serial_ent = ttk.Entry(inv_left, width=30); part_serial_ent.grid(row=1, column=1, pady=5)
    ttk.Label(inv_left, text="Supplier:").grid(row=2, column=0, sticky="w", pady=5)
    part_supp_ent = ttk.Combobox(inv_left, values=["Toyota Kenya", "Isuzu EA", "Bosch", "Hyundai Mobis", "Mitsubishi Electric", "ZF Friedrichshafen (ZF Group)", "Yanfeng Automotive", "Hitachi Astemo"], width=27); part_supp_ent.grid(row=2, column=1, pady=5)

    def refresh_inventory():
        for i in inv_tree.get_children(): inv_tree.delete(i)
        conn = connect_db(); cur = conn.cursor()
        cur.execute("SELECT id, part_name, serial_number, supplier, status, date_added FROM parts_inventory ORDER BY id DESC")
        for row in cur.fetchall(): inv_tree.insert("", "end", values=row)
        conn.close()

    def add_stock():
        n = part_name_ent.get(); s = part_serial_ent.get(); sp = part_supp_ent.get()
        if n and s:
            try:
                c = connect_db(); c.execute("INSERT INTO parts_inventory (part_name, serial_number, supplier, date_added) VALUES (%s,%s,%s,%s)", (n, s, sp, datetime.now().strftime("%Y-%m-%d")))
                c.commit(); c.close(); messagebox.showinfo("Success", "Added"); refresh_inventory()
            except: messagebox.showerror("Error", "Duplicate Serial")
    
    tk.Button(inv_left, text="Add to Stock", bg="#007bff", fg="white", command=add_stock).grid(row=3, column=0, columnspan=2, pady=20, sticky="ew")
    inv_cols = ("ID", "Part Name", "Serial #", "Supplier", "Status", "Date Added")
    inv_tree = ttk.Treeview(inv_right, columns=inv_cols, show="headings")
    for c in inv_cols: inv_tree.heading(c, text=c); inv_tree.column(c, width=100)
    inv_tree.pack(fill="both", expand=True)

    # ==========================================
    # TAB 4: EXPIRY
    # ==========================================
    cols_exp = ("Vehicle", "Insurance", "Inspection", "Speed Gov", "Status")
    tree_exp = ttk.Treeview(tab_expiry, columns=cols_exp, show="headings")
    for c in cols_exp: tree_exp.heading(c, text=c)
    tree_exp.pack(fill="both", expand=True, padx=10, pady=10)
    
    def load_expiry():
        for i in tree_exp.get_children(): tree_exp.delete(i)
        conn = connect_db(); cur = conn.cursor()
        cur.execute("SELECT vehicle, insurance_expiry, inspection_expiry, speed_governor_expiry FROM expiry_alerts")
        today = datetime.now().date()
        for row in cur.fetchall():
            status = "OK"
            for d_str in row[1:]:
                if d_str:
                    try:
                        if datetime.strptime(d_str, "%Y-%m-%d").date() < today: status = "EXPIRED"
                    except: pass
            tree_exp.insert("", "end", values=(row[0], row[1], row[2], row[3], status))
        conn.close()
    tk.Button(tab_expiry, text="Refresh Alerts", command=load_expiry).pack(pady=5)

    # ==========================================
    # TAB 5: ACCOUNT & USER MANAGEMENT
    # ==========================================
    def logout(): app.destroy(); login_window()

    def refresh_users():
        if 'users_tree' not in locals() and 'users_tree' not in globals(): return
        for i in users_tree.get_children(): users_tree.delete(i)
        conn = connect_db(); cur = conn.cursor()
        cur.execute("SELECT username, role, email, phone FROM users")
        for row in cur.fetchall(): users_tree.insert("", "end", values=row)
        conn.close()

    def register_user_gui():
        if role != "admin": messagebox.showerror("Permission Denied", "Only admin may register new users."); return
        reg_win = tk.Toplevel(app); reg_win.title("Register New User"); reg_win.geometry("400x350")
        tk.Label(reg_win, text="Username").grid(row=0, column=0, sticky="e", pady=5, padx=5); user_entry = tk.Entry(reg_win); user_entry.grid(row=0, column=1, pady=5, padx=5)
        tk.Label(reg_win, text="Password").grid(row=1, column=0, sticky="e", pady=5, padx=5); pass_entry = tk.Entry(reg_win, show="*"); pass_entry.grid(row=1, column=1, pady=5, padx=5)
        tk.Label(reg_win, text="Role").grid(row=2, column=0, sticky="e", pady=5, padx=5); role_var = tk.StringVar(value="mechanic"); ttk.Combobox(reg_win, textvariable=role_var, values=["admin", "mechanic"], state="readonly").grid(row=2, column=1, pady=5, padx=5)
        tk.Label(reg_win, text="Email").grid(row=3, column=0, sticky="e", pady=5, padx=5); email_entry = tk.Entry(reg_win); email_entry.grid(row=3, column=1, pady=5, padx=5)
        tk.Label(reg_win, text="Phone").grid(row=4, column=0, sticky="e", pady=5, padx=5); phone_entry = tk.Entry(reg_win); phone_entry.grid(row=4, column=1, pady=5, padx=5)
        tk.Label(reg_win, text="Security Question").grid(row=5, column=0, sticky="e", pady=5, padx=5); secq_entry = tk.Entry(reg_win, width=30); secq_entry.grid(row=5, column=1, pady=5, padx=5)
        tk.Label(reg_win, text="Security Answer").grid(row=6, column=0, sticky="e", pady=5, padx=5); seca_entry = tk.Entry(reg_win, show="*"); seca_entry.grid(row=6, column=1, pady=5, padx=5)
        def do_register():
            u = user_entry.get().strip(); p = pass_entry.get(); r = role_var.get(); e = email_entry.get().strip(); ph = phone_entry.get().strip(); sq = secq_entry.get().strip(); sa = seca_entry.get().strip()
            if not u or not p or not r or not sq or not sa: messagebox.showerror("Error", "All fields required."); return
            conn = connect_db(); cur = conn.cursor()
            try:
                cur.execute("INSERT INTO users (username, password, role, email, phone, security_question, security_answer_hash) VALUES (%s, %s, %s, %s, %s, %s, %s)", (u, hash_text(p), r, e, ph, sq, hash_text(sa)))
                conn.commit(); messagebox.showinfo("Success", "User registered."); reg_win.destroy(); refresh_users()
            except psycopg2.IntegrityError: messagebox.showerror("Error", "Username exists.")
            finally: conn.close()
        tk.Button(reg_win, text="Register User", bg="#28a745", fg="white", command=do_register).grid(row=7, column=0, columnspan=2, pady=15)

    def self_change_password_gui():
        conn = connect_db(); cur = conn.cursor(); cur.execute("SELECT email, phone, security_question, security_answer_hash FROM users WHERE username=%s", (username,)); row = cur.fetchone(); conn.close()
        if not row: return
        email, phone, sec_q, sec_a_hash = row
        win = tk.Toplevel(app); win.title("Change My Password"); win.geometry("350x300")
        tk.Label(win, text=f"Security Question:\n{sec_q}", font=("Arial", 10, "italic")).pack(pady=10)
        tk.Label(win, text="Your Answer:").pack(); ans_entry = tk.Entry(win, show="*"); ans_entry.pack()
        tk.Label(win, text="New Password:").pack(); new_entry = tk.Entry(win, show="*"); new_entry.pack()
        code = gen_code(); sent = False
        if email or phone: sent = send_verification_code(email, phone, code)
        def show_code_hint():
            if sent: messagebox.showinfo("Sent", f"Code sent to {email or phone}")
            else: messagebox.showinfo("Simulation Mode", f"SMS/Email Failed (Simulated).\nYour Verification Code is: {code}")
        tk.Button(win, text="Get Verification Code", command=show_code_hint).pack(pady=5)
        tk.Label(win, text="Enter Code:").pack(); code_entry = tk.Entry(win); code_entry.pack()
        def do_change():
            if hash_text(ans_entry.get()) != sec_a_hash: messagebox.showerror("Error", "Wrong Security Answer"); return
            if code_entry.get() != code: messagebox.showerror("Error", "Wrong Verification Code"); return
            c = connect_db(); c.execute("UPDATE users SET password=? WHERE username=?", (hash_text(new_entry.get()), username)); c.commit(); c.close()
            messagebox.showinfo("Success", "Password Changed"); win.destroy()
        tk.Button(win, text="Update Password", bg="orange", command=do_change).pack(pady=10)

    def admin_reset_gui():
        if role != "admin": return
        win = tk.Toplevel(app); win.title("Admin Reset Password"); win.geometry("300x200")
        tk.Label(win, text="Target Username:").pack(pady=5); u_ent = tk.Entry(win); u_ent.pack()
        tk.Label(win, text="New Password:").pack(pady=5); p_ent = tk.Entry(win); p_ent.pack()
        def do_reset():
            u = u_ent.get(); p = p_ent.get()
            if not u or not p: return
            c = connect_db(); c.execute("UPDATE users SET password=%s WHERE username=%s", (hash_text(p), u))
            c.commit(); c.close(); messagebox.showinfo("Success", f"Password reset for {u}"); win.destroy()
        tk.Button(win, text="Reset Password", bg="red", fg="white", command=do_reset).pack(pady=10)

    def delete_user_action():
        if role != "admin": return
        selected_user = None
        try:
            sel_item = users_tree.selection()
            if sel_item: selected_user = users_tree.item(sel_item)['values'][0]
        except: pass
        if selected_user: uname = selected_user
        else: uname = simpledialog.askstring("Delete User", "Enter username to delete:")
        if not uname: return
        if uname == username: messagebox.showerror("Error", "Cannot delete yourself"); return
        if messagebox.askyesno("Confirm", f"Delete user {uname}?"):
            conn = connect_db(); conn.execute("DELETE FROM users WHERE username=%s", (uname,)); conn.commit(); conn.close()
            messagebox.showinfo("Deleted", f"User {uname} deleted."); refresh_users()

    ttk.Label(tab_account, text="👤 User Profile", font=("Arial", 14, "bold")).pack(pady=15)
    info_frame = ttk.LabelFrame(tab_account, text="My Info", padding=10); info_frame.pack(fill="x", padx=20)
    ttk.Label(info_frame, text=f"Logged in as: {username}", font=("Arial", 11)).pack(anchor="w")
    ttk.Label(info_frame, text=f"Role: {role.upper()}", font=("Arial", 11, "bold")).pack(anchor="w")
    tk.Button(info_frame, text="Change My Password", command=self_change_password_gui).pack(pady=5, anchor="w")
    tk.Button(info_frame, text="Log Out", bg="#dc3545", fg="white", command=logout).pack(pady=10, anchor="w")

    if role == "admin":
        admin_frame = ttk.LabelFrame(tab_account, text="🛡️ Admin Zone", padding=10); admin_frame.pack(fill="x", padx=20, pady=20)
        tk.Button(admin_frame, text="➕ Register New User", bg="#28a745", fg="white", command=register_user_gui).pack(side="left", padx=5)
        tk.Button(admin_frame, text="🔑 Reset User Password", bg="#ffc107", command=admin_reset_gui).pack(side="left", padx=5)
        tk.Button(admin_frame, text="🗑️ Delete User (Select Below)", bg="#dc3545", fg="white", command=delete_user_action).pack(side="left", padx=5)
        list_frame = ttk.LabelFrame(tab_account, text="👥 User Directory", padding=10); list_frame.pack(fill="both", expand=True, padx=20, pady=10)
        u_cols = ("Username", "Role", "Email", "Phone")
        users_tree = ttk.Treeview(list_frame, columns=u_cols, show="headings", height=8)
        for c in u_cols: users_tree.heading(c, text=c); users_tree.column(c, width=100)
        users_tree.pack(fill="both", expand=True)

    refresh_tree(); load_expiry(); refresh_inventory()
    if role == "admin": refresh_users()
    app.mainloop()

# --- 7. LOGIN SCREEN ---
def login_window():
    root = tk.Tk()
    root.title("Login - DAGIV ENGINEERING ERP")
    root.geometry("400x350")
    
    ttk.Label(root, text="DAGIV ERP (Cloud Edition)", font=("Arial", 16, "bold")).pack(pady=20)
    
    frame = ttk.Frame(root); frame.pack()
    ttk.Label(frame, text="Username:").grid(row=0, column=0)
    user_ent = ttk.Entry(frame); user_ent.grid(row=0, column=1)
    ttk.Label(frame, text="Password:").grid(row=1, column=0)
    pass_ent = ttk.Entry(frame, show="*"); pass_ent.grid(row=1, column=1)
    
    def check_login():
        u = user_ent.get(); p = pass_ent.get()
        
        # Connect to Supabase
        conn = connect_db()
        if not conn:
            messagebox.showerror("Error", "No Internet Connection or Database Offline")
            return
            
        try:
            cur = conn.cursor()
            cur.execute("SELECT role, password FROM users WHERE username=%s", (u,))
            res = cur.fetchone()
            conn.close()
            
            if res and res[1] == hash_text(p): 
                root.destroy()
                run_main_app(u, res[0])
            else: 
                messagebox.showerror("Error", "Invalid Credentials")
        except Exception as e:
            messagebox.showerror("Error", f"Login failed: {e}")
            
    tk.Button(root, text="LOGIN", bg="#007bff", fg="white", width=20, command=check_login).pack(pady=20)
    root.mainloop()

if __name__ == "__main__":
    initialize_db()
    login_window()