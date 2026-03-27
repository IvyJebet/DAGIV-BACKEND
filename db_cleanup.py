import os
import json
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    """Creates a robust database connection with SSL handling for cloud DBs."""
    db_url = DATABASE_URL
    if db_url and "localhost" not in db_url and "127.0.0.1" not in db_url and "sslmode" not in db_url:
        sep = "&" if "?" in db_url else "?"
        db_url += f"{sep}sslmode=require"
    return psycopg2.connect(db_url)

def delete_test_user(email_to_delete: str):
    print(f"\n🔍 Searching for account with email: {email_to_delete}")
    try:
        conn = get_connection()
        cursor = conn.cursor()
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return
        
    try:
        # 1. Find the user ID
        cursor.execute("SELECT id FROM users WHERE email = %s", (email_to_delete,))
        user = cursor.fetchone()
        
        if not user:
            print("⚠️ No user found with that email. Are you sure it's spelled correctly?")
            return
            
        user_id = user[0]
        print(f"🗑️ Found user ID: {user_id}. Beginning cleanup...")
        
        # 2. Cleanup Cart Items
        cursor.execute("DELETE FROM cart_items WHERE user_id = %s", (user_id,))
        print(f"   - Cleared {cursor.rowcount} Cart item(s).")
        cursor.execute("DELETE FROM transactions WHERE order_id IN (SELECT id FROM orders WHERE buyer_id = %s)", (user_id,))
        print(f"   - Cleared {cursor.rowcount} Transaction(s).")
        cursor.execute("DELETE FROM order_line_items WHERE order_id IN (SELECT id FROM orders WHERE buyer_id = %s)", (user_id,))
        print(f"   - Cleared {cursor.rowcount} Order Line Item(s).")
        cursor.execute("DELETE FROM orders WHERE buyer_id = %s", (user_id,))
        print(f"   - Cleared {cursor.rowcount} Order(s).")
        cursor.execute("DELETE FROM wallets WHERE user_id = %s", (user_id,))
        print(f"   - Cleared {cursor.rowcount} Wallet record(s).")
        cursor.execute("DELETE FROM sellers WHERE email = %s", (email_to_delete,))
        print(f"   - Cleared {cursor.rowcount} Seller profile(s).")

        try:
            cursor.execute("SAVEPOINT pre_otp")
            cursor.execute("DELETE FROM otps WHERE user_id = %s", (user_id,))
            print("   - Cleared legacy OTP records.")
            cursor.execute("RELEASE SAVEPOINT pre_otp")
        except psycopg2.errors.UndefinedTable:
            cursor.execute("ROLLBACK TO SAVEPOINT pre_otp")
            pass       
            
        cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
        print(f"   - Cleared User record.")
        conn.commit()
        print(f"\n✅ Success! The account '{email_to_delete}' has been completely deleted.")
        print("You can now register fresh.")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error during deletion: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def is_heavy_base64(string):
    """
    Heuristic to detect Base64 strings. 
    If it starts with the data header or is longer than 500 characters, it's media.
    """
    if not isinstance(string, str):
        return False
    return string.startswith('data:image/') or string.startswith('data:video/') or len(string) > 500

def purge_base64_media():
    print("\n🧹 Booting up Database Sweeper...")
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return

    try:
        cursor.execute("SELECT id, specs FROM marketplace_listings")
        listings = cursor.fetchall()
        
        cleaned_listings_count = 0
        bytes_freed = 0

        for listing_id, specs in listings:
            if not specs:
                continue
                
            # Handle stringified JSON if it hasn't been cast cleanly by the driver
            if isinstance(specs, str):
                try:
                    specs = json.loads(specs)
                except Exception:
                    continue

            needs_update = False
            
            # We target all three media arrays in your schema
            for field in ['images', 'videos', 'complianceDocs']:
                if field in specs and isinstance(specs[field], list):
                    clean_list = []
                    
                    for item in specs[field]:
                        if is_heavy_base64(item):
                            needs_update = True
                            bytes_freed += len(item)
                        else:
                            # Keep valid URLs (like Supabase links)
                            clean_list.append(item)
                    
                    specs[field] = clean_list

            # Only write to the DB if we actually stripped something out
            if needs_update:
                cursor.execute(
                    "UPDATE marketplace_listings SET specs = %s WHERE id = %s",
                    (json.dumps(specs), listing_id)
                )
                cleaned_listings_count += 1

        conn.commit()
        
        megabytes_freed = bytes_freed / (1024 * 1024)
        print(f"✅ Purge Complete! Cleaned {cleaned_listings_count} listings.")
        print(f"📉 Estimated Database Space Freed: {megabytes_freed:.2f} MB")

    except Exception as e:
        conn.rollback()
        print(f"❌ Critical Error during sweep: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    while True:
        print("\n" + "="*45)
        print("🛠️   DAGIV DATABASE UTILITY TOOL   🛠️")
        print("="*45)
        print("1. Delete a test user account & data")
        print("2. Purge heavy Base64 media from listings")
        print("3. Exit")
        print("="*45)
        
        choice = input("Enter your choice (1-3): ").strip()
        
        if choice == '1':
            target_email = input("\nEnter the email address you want to delete: ").strip()
            if target_email:
                delete_test_user(target_email)
            else:
                print("No email provided. Returning to menu.")
                
        elif choice == '2':
            print("\n⚠️  WARNING: This will permanently delete Base64 media from your database.")
            print("Listings will fall back to using placeholder images.")
            confirmation = input("Type 'PURGE' to confirm you want to proceed: ").strip()
            if confirmation == 'PURGE':
                purge_base64_media()
            else:
                print("Operation aborted. Your database is untouched.")
                
        elif choice == '3':
            print("\nExiting utility tool. Goodbye! 👋")
            break
            
        else:
            print("\nInvalid choice. Please enter 1, 2, or 3.")

if __name__ == "__main__":
    main()