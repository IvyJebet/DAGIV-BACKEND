import os
import psycopg2
from dotenv import load_dotenv
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def delete_test_user(email_to_delete: str):
    print(f"🔍 Searching for account with email: {email_to_delete}")
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
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

if __name__ == "__main__":
    target_email = input("Enter the email address you want to delete: ")
    if target_email.strip():
        delete_test_user(target_email.strip())
    else:
        print("No email provided. Exiting.")