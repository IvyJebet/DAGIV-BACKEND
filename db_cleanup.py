import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def delete_test_user(email_to_delete: str):
    print(f"🔍 Searching for account with email: {email_to_delete}")
    
    # Connect to the database
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    try:
        # 1. Find the user ID
        cursor.execute("SELECT id FROM users WHERE email = %s", (email_to_delete,))
        user = cursor.fetchone()
        
        if not user:
            print("⚠️ No user found with that email. Are you sure it's spelled correctly?")
            return
            
        user_id = user[0]
        print(f"🗑️ Found user ID: {user_id}. Beginning cleanup...")
        
        # 2. Delete associated records first to keep the database clean
        cursor.execute("DELETE FROM otps WHERE user_id = %s", (user_id,))
        print("   - Cleared OTP records.")
        
        cursor.execute("DELETE FROM wallets WHERE user_id = %s", (user_id,))
        print("   - Cleared Wallet records.")
        
        cursor.execute("DELETE FROM sellers WHERE email = %s", (email_to_delete,))
        print("   - Cleared Seller profiles (if any).")
        
        # 3. Delete the actual user
        cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
        print("   - Cleared User record.")
        
        # 4. Commit the transaction
        conn.commit()
        print(f"\n✅ Success! The account '{email_to_delete}' has been completely deleted.")
        print("You can now register fresh.")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error during deletion: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    target_email = input("Enter the email address you want to delete: ")
    delete_test_user(target_email)