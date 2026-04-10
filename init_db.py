from database import engine, Base
import models  # This must be imported so Base knows about the Company table

def create_tables():
    print("Connecting to PostgreSQL...")
    try:
        # This looks at every class inheriting from Base and creates the SQL table
        Base.metadata.drop_all(bind=engine) 
        Base.metadata.create_all(bind=engine)
        print("--- Tables created successfully! ---")
    except Exception as e:
        print(f"Error creating tables: {e}")

if __name__ == "__main__":
    create_tables()