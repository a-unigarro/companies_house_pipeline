import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase


# 1. Load the variables from your .env file
load_dotenv()
# 2. Grab the variables using the keys you defined
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


DATABASE_URL=f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

#with engine.connect() as conn:
#    print('Connection Succesfull')



# This is the "catalogue" where all your models will be registered
class Base(DeclarativeBase):
    pass

############## FOR LATER ########################
## This is what we will use in our FastAPI routes to talk to the DB
#SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
#
##Helper for FastAPI
#def get_db():
#    db = SessionLocal()
#    try:
#        yield db
#    finally:
#        db.close()