"""
============================================================
Database Connection Setup
============================================================
WHAT: Connects Python to PostgreSQL using SQLAlchemy
WHY:  We need a database to store:
      - Table health metrics (how healthy are our Delta tables?)
      - Operations history (what did the optimizer do?)
      - Cost tracking (how much did it save?)

STORY: SQLAlchemy is like a TRANSLATOR between Python and SQL.
       You write Python code, SQLAlchemy converts it to SQL.
       This means you can switch databases later (PostgreSQL → 
       MySQL → Snowflake) without rewriting all your code!

HOW IT WORKS:
  1. "engine" = the connection to the database
  2. "SessionLocal" = a conversation with the database
     (open it, do stuff, close it)
  3. "Base" = the parent class for all our table definitions
============================================================
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import DATABASE_URL

# Step 1: Create the engine (the connection)
# STORY: Like plugging in the phone line to the filing cabinet
engine = create_engine(
    DATABASE_URL,
    # Pool settings: how many simultaneous connections allowed
    # STORY: Like having 5 phone lines to the filing cabinet
    #        instead of 1 (faster when busy!)
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
)

# Step 2: Create a session factory
# STORY: Every time your code needs to talk to the database,
#        it opens a "session" (picks up the phone),
#        does its work, then closes (hangs up).
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Step 3: Create the base class for table definitions
# STORY: All our database tables will inherit from this.
#        Like saying "every form in our filing cabinet 
#        must have a header and page numbers"
Base = declarative_base()


def get_db():
    """
    Get a database session.
    
    USAGE (in FastAPI):
        @app.get("/something")
        def my_endpoint(db: Session = Depends(get_db)):
            # use db here
            
    WHY this pattern?
    - Opens connection when needed
    - Automatically closes when done (even if error occurs!)
    - Called "dependency injection" — a fancy term that means
      "give me what I need, I don't care how you make it"
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()