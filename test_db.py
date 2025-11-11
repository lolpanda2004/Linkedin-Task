"""Test database connection and table creation"""
from app.db.models import init_db, Base
from app.db.repo import DatabaseRepository
from app.config import settings

def test_database():
    print(f"Testing database: {settings.DATABASE_URL}")
    
    # Initialize database
    print("Creating tables...")
    init_db()
    print("✓ Tables created")
    
    # Test repository
    print("\nTesting repository...")
    repo = DatabaseRepository(db_url=settings.DATABASE_URL)
    
    with repo.get_session() as session:
        # Test counts
        summary = repo.get_database_summary(session)
        print(f"✓ Database summary: {summary}")
    
    repo.close()
    print("\n✓ Database test passed!")

if __name__ == "__main__":
    test_database()