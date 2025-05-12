from sqlalchemy import create_engine, Column, String, Float, Boolean, DateTime, ARRAY, Integer 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

DATABASE_URL = os.getenv("DB_URL")
Base = declarative_base()

# Define the Campground table structure
class CampgroundDB(Base):
    __tablename__ = "campgrounds"
    id = Column(String, primary_key=True, index=True)
    type = Column(String)
    links_self = Column(String)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    region_name = Column(String)
    administrative_area = Column(String, nullable=True)
    nearest_city_name = Column(String, nullable=True)
    accommodation_type_names = Column(ARRAY(String), nullable=True)
    bookable = Column(Boolean, default=False)
    camper_types = Column(ARRAY(String), nullable=True)
    operator = Column(String, nullable=True)
    photo_url = Column(String, nullable=True)
    photo_urls = Column(ARRAY(String), nullable=True)
    photos_count = Column(Integer, default=0) 
    rating = Column(Float, nullable=True)
    reviews_count = Column(Integer, default=0)
    slug = Column(String, nullable=True)
    price_low = Column(Float, nullable=True)
    price_high = Column(Float, nullable=True)
    availability_updated_at = Column(DateTime, nullable=True)
    address = Column(String, nullable=True)  # Added for geocoding reverse lookup

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def create_tables():
    Base.metadata.create_all(bind=engine)
    print("Database tables created (if they didn't exist previously).")

def get_db():
    db = SessionLocal()
    try:
        return db
    except Exception:
        db.close()
        raise 