from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Date, Integer, JSON
from sqlalchemy.dialects.postgresql import JSONB # Specific for Postgres
from typing import Optional
from datetime import date
from database import Base

class CompanyCSV(Base):
    __tablename__ = "companies_csv"
    
    #Identifiers
    company_number: Mapped[str] = mapped_column(String(20), primary_key=True)
    company_name: Mapped[str] = mapped_column(String(255), index=True)
    
    #Categorization
    company_category: Mapped[str] = mapped_column(String(100))
    company_status: Mapped[str] = mapped_column(String(50), index=True)
    country_of_origin: Mapped[str] = mapped_column(String(100))
    
    #Important Dates & Codes
    incorporation_date: Mapped[Optional[date]] = mapped_column(Date)
    sic_code: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    
    #Mortgage/Financial Data
    # We use Integer here because the CSV provides counts
    no_mortgages: Mapped[int] = mapped_column(Integer, default=0)
    mortgages_outstanding: Mapped[int] = mapped_column(Integer, default=0)
    mortgages_part_satisfied: Mapped[int] = mapped_column(Integer, default=0)
    mortgages_satisfied: Mapped[int] = mapped_column(Integer, default=0)
    
class CompanyAPI(Base):
    __tablename__ = "companies_api"
    
    # Primary Key to link both tables
    company_number: Mapped[str] = mapped_column(String(20), primary_key=True)
    
    # Standardized fields from API to compare easily with CSV
    company_status: Mapped[Optional[str]] = mapped_column(String(50))

    profile_data: Mapped[dict] = mapped_column(JSONB, nullable=True)
    filing_history: Mapped[dict] = mapped_column(JSONB, nullable=True)


    
    ## The "Big Data" containers
    ## JSONB allows us to store the full response (for Address History & Officers)
    #profile_data: Mapped[dict] = mapped_column(JSONB, nullable=True) 
    #officers_data: Mapped[dict] = mapped_column(JSONB, nullable=True)
    #filing_history: Mapped[dict] = mapped_column(JSONB, nullable=True)