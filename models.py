from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Date, Integer, JSON
from sqlalchemy.dialects.postgresql import JSONB # Specific for Postgres
from typing import Optional
from datetime import date
from database import Base

class Company(Base):
    __tablename__ = "companies"
    
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
    
    
    # This remains as the placeholder for nested API data
    api_data: Mapped[dict] = mapped_column(JSONB, nullable=True)