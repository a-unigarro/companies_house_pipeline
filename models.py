from sqlalchemy import Column, String, Date, Integer
from sqlalchemy.dialects.postgresql import JSONB # Específico para Postgres
from database import Base

# NOTA: Eliminamos 'Mapped', 'mapped_column', 'JSON', 'Optional' y 'date' 
# porque ya no son necesarios en las columnas clásicas de SQLAlchemy 1.4.

class CompanyCSV(Base):
    __tablename__ = "companies_csv"
    
    # Identifiers
    company_number = Column(String(20), primary_key=True)
    company_name = Column(String(255), index=True, nullable=False)
    
    # Categorization
    company_category = Column(String(100), nullable=False)
    company_status = Column(String(50), index=True, nullable=False)
    country_of_origin = Column(String(100), nullable=False)
    
    # Important Dates & Codes
    # En 1.4, 'Optional' se convierte en 'nullable=True'
    incorporation_date = Column(Date, nullable=True) 
    sic_code = Column(String(255), index=True, nullable=True)
    
    # Mortgage/Financial Data
    no_mortgages = Column(Integer, default=0, nullable=False)
    mortgages_outstanding = Column(Integer, default=0, nullable=False)
    mortgages_part_satisfied = Column(Integer, default=0, nullable=False)
    mortgages_satisfied = Column(Integer, default=0, nullable=False)
    
class CompanyAPI(Base):
    __tablename__ = "companies_api"
    
    # Primary Key to link both tables
    company_number = Column(String(20), primary_key=True)
    
    # Standardized fields from API to compare easily with CSV
    company_status = Column(String(50), nullable=True)

    # Contenedores de Big Data usando JSONB directo de Postgres
    profile_data = Column(JSONB, nullable=True)
    filing_history = Column(JSONB, nullable=True)