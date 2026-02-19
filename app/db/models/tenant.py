# Tenant model (for RDS). Placeholder schema.
# from sqlalchemy import Column, String, DateTime
# from sqlalchemy.sql import func
# Base = ...
# class Tenant(Base):
#     __tablename__ = "tenants"
#     id = Column(UUID, primary_key=True)
#     slug = Column(String(64), unique=True, nullable=False)
#     name = Column(String(256))
#     created_at = Column(DateTime(timezone=True), server_default=func.now())
#     updated_at = Column(DateTime(timezone=True), onupdate=func.now())
