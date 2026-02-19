# User model (for RDS). Placeholder schema.
# class User(Base):
#     __tablename__ = "users"
#     id = Column(UUID, primary_key=True)
#     cognito_sub = Column(String(256), unique=True)
#     email = Column(String(256), nullable=False)
#     created_at = Column(DateTime(timezone=True), server_default=func.now())
