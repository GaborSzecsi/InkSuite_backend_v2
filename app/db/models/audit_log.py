# Append-only audit log. Placeholder schema.
# class AuditLog(Base):
#     __tablename__ = "audit_log"
#     id = Column(BigInteger, primary_key=True, autoincrement=True)
#     actor_id = Column(UUID)
#     acted_as_user_id = Column(UUID, nullable=True)
#     tenant_id = Column(UUID, nullable=True)
#     action = Column(String(64))
#     resource = Column(String(256))
#     created_at = Column(DateTime(timezone=True), server_default=func.now())
