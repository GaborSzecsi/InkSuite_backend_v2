# Support session (platform admin acting for tenant). Placeholder schema.
# class SupportSession(Base):
#     __tablename__ = "support_sessions"
#     id = Column(UUID, primary_key=True)
#     platform_user_id = Column(UUID, ForeignKey("users.id"))
#     tenant_id = Column(UUID, ForeignKey("tenants.id"))
#     reason = Column(String(512))
#     expires_at = Column(DateTime(timezone=True))
