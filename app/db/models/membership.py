# Membership model (user_id, tenant_id, role). Placeholder schema.
# class Membership(Base):
#     __tablename__ = "memberships"
#     id = Column(UUID, primary_key=True)
#     user_id = Column(UUID, ForeignKey("users.id"))
#     tenant_id = Column(UUID, ForeignKey("tenants.id"))
#     role = Column(String(32))  # tenant_admin, tenant_editor
#     unique(user_id, tenant_id)
