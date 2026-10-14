"""Shared title description projection from ONIX TextType 03.

Edition text is authoritative when present; works.main_description supports
legacy titles without ONIX text. All joins include the publisher boundary.
"""
DESCRIPTION_SQL = """COALESCE((
    SELECT t.text_value FROM edition_texts t
    JOIN editions de ON de.id=t.edition_id AND de.tenant_id=t.tenant_id
    WHERE de.tenant_id=w.tenant_id AND de.work_id=w.id AND t.text_type='03'
    ORDER BY t.updated_at DESC NULLS LAST, t.created_at DESC, t.item_order, t.id
    LIMIT 1
), w.main_description, '')"""


def description_for_work(cur, tenant_id, work_id):
    cur.execute(f"SELECT {DESCRIPTION_SQL} AS description FROM works w WHERE w.tenant_id=%s AND w.id=%s", (tenant_id, work_id))
    row = cur.fetchone()
    return (row or {}).get("description") or ""


def save_shared_description(cur, tenant_id, work_id, text):
    """Update the authoritative ONIX entry if present, plus its legacy mirror."""
    cur.execute("""SELECT t.id FROM edition_texts t
        JOIN editions e ON e.id=t.edition_id AND e.tenant_id=t.tenant_id
        WHERE e.tenant_id=%s AND e.work_id=%s AND t.text_type='03'
        ORDER BY t.updated_at DESC NULLS LAST, t.created_at DESC, t.item_order, t.id
        LIMIT 1""", (tenant_id, work_id))
    row = cur.fetchone()
    if row:
        cur.execute("UPDATE edition_texts SET text_value=%s, text_format='06', updated_at=now() WHERE tenant_id=%s AND id=%s", (text, tenant_id, row["id"]))
    cur.execute("UPDATE works SET main_description=%s WHERE tenant_id=%s AND id=%s", (text, tenant_id, work_id))
