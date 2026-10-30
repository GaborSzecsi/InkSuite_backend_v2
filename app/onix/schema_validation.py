"""Offline XSD validation using pinned, local EDItEUR schema files.
No catalog metadata is inferred, fetched or modified by this module.
"""
from functools import lru_cache
from pathlib import Path
from threading import RLock
from lxml import etree

@lru_cache(maxsize=2)
def _schema(release):
    if release not in ("3.0", "3.1"):
        raise ValueError("Supported ONIX releases are 3.0 and 3.1")
    path = Path(__file__).parent / "schemas" / ("xsd" + release) / ("ONIX_BookProduct_" + release + "_reference.xsd")
    parser = etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False)
    return etree.XMLSchema(etree.parse(str(path), parser)), RLock()

def validate_xml(xml, release):
    data = xml.encode("utf8") if isinstance(xml,str) else xml
    if b"<!DOCTYPE" in data:
        return {"valid": False, "errors": [{"message":"DOCTYPE is not allowed in exported ONIX", "path":""}]}
    schema, lock = _schema(release)
    # Validate incrementally and release completed product trees. Large publisher
    # catalogs do not require a second complete XML DOM alongside the export.
    parser = etree.XMLPullParser(events=("end",), schema=schema,
                                 resolve_entities=False, no_network=True, load_dtd=False)
    try:
        with lock:
            for offset in range(0, len(data), 65536):
                parser.feed(data[offset:offset+65536])
                for _, node in parser.read_events():
                    if etree.QName(node).localname == "Product":
                        node.clear()
                        while node.getprevious() is not None:
                            del node.getparent()[0]
            parser.close()
        return {"valid": True, "errors": []}
    except (etree.XMLSyntaxError, etree.XMLSchemaValidateError) as error:
        errors=[{"message":e.message,"path":e.path or ""} for e in parser.feed_error_log][:50]
        return {"valid": False, "errors":errors or [{"message":str(error),"path":""}]}
