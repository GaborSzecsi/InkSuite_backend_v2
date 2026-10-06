"""Separate worker process. No server startup thread and no live sending in this release."""
import logging
import time
from . import service as s
from .adapters import adapter_for, ConfigurationRequired
from .transports import sftp,TransportError
from .secrets import read

log=logging.getLogger(__name__)

def run_once():
    with s.transaction() as cur:
        # A crashed submission has an unknown outcome: do not queue it again.
        cur.execute("UPDATE distribution_jobs SET status='REVIEW_REQUIRED',error_code='WORKER_INTERRUPTED' WHERE status='RUNNING' AND started_at<now()-interval '10 minutes'")
        job=s.one(cur,"SELECT * FROM distribution_jobs WHERE status='QUEUED' AND due_at<=now() ORDER BY due_at,id FOR UPDATE SKIP LOCKED LIMIT 1")
        if not job: return False
        cur.execute("UPDATE distribution_jobs SET status='RUNNING',attempts=attempts+1,started_at=now() WHERE id=%s",(job['id'],))
        conn=s.connection(cur,job['tenant_id'],job['connection_id'])
    code=None
    try:
        adapter=adapter_for(conn)
        if job['kind']=='TEST':
            config=conn['configuration'].get('orders',{})
            if config.get('transport')!='sftp': raise ConfigurationRequired('SFTP configuration is required for this adapter.')
            with sftp(config,read(conn['secret_reference'])) as client:
                client.stat(config['directory'])
        elif job['kind']=='INVENTORY':
            if not adapter.capabilities.inventory: raise ConfigurationRequired('Inventory specification is required.')
            # No inventory-capable adapter is registered until snapshot semantics are verified.
            raise ConfigurationRequired('Inventory reconciliation is not configured.')
        else:
            raise ConfigurationRequired('Live distributor transmission is disabled while Exporteo remains active.')
    except ConfigurationRequired: code='CONFIGURATION_REQUIRED'
    except TransportError: code='TRANSPORT_FAILED'
    except Exception: code='CONNECTION_FAILED'
    with s.transaction() as cur:
        cur.execute('UPDATE distribution_jobs SET status=%s,error_code=%s,finished_at=now() WHERE id=%s',('FAILED' if code else 'DONE',code,job['id']))
        cur.execute('UPDATE distribution_connections SET status=%s,last_error=%s,last_successful_connection_at=CASE WHEN %s THEN now() ELSE last_successful_connection_at END WHERE tenant_id=%s AND id=%s',('ERROR' if code else 'CONNECTED',code,not bool(code),job['tenant_id'],job['connection_id']))
        s.event(cur,job['tenant_id'],job['connection_id'],'distribution-worker',job['kind']+('_FAILED' if code else '_COMPLETED'),job['order_id'],{'error_code':code})
    return True

if __name__=='__main__':
    import os
    from dotenv import load_dotenv
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    if os.getenv('DISTRIBUTION_WORKER_ENABLED') != 'true':
        raise SystemExit('Worker disabled. Enable only in the intended service environment; never against the development production tunnel.')
    from . import privacy
    last_retention = 0
    while True:
        try:
            # Privacy work cannot starve behind catalog/order queues.
            if time.monotonic() - last_retention > 3600:
                processed = privacy.retention_once()
                last_retention = time.monotonic() if processed < 100 else 0
            if not privacy.run_once() and not run_once():
                from .shopify import process_inbox_once, sync_catalog_once
                if not process_inbox_once() and not sync_catalog_once(): time.sleep(5)
        except Exception:
            log.error('Distribution worker unavailable; check database and migrations 020/021.')
            time.sleep(15)
