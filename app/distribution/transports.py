"""Bounded transports for operator-approved distributor hosts, never an HTTP proxy."""
import base64
import http.client
import io
import ipaddress
import json
import os
import posixpath
import socket
import ssl
from contextlib import contextmanager
from ftplib import FTP, FTP_TLS
from urllib.parse import urlsplit

MAX_BYTES=10*1024*1024
class TransportError(Exception): pass


def approved_address(host,port):
    allowed={h.strip().lower() for h in os.getenv('DISTRIBUTION_ALLOWED_HOSTS','').split(',') if h.strip()}
    if host.lower() not in allowed: raise TransportError('Distributor host requires operator approval.')
    addresses=socket.getaddrinfo(host,port,type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):
        raise TransportError('Distributor host must resolve only to public addresses.')
    return addresses[0][4][0]


def remote_path(folder,name):
    if not folder.startswith('/') or '..' in folder.split('/') or posixpath.basename(name)!=name or not name:
        raise TransportError('Invalid remote path.')
    if any(c in folder+name for c in ('\r','\n','\x00')): raise TransportError('Invalid remote path.')
    return posixpath.join(folder,name)


class HTTPS:
    def __init__(self,base_url,credentials=None):
        self.url=urlsplit(base_url)
        if self.url.scheme!='https' or not self.url.hostname or self.url.username or self.url.query or self.url.fragment:
            raise TransportError('An approved HTTPS endpoint is required.')
        self.credentials=credentials or {}
    def request(self,method,path='',body=None,content_type='application/json'):
        if method not in ('GET','POST','PUT') or path.startswith('//') or '://' in path:
            raise TransportError('Unsupported distributor request.')
        host=self.url.hostname;port=self.url.port or 443
        ip=approved_address(host,port)
        conn=http.client.HTTPSConnection(host,port,timeout=20,context=ssl.create_default_context())
        # Pin the validated IP; retain the hostname for TLS identity and SNI.
        sock=socket.create_connection((ip,port),timeout=20)
        conn.sock=ssl.create_default_context().wrap_socket(sock,server_hostname=host)
        headers={'Content-Type':content_type,'Accept':'application/json, application/xml'}
        auth=self.credentials
        if auth.get('bearer_token'): headers['Authorization']='Bearer '+auth['bearer_token']
        elif auth.get('password'):
            headers['Authorization']='Basic '+base64.b64encode((auth.get('username','')+':'+auth['password']).encode()).decode()
        if auth.get('api_key'): headers['X-API-Key']=auth['api_key']
        try:
            conn.request(method,(self.url.path.rstrip('/')+'/'+path.lstrip('/')) or '/',body=body,headers=headers)
            response=conn.getresponse()
            data=response.read(MAX_BYTES+1)
            if len(data)>MAX_BYTES: raise TransportError('Distributor response exceeds the size limit.')
            if not 200<=response.status<300: raise TransportError(f'Distributor returned HTTP {response.status}.')
            return data
        except (OSError,http.client.HTTPException): raise TransportError('Distributor request failed; transmission outcome may be unknown.') from None
        finally: conn.close()


@contextmanager
def sftp(config,credentials):
    import paramiko
    host=config['host'];port=int(config.get('port',22));ip=approved_address(host,port)
    sock=socket.create_connection((ip,port),timeout=20)
    transport=paramiko.Transport(sock)
    try:
        transport.start_client(timeout=20)
        actual=transport.get_remote_server_key()
        expected=config.get('host_key')
        if not expected or not __import__('hmac').compare_digest(actual.get_base64(),expected):
            raise TransportError('SFTP host key is missing or does not match.')
        if credentials.get('private_key'):
            key=None
            for cls in (paramiko.Ed25519Key,paramiko.ECDSAKey,paramiko.RSAKey):
                try:
                    key=cls.from_private_key(io.StringIO(credentials['private_key']),password=credentials.get('passphrase'));break
                except (ValueError,paramiko.SSHException): continue
            if key is None: raise TransportError('SSH private key could not be loaded.')
            transport.auth_publickey(credentials['username'],key)
        else: transport.auth_password(credentials['username'],credentials['password'])
        client=paramiko.SFTPClient.from_transport(transport)
        client.get_channel().settimeout(20)
        yield client
    except TransportError: raise
    except Exception: raise TransportError('SFTP operation failed; inspect transmission status before retrying an upload.') from None
    finally: transport.close();sock.close()


def sftp_upload(client,folder,filename,content):
    if len(content)>MAX_BYTES: raise TransportError('File exceeds the size limit.')
    final=remote_path(folder,filename);temp=remote_path(folder,filename+'.partial')
    # Existing final or temporary file requires reconciliation, never blind overwrite.
    for path in (final,temp):
        try: client.stat(path)
        except FileNotFoundError: continue
        raise TransportError('Transmission file already exists; reconciliation is required.')
    with client.open(temp,'wx') as f: f.write(content)
    client.rename(temp,final)


def sftp_download(client,folder,filename):
    with client.open(remote_path(folder,filename),'rb') as f: data=f.read(MAX_BYTES+1)
    if len(data)>MAX_BYTES: raise TransportError('File exceeds the size limit.')
    return data


@contextmanager
def ftp(config,credentials):
    # Legacy FTP must be explicitly enabled by an operator; encrypted FTPS preferred.
    if not config.get('tls',True) and os.getenv('DISTRIBUTION_ALLOW_PLAIN_FTP')!='1':
        raise TransportError('Plain FTP is disabled.')
    host=config['host'];port=int(config.get('port',21));ip=approved_address(host,port)
    client=FTP_TLS(context=ssl.create_default_context(),timeout=20) if config.get('tls',True) else FTP(timeout=20)
    try:
        client.connect(ip,port);client.host=host
        client.login(credentials['username'],credentials['password'])
        if isinstance(client,FTP_TLS): client.prot_p()
        # Python uses control peer address for passive IPv4 data connections by default.
        client.trust_server_pasv_ipv4_address=False
        yield client
    except Exception: raise TransportError('FTP operation failed; reconcile uploads before retry.') from None
    finally: client.close()
