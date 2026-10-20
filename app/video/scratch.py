"""Private scratch folders with inherited Windows ACLs; cleaned after every render."""
from contextlib import contextmanager
from pathlib import Path
import os,shutil,tempfile,uuid
@contextmanager
def scratch():
    if os.name!='nt':
        with tempfile.TemporaryDirectory(prefix='inksuite-video-') as folder: yield folder
        return
    parent=Path(os.environ.get('VIDEO_TEMP_DIR') or tempfile.gettempdir()).resolve()
    folder=parent/('inksuite-video-'+str(uuid.uuid4()))
    folder.mkdir(mode=0o777)
    try: yield str(folder)
    finally:
        if folder.resolve().parent!=parent: raise RuntimeError('Unsafe scratch cleanup path.')
        shutil.rmtree(folder)
