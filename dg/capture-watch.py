"""Read-only external capture-file observer for paired diagnostic controls."""
from pathlib import Path
import ctypes,datetime,hashlib,json,os,re,select,signal,struct,sys,time

label=sys.argv[1]
assert re.fullmatch(r'[a-z0-9_-]+',label)
out=Path('<EVIDENCE>/controls')/label
out.mkdir(parents=True,exist_ok=True)
stop=False
def request_stop(*args):
    global stop
    stop=True
signal.signal(signal.SIGUSR1,request_stop)
signal.signal(signal.SIGTERM,request_stop)
start=time.monotonic();cpu_start=time.process_time()
held={};launches={};watches={}
libc=ctypes.CDLL(None,use_errno=True)
notify=libc.inotify_init1(os.O_NONBLOCK|os.O_CLOEXEC)
def hold(path,via,tailpid=None):
    try:
        fd=path.open('rb');info=os.fstat(fd.fileno());key=(info.st_dev,info.st_ino)
        if key in held:
            if tailpid is not None:held[key]['tailPids'].add(tailpid)
            held[key]['seenVia'].add(via);fd.close()
        else:
            held[key]={'fd':fd,'path':str(path),'tailPids':{tailpid} if tailpid is not None else set(),'seenVia':{via},'firstSeenAtUtc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    except (FileNotFoundError,PermissionError,IsADirectoryError):pass
def add_tree(root):
    if notify<0:return
    for current,dirs,files in os.walk(root,followlinks=False):
        p=Path(current)
        wd=libc.inotify_add_watch(notify,os.fsencode(p),0x100|0x80|0x8)
        if wd>=0:watches[wd]=p
        if 'compute_logs' in p.parts:
            for name in files:
                target=p/name
                if target.suffix in ['.err','.out']:hold(target,'inotify-directory-discovery')
add_tree(Path('/tmp'))
next_proc_scan=0.0
(out/'watcher-runtime.json').write_text(json.dumps({'pid':os.getpid(),'startedAtUtc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'watcherSourceSha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'scope':'Owned /tmp compute_logs .err/.out via inotify, plus compute-style tail -F -c +0 targets in this container; read-only FDs held across unlink; no process pipes read or consumed.','procPollIntervalSec':0.25,'inotifyEnabled':notify>=0,'inotifyWatchesAtReady':len(watches)},indent=2)+'\n')
while not stop:
    if notify>=0:
        if select.select([notify],[],[],0.05)[0]:
            try:events=os.read(notify,65536)
            except BlockingIOError:events=b''
            offset=0
            while offset+16<=len(events):
                wd,mask,cookie,length=struct.unpack_from('iIII',events,offset)
                name=os.fsdecode(events[offset+16:offset+16+length].split(b'\0',1)[0]);offset+=16+length
                base=watches.get(wd)
                if base is None:continue
                path=base/name
                if mask&0x40000000 and mask&(0x100|0x80):add_tree(path)
                elif 'compute_logs' in path.parts and path.suffix in ['.err','.out']:hold(path,'inotify-capture-file-event')
    if time.monotonic()<next_proc_scan:continue
    next_proc_scan=time.monotonic()+0.25
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():continue
        try:
            args=(proc/'cmdline').read_bytes().split(b'\0')
            args=[a for a in args if a]
            if b'launch' in args and any(a.rsplit(b'/',1)[-1]==b'dg' for a in args[:2]) and proc.name not in launches:
                cwd=Path(os.readlink(proc/'cwd'))
                source={}
                for relative in ['config.json','config.yaml','pyproject.toml','src/foo_bar/defs/mydefs/definitions.py']:
                    p=cwd/relative
                    if p.is_file():
                        raw=p.read_bytes()
                        source[relative]={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'text':raw.decode(errors='replace')}
                dgscript=next((Path(os.fsdecode(a)) for a in args[:2] if a.rsplit(b'/',1)[-1]==b'dg'),None)
                pths={}
                if dgscript:
                    for p in (dgscript.parent.parent/'lib/python3.12/site-packages').glob('*foo*.pth'):
                        pths[str(p)]=p.read_text()
                launches[proc.name]={'pid':int(proc.name),'cwd':str(cwd),'command':[os.fsdecode(a) for a in args],'sourceAtLaunch':source,'fooBarEditablePths':pths,'atUtc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
            if not args or args[0].rsplit(b'/',1)[-1]!=b'tail' or b'-F' not in args or b'+0' not in args:continue
            path=Path(os.fsdecode(args[-1]))
            if path.suffix not in ['.err','.out'] or not path.is_absolute():continue
            hold(path,'tail-process-target',int(proc.name))
        except (FileNotFoundError,ProcessLookupError,PermissionError,IsADirectoryError):pass
    if notify<0:time.sleep(0.25)
index=[]
for i,((dev,inode),entry) in enumerate(held.items()):
    fd=entry.pop('fd');fd.seek(0);raw=fd.read();info=os.fstat(fd.fileno());fd.close()
    name=f'capture-{i:03d}'+Path(entry['path']).suffix
    (out/name).write_bytes(raw)
    index.append({**entry,'tailPids':sorted(entry['tailPids']),'seenVia':sorted(entry['seenVia']),'device':dev,'inode':inode,'finalLinkCount':info.st_nlink,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'artifact':name,'containsConfig7':b'CONFIG: 7' in raw,'containsStepStart':b'STEP_START' in raw,'mtimeNs':info.st_mtime_ns})
(out/'capture-index.json').write_text(json.dumps({'label':label,'finishedAtUtc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'seconds':time.monotonic()-start,'cpuSeconds':time.process_time()-cpu_start,'files':index,'launchProcesses':list(launches.values()),'captureCompleteness':'Only targets observed while tail process existed; absent files are not proof of absent capture. Holding read-only file descriptors adds bounded memory/CPU overhead and is not a zero-impact claim.'},indent=2)+'\n')
