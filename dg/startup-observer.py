"""Approved, read-only runtime observations; no test or subprocess result changes."""
import sys

# All other Python invocations perform only this argv check. Even that adds a
# small startup cost; import_perf must pass its real official assertions.
if sys.argv and sys.argv[0].startswith('<DG_RUNTIME>/.tox/') and sys.argv[0].endswith('/bin/pytest'):
    import atexit
    import datetime
    import hashlib
    import json
    import os
    from pathlib import Path

    tag=os.environ.get('CI_DG_REPORT_TAG','unknown')
    report=Path('<EVIDENCE>')/('runtime-observer-'+tag+'.json')
    site=Path(sys.prefix)/'lib/python3.12/site-packages'
    def snapshot():
        entries=[]
        for dist in sorted(site.glob('*.dist-info')):
            row={'directory':dist.name}
            for name in ['METADATA','RECORD','direct_url.json']:
                p=dist/name
                row[name]=hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None
            entries.append(row)
        pths={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(site.glob('*.pth'))}
        content={'distributions':entries,'pth':pths}
        return {'sha256':hashlib.sha256(json.dumps(content,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'content':content}
    record={'atInterpreterStartUtc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'argv0':sys.argv[0],'prefix':sys.prefix,'UV_PROJECT_ENVIRONMENT_at_start':os.environ.get('UV_PROJECT_ENVIRONMENT'),'outerEnvironmentBefore':snapshot(),'observerScope':'No original function/arguments/environment/return values modified; profile removed immediately after original scrub return.','originalProfileWasNone':sys.getprofile() is None}
    def save():
        report.write_text(json.dumps(record,indent=2)+'\n')
    def profile(frame,event,arg):
        code=frame.f_code
        if event=='return' and code.co_name=='scrub_tox_uv_project_environment' and code.co_filename.endswith('/dagster_test/dg_utils/utils.py'):
            sys.setprofile(None)
            record['scrubReturn']={'atUtc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'originalFunctionFile':code.co_filename,'UV_PROJECT_ENVIRONMENT_after_original_return':os.environ.get('UV_PROJECT_ENVIRONMENT'),'guardActuallyCleared':bool(record['UV_PROJECT_ENVIRONMENT_at_start'] and '/.tox/' in record['UV_PROJECT_ENVIRONMENT_at_start'] and os.environ.get('UV_PROJECT_ENVIRONMENT') is None),'profileRemovedImmediately':sys.getprofile() is None}
            save()
        elif event=='call' and code.co_name=='pytest_runtestloop':
            sys.setprofile(None)
            record['guardNotObservedBeforeTestLoop']=True
            save()
    def finish():
        after=snapshot()
        record['outerEnvironmentAfter']=after
        record['outerDistributionMetadataRecordPthUnchanged']=after['sha256']==record['outerEnvironmentBefore']['sha256']
        record['finishedAtUtc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
        save()
    save()
    atexit.register(finish)
    if record['originalProfileWasNone']:
        sys.setprofile(profile)
