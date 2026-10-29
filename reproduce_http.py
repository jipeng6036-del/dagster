"""Real requests + Dagster materialization against a loopback dbt Cloud fixture."""
import argparse, hashlib, json, os, sys, threading, traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from dagster import AssetSelection, define_asset_job
from dagster._core.definitions.assets.graph.asset_graph import AssetGraph
from dagster._core.test_utils import instance_for_test
from dagster_dbt import DbtCloudClientResource, load_assets_from_dbt_cloud_job
from dagster_dbt.cloud.resources import dbt_cloud_resource
import dagster_dbt.cloud.asset_defs as imported_source

ap=argparse.ArgumentParser();ap.add_argument('--label',required=True);ap.add_argument('--output',required=True);args=ap.parse_args()
out=Path(args.output);repo=Path(imported_source.__file__).parents[5]
fixtures=Path(os.environ['DAGSTER_DBT_FIXTURE_DIR'])
manifest=json.loads((fixtures/'sample_manifest.json').read_text());run_results=json.loads((fixtures/'sample_run_results.json').read_text())
records=[]
for docs in [True,False]:
 for cached in [True,False]:
  for legacy in [True,False]:
   state={'docs':docs,'calls':[],'runs':{},'commands':['dbt test','dbt run','dbt run-operation upload_artifacts']}
   class Handler(BaseHTTPRequestHandler):
    def log_message(self,*a):pass
    def reply(self,status,payload):
     data=json.dumps(payload).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def dispatch(self):
     path=urlparse(self.path);query=parse_qs(path.query);size=int(self.headers.get('Content-Length','0'));body=json.loads(self.rfile.read(size) or b'null')
     call={'method':self.command,'path':path.path,'query':query,'body':body};state['calls'].append(call)
     if self.headers.get('Authorization')!='Bearer loopback-fixture':return self.reply(401,{'error':'unexpected fixture auth'})
     if path.path=='/api/v2/accounts/30000/jobs/123/' and self.command=='GET':
      return self.reply(200,{'data':{'project_id':12,'environment_id':376530,'id':123,'name':'Loopback dbt job','generate_docs':docs,'execute_steps':state['commands'],'triggers':{'schedule':False}}})
     if path.path=='/api/v3/accounts/30000/projects/12/environment-variables/job':return self.reply(200,{'data':{}})
     if path.path=='/api/v2/accounts/30000/jobs/123/run/' and self.command=='POST':
      run_id=1000+len(state['runs']);commands=body['steps_override'];state['runs'][run_id]={'commands':commands,'step':2+next((i for i,c in enumerate(commands) if c.startswith(('dbt run','dbt build'))),0)}
      return self.reply(200,{'data':{'id':run_id,'href':f'http://127.0.0.1:{server.server_port}/runs/{run_id}/'}})
     parts=path.path.strip('/').split('/')
     if len(parts)>=6 and parts[:5]==['api','v2','accounts','30000','runs']:
      run_id=int(parts[5]);run=state['runs'][run_id]
      if len(parts)==6:
       return self.reply(200,{'data':{'status_humanized':'Success','job':None,'account_id':30000,'job_id':123,'id':run_id,'run_steps':[{'name':'dbt deps'}]+[{'name':c} for c in run['commands']]+([{'name':'dbt docs generate'}] if docs else [])}})
      if len(parts)==8 and parts[6]=='artifacts':
       step=query.get('step',[None])[0]
       if step is not None and int(step)!=run['step']:
        call['status']=404;return self.reply(404,{'error':'artifact does not belong to the materialization step','expected_step':run['step'],'received_step':step})
       call['status']=200
       return self.reply(200,manifest if parts[7]=='manifest.json' else run_results)
     call['status']=404;return self.reply(404,{'error':'unknown fixture endpoint'})
    do_GET=dispatch
    do_POST=dispatch
   server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
   record={'generate_docs':docs,'rebuild_from_cache':cached,'resource':'legacy' if legacy else 'modern','loopback_port':server.server_port}
   try:
    config={'auth_token':'loopback-fixture','account_id':30000,'dbt_cloud_host':f'http://127.0.0.1:{server.server_port}/','request_max_retries':0,'request_retry_delay':0}
    resource=dbt_cloud_resource.configured(config) if legacy else DbtCloudClientResource(**config)
    cacheable=load_assets_from_dbt_cloud_job(dbt_cloud=resource,job_id=123);data=cacheable.compute_cacheable_data()
    if cached:cacheable=load_assets_from_dbt_cloud_job(dbt_cloud=resource,job_id=123)
    assets=cacheable.build_definitions(data);job=define_asset_job('loopback_materialize',selection=AssetSelection.assets(*assets)).resolve(asset_graph=AssetGraph.from_assets(assets))
    with instance_for_test() as instance:result=job.execute_in_process(instance=instance)
    record.update(success=result.success,materialization_count=len(result.get_asset_materialization_events()))
    assert result.success and record['materialization_count']==4,record
   except Exception as e:record.update(success=False,error=repr(e));traceback.print_exc()
   finally:
    server.shutdown();server.server_close();thread.join(timeout=2);record['http_calls']=state['calls'];record['runs']=state['runs'];records.append(record)
    print(json.dumps({k:v for k,v in record.items() if k not in ['http_calls','runs']}),flush=True)
source=Path(imported_source.__file__);summary={'label':args.label,'python':sys.version,'source':str(source),'sourceSha256':hashlib.sha256(source.read_bytes()).hexdigest(),'boundary':'Real SDK requests over TCP loopback and Dagster execute_in_process; dbt Cloud service is a local fixture, no cloud account, no paid calls.','cases':records}
(out/(args.label+'-http.json')).write_text(json.dumps(summary,indent=2),encoding='utf8');print('successes',sum(r['success'] for r in records),'of',len(records))
