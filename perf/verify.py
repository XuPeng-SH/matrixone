import sys
sys.dont_write_bytecode=True
if not __debug__:raise SystemExit('Refuse optimized Python: required validation checks must execute')
from pathlib import Path
import json,sys,hashlib
from analyze import analyze,sha
A=Path(sys.argv[1]);final=json.loads((A/'run-receipt.json').read_text());assert final['normal_user_uid']!=0 and final['ended']>=final['started']
actual={str(p.relative_to(A)) for p in A.rglob('*') if p.is_file() and p!=A/'run-receipt.json'}
assert actual==set(final['files']),'Unexpected/missing result files'
for name,h in final['files'].items():assert sha(A/name)==h,name
if final.get('fallback'):
 assert final['status'] in ['PRE_CONTROLLER_INFRASTRUCTURE_FAILURE','INCOMPLETE_EVIDENCE_FALLBACK']
 print('Verified incomplete infrastructure/fallback evidence; no performance gates passed');raise SystemExit(0)
assert final['registration_sha256']==sha(A/'harness/registration.json');manifest=json.loads((A/'harness/inputs-manifest.json').read_text());assert sha(A/'harness/inputs-manifest.json')==final['harness_manifest_sha256']
expected_harness={name.removeprefix('perf/') for name in manifest if name.startswith('perf/')}|{'inputs-manifest.json'}
assert {str(p.relative_to(A/'harness')) for p in (A/'harness').rglob('*') if p.is_file()}==expected_harness
for name,h in manifest.items():
 p=A/'harness'/name.removeprefix('perf/') if name.startswith('perf/') else A/'workflow.yml';assert sha(p)==h,name
if 'build_receipt_sha256' in final:assert sha(A/'build.json')==final['build_receipt_sha256']
reg=json.loads((A/'harness/registration.json').read_text());known=[s['name'] for s in reg['studies']];seen=[]
for row in final['stage_receipts']:
 p=A/row['path'];assert sha(p)==row['sha256'];stage=json.loads(p.read_text());assert stage['stage']==known[len(seen)];seen.append(stage['stage'])
 for r in stage['runs']:
  for fk,hk in [('log','log_sha256'),('host_file','host_sha256'),('timing_file','timing_sha256')]:
   if fk in r:assert sha(A/r[fk])==r[hk]
 if stage['complete']:
  actual=analyze(A,stage['stage']);reported=A/(stage['stage']+'-results.json')
  if reported.exists():assert actual==json.loads(reported.read_text())
  if actual['primary_pass'] is False:assert len(final['stage_receipts'])==len(seen),'Measured after failed stage'
 if not stage['complete']:assert final['status']=='INCOMPLETE' and len(final['stage_receipts'])==len(seen)
if final['status'].startswith('NUMERICAL_GATES_PASS'):
 assert seen==known and all(json.loads((A/(s+'-results.json')).read_text())['primary_pass'] for s in seen)
print('Final sealed provenance verified; status:',final['status'],'stages:',seen)
