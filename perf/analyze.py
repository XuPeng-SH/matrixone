import sys
sys.dont_write_bytecode=True
if not __debug__:raise SystemExit('Refuse optimized Python: required validation checks must execute')
from pathlib import Path
import collections,hashlib,json,random,re,statistics,sys
from checks import host_reasons

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def compare(ps,seed):
 point=(statistics.median(sum([p['fix'] for p in ps],[]))/statistics.median(sum([p['control'] for p in ps],[]))-1)*100;pair=[(statistics.median(p['fix'])/statistics.median(p['control'])-1)*100 for p in ps];cache={};draws=[];rng=random.Random(seed)
 for _ in range(20000):
  counts=[0]*6
  for j in range(6):counts[rng.randrange(6)]+=1
  key=tuple(counts)
  if key not in cache:
   meds={role:statistics.median([x for i,p in enumerate(ps) for n in range(counts[i]) for x in p[role]]) for role in ['control','fix']};cache[key]=(meds['fix']/meds['control']-1)*100
  draws.append(cache[key])
 draws.sort();return {'pooled_delta_pct':point,'conditional95_whole_process_pair_bootstrap_ci_pct':[draws[499],draws[19499]],'pair_delta_pct':pair,'pair_inverse_delta_pct':[(1/(1+x/100)-1)*100 for x in pair],'supplementary_pair_minmax_pct':[min(pair),max(pair)],'bootstrap_seed':seed}
def analyze(A,name):
 reg=json.loads((A/'harness/registration.json').read_text());stage=next(s for s in reg['studies'] if s['name']==name);si=reg['studies'].index(stage);receipt=A/(name+'-receipt.json');meta=json.loads(receipt.read_text());build=json.loads((A/'build.json').read_text());assert meta['registration_sha256']==sha(A/'harness/registration.json') and meta['build_receipt_sha256']==sha(A/'build.json');assert meta['complete'] and len(meta['runs'])==24 and meta['normal_user_uid']!=0
 expected=[(p['id'],p['kind'],p['block'],r['role'],r['binary']) for p in stage['pairs'] for r in p['runs']];actual=[(r['pair_id'],r['kind'],r['block'],r['role'],r['binary']) for r in meta['runs']];assert actual==expected and len({r['name'] for r in meta['runs']})==24
 data={};processes=[];invalid=[]
 for r in meta['runs']:
  assert r['complete'] and r['group_terminated'] and r['exit_code']==0 and r['normal_user_uid']==meta['normal_user_uid'] and r['binary_sha256']==build['binary_manifest'][r['binary']]['sha256'];p=A/r['log'];assert sha(p)==r['log_sha256'] and sha(A/r['host_file'])==r['host_sha256'] and sha(A/r['timing_file'])==r['timing_sha256'];timing=json.loads((A/r['timing_file']).read_text());assert timing==r['measurement'] and timing['normal_user_uid']==meta['normal_user_uid'] and timing['selected_cpus']==meta['selected_cpus'] and timing['exit_code']==0
  host=json.loads((A/r['host_file']).read_text());assert len(host)>=2;reasons=host_reasons(host,timing,meta['selected_cpus']);assert reasons==r['host_invalid_reasons'];invalid.extend({'name':r['name'],'reason':v} for v in reasons);s=p.read_text();assert '\nPASS\n' in s;caseRaw={};current=None
  for line in s.splitlines():
   m=re.match(r'Benchmark(?:OrderedODKU|GeneratedDefault)/([^\s-]+)',line)
   if m:current=m[1]
   m=re.search(r'timed_exec_ns=\[([^]]+)\]',line)
   if m:
    v=list(map(int,m[1].split()))
    if len(v)==stage['count']:assert current and current not in caseRaw;caseRaw[current]=v
  assert list(caseRaw)==stage['cases']
  for case,v in caseRaw.items():
   assert all(x>0 for x in v);m=re.search(r'Benchmark(?:OrderedODKU|GeneratedDefault)/'+re.escape(case)+r'-\d+\s+'+str(stage['count'])+r'\s+([\d.eE+-]+) ns/op',s);mean=statistics.mean(v);assert m and abs(float(m[1])-mean)<=max(1,mean/1e6);data.setdefault(case,{}).setdefault(r['pair_id'],{})[r['role']]=v;step=50 if len(v)==501 else 7
   processes.append({'name':r['name'],'pair_id':r['pair_id'],'role':r['role'],'binary':r['binary'],'case':case,'raw_ns':v,'median_ns':statistics.median(v),'age_profile':[{'start':i,'end':min(i+step,len(v)),'median_ns':statistics.median(v[i:i+step])} for i in range(0,len(v),step)]})
 out={'registration_sha256':sha(A/'harness/registration.json'),'sealed_stage_receipt_sha256':sha(receipt),'stage':name,'processes':processes,'cases':{},'host_invalid_reasons':invalid,'primary_pass':not invalid,'conditional_only':True}
 for ci,case in enumerate(stage['cases']):
  limit=5 if case.startswith('single') else 10;groups={kind:compare([data[case][f'{kind}-{i+1}'] for i in range(6)],reg['bootstrap_seed']+si*100+ci) for kind in ['main','aa']};main,aa=groups['main'],groups['aa'];mp=main['pooled_delta_pct']<=limit and main['conditional95_whole_process_pair_bootstrap_ci_pct'][1]<=limit and max(main['pair_delta_pct'])<=limit;ap=max(aa['pair_delta_pct']+aa['pair_inverse_delta_pct'])<=limit;out['cases'][case]={'limit_pct':limit,**groups,'main_gate_pass':mp,'aa_resolution_pass':ap,'pass':mp and ap};out['primary_pass']&=mp and ap
 out['disposition']='Conditional numerical gates passed; final comparability and overall review remain' if out['primary_pass'] else 'STOP later stages: main bound open or AA/host resolution inconclusive; no rerun'
 return out
if __name__=='__main__':
 A=Path(sys.argv[1]);name=sys.argv[2];out=analyze(A,name);p=A/(name+'-results.json');assert not p.exists();p.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({k:v for k,v in out.items() if k not in ['processes']},indent=2));raise SystemExit(0 if out['primary_pass'] else 42)
