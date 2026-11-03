import sys
sys.dont_write_bytecode=True
if not __debug__:raise SystemExit('Refuse optimized Python: required validation checks must execute')
from pathlib import Path
from checks import host_reasons
import collections,hashlib,json,os,shutil,signal,subprocess,sys,threading,time,traceback
P=Path(__file__).resolve().parent;WORK=Path(os.environ['GITHUB_WORKSPACE']);D=Path(os.environ['RUNNER_TEMP'])/'odku31979';R=D/'results';R.mkdir(parents=True,exist_ok=True);assert {p.name for p in R.iterdir()}<= {'bootstrap.json'},'No repeat/resume in existing experiment directory';REG=json.loads((P/'registration.json').read_text());M=json.loads((P/'source-map.json').read_text());B=WORK/'baseline';C=WORK/'candidate';H=WORK/'published-head';ROOTS={'base':B,'fix':C,'genfix':C,'genhead':H};cutoff=float(os.environ['CI_JOB_START'])+REG['timeouts']['internal_cutoff_minutes']*60;active=None;mono_cutoff=time.monotonic()+max(0,cutoff-time.time());records={'registration_sha256':None,'normal_user_uid':os.getuid(),'stage_receipts':[],'builds':[],'status':'INCOMPLETE','started':time.time(),'cleanup_errors':[]};env=os.environ.copy();env.update(GOWORK='off',GOTOOLCHAIN='local',GOAMD64='v3',GOEXPERIMENT='simd',GOMAXPROCS='4',GOMEMLIMIT='3GiB',GOCACHE=str(D/'cache'),GOMODCACHE=str(D/'modules'),TMPDIR=str(D/'tmp'),GOPROXY='https://proxy.golang.org,direct')
(R/'controller-started.json').write_text(json.dumps({'started':time.time(),'run_id':os.environ.get('GITHUB_RUN_ID'),'attempt':os.environ.get('GITHUB_RUN_ATTEMPT')}))
for x in ['cache','modules','tmp','cluster-tmp']:(D/x).mkdir(exist_ok=True)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
def textcmd(cmd,cwd=None,e=None):return subprocess.check_output(cmd,cwd=cwd,env=e or env,text=True).strip()
def git(root,*args,e=None):return textcmd(['git',*args],root,e)
def group_alive(pgid):
 try:os.killpg(pgid,0);return True
 except ProcessLookupError:return False
def stop_child():
 global active
 if active is None:return True
 child=active;pgid=child.pid
 if group_alive(pgid):
  
  try:os.killpg(pgid,signal.SIGTERM)
  except ProcessLookupError:pass
  until=time.monotonic()+10
  while group_alive(pgid) and time.monotonic()<until:child.poll();time.sleep(.05)
  if group_alive(pgid):
   
   try:os.killpg(pgid,signal.SIGKILL)
   except ProcessLookupError:pass
   until=time.monotonic()+3
   while group_alive(pgid) and time.monotonic()<until:child.poll();time.sleep(.05)
 try:child.wait(timeout=1)
 except subprocess.TimeoutExpired:pass
 ok=not group_alive(pgid)
 if not ok:records['cleanup_errors'].append({'process_group':pgid,'error':'owned group not confirmed terminated'})
 active=None
 return ok

def interrupted(signum,frame):raise KeyboardInterrupt(f'signal {signum}')
signal.signal(signal.SIGTERM,interrupted)
def run(cmd,name,cwd,e=None,seconds=3600):
 global active
 remaining=min(cutoff-time.time(),mono_cutoff-time.monotonic());assert remaining>20,'Internal cutoff reserves cleanup/upload time';start=time.time()
 save(R/'active-command.json',{'name':name,'command':list(map(str,cmd)),'start':start})
 with (R/(name+'.log')).open('wb') as out:
  active=subprocess.Popen(cmd,cwd=cwd,env=e or env,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
  try:code=active.wait(timeout=min(seconds,remaining-10))
  except BaseException:stop_child();raise
  pgid=active.pid
  if group_alive(pgid):
   stop_child();raise RuntimeError('Descendant remained after wrapper exit')
  active=None
 return {'process_group':pgid,'group_terminated':True,'command':list(map(str,cmd)),'exit_code':code,'start':start,'end':time.time(),'elapsed_s':time.time()-start,'log':name+'.log','log_sha256':sha(R/(name+'.log'))}
def cgpaths():
 root=Path('/sys/fs/cgroup');rows=[]
 for line in Path('/proc/self/cgroup').read_text().splitlines():
  if line.startswith('0::'):
   p=(root/line.split('::',1)[1].lstrip('/')).resolve()
   while p.is_relative_to(root):
    rows.append(p)
    if p==root:break
    p=p.parent
 return rows
CG=cgpaths()
def snapshot():
 r={'time':time.time(),'cpu':{},'pressure':{},'cgroups':{},'freq':{},'swap':{}}
 for line in Path('/proc/stat').read_text().splitlines():
  if line.split()[0] in ['cpu'+str(c) for c in CPUS]:r['cpu'][line.split()[0]]=list(map(int,line.split()[1:]))
 for line in Path('/proc/vmstat').read_text().splitlines():
  if line.startswith(('pswpin ','pswpout ')):k,v=line.split();r['swap'][k]=int(v)
 r['memory']='\n'.join(x for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith(('MemAvailable:','SwapFree:')))
 for k in ['cpu','io','memory']:
  p=Path('/proc/pressure')/k
  if p.exists():
   r['pressure'][k]={}
   for l in p.read_text().splitlines():
    bits=l.split();r['pressure'][k][bits[0]]={a:float(b) for a,b in (v.split('=') for v in bits[1:])}
 for p in cgpaths():
  state={'root':p==Path('/sys/fs/cgroup'),'unavailable':[]}
  for name in ['cpu.max','cpu.stat','memory.max','memory.events']:
   q=p/name
   if q.exists():state[name]=q.read_text().strip()
   else:state['unavailable'].append(name)
  r['cgroups'][str(p)]=state
 for c in CPUS:
  q=Path(f'/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_cur_freq')
  r['freq'][str(c)]=q.read_text().strip() if q.exists() else None
 r['unavailable']={'swap':[k for k in ['pswpin','pswpout'] if k not in r['swap']],'memory_psi':[k for k in ['some','full'] if 'total' not in r['pressure'].get('memory',{}).get(k,{})],'frequency':[k for k,v in r['freq'].items() if v is None],'cgroup_hierarchy':not bool(r['cgroups']),'steal':[str(c) for c in CPUS if len(r['cpu'].get('cpu'+str(c),[]))<=7]}
 return r

CPUS=[];NATIVE=None
def check_source():
 assert git(B,'rev-parse','HEAD')==REG['baseline'];assert git(H,'rev-parse','HEAD')==REG['published_head'];assert git(C,'rev-parse','HEAD')==REG['published_head']
 assert not git(B,'diff','--name-only','HEAD') and not git(H,'diff','--name-only','HEAD')
 assert sorted(git(C,'diff','--name-only','HEAD').splitlines())==sorted(PACKAGE['intended_patch_paths'])
 for x in M['paths']:assert sha(C/x['path'])==x['worktree_sha256'],x['path']
 for root in [B,C,H]:
  for name in git(root,'ls-files','--others','--exclude-standard','--','*.go').splitlines():assert name.startswith('thirdparties/') and any(part.startswith('_') for part in Path(name).parts),name
 if NATIVE is not None:
  for root in [B,C,H]:
   for name,h in NATIVE['files'].items():assert sha(root/name)==h
 for name in ['go.mod','go.sum']:assert sha(B/name)==sha(C/name)==sha(H/name)
 for name in ['normal_benchmark_test.go','generated_benchmark_test.go']:
  key='normal_fixture_sha256' if name.startswith('normal') else 'generated_fixture_sha256';assert sha(P/name)==REG[key]
try:
 records['registration_sha256']=sha(P/'registration.json');shutil.copytree(P,R/'harness',ignore=shutil.ignore_patterns('__pycache__'));shutil.copyfile(P.parent/'.github/workflows/odku31979-perf.yml',R/'workflow.yml');records['initial_free_disk_bytes']=shutil.disk_usage(D).free
 assert os.environ.get('GITHUB_RUN_ATTEMPT')=='1' and os.environ.get('GITHUB_EVENT_NAME')=='push' and json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())['created'] is True,'Only the intended creation push, attempt1, can measure'
 assert git(P.parent,'rev-parse','HEAD')==os.environ['GITHUB_SHA']
 assert os.geteuid()!=0 and os.environ.get('GITHUB_ACTIONS')=='true';assert records['initial_free_disk_bytes']>=30*1024**3,'Ephemeral VM lacks preregistered30GiB beforebuild'
 assert sha(P/'source-map.json')==REG['source_map_sha256'] and sha(P/'source-package.json')==REG['source_package_sha256'];PACKAGE=json.loads((P/'source-package.json').read_text());assert sha(P/'candidate.patch')==REG['patch_sha256']
 manifest=json.loads((P/'inputs-manifest.json').read_text())
 for name,h in manifest.items():assert sha(P.parent/name)==h,name
 payload={str(p.relative_to(P.parent)) for p in P.parent.rglob('*') if p.is_file() and p.relative_to(P.parent).parts[0]!='.git'};assert payload==set(manifest)|{'perf/inputs-manifest.json'},'Unexpected public payload files'
 allowed=sorted(os.sched_getaffinity(0));topology={};used=set()
 for cpu in allowed:
  root=Path(f'/sys/devices/system/cpu/cpu{cpu}/topology');key=tuple((root/n).read_text().strip() if (root/n).exists() else '?' for n in ['physical_package_id','core_id']);topology[str(cpu)]=key
  if key not in used:CPUS.append(cpu);used.add(key)
  if len(CPUS)==2:break
 if len(CPUS)<2:CPUS=allowed[:2]
 assert len(CPUS)==2;records['selected_cpus']=CPUS;records['reported_topology']=topology;records['allowed_cpus']=allowed
 for p in CG:
  q=p/'cpu.max'
  if q.exists():
   quota,period=q.read_text().split();assert quota=='max' or int(quota)/int(period)>=2,'CPU quota belowtwo'
 r=run(['cc','-O2',str(P/'check_v3.c'),'-o',str(D/'check_v3')],'compile-cpu-probe',WORK);assert r['exit_code']==0
 for cpu in CPUS:
  r=run(['taskset','-c',str(cpu),str(D/'check_v3')],f'cpu-check-{cpu}',WORK);assert r['exit_code']==0
 records['host']={'uname':textcmd(['uname','-a']),'lscpu':textcmd(['lscpu']),'go':textcmd(['go','version']),'cc':textcmd(['cc','--version']),'image_version':os.environ.get('ImageVersion'),'runner_os':os.environ.get('RUNNER_OS'),'go_env':textcmd(['go','env','GOOS','GOARCH','GOAMD64','GOEXPERIMENT','GOVERSION','CGO_ENABLED']),'snapshot':snapshot()};assert records['host']['go'].startswith('go version go1.26.4 ')
 preflight=snapshot();records['host_counter_preflight']={'snapshot':preflight,'reasons':host_reasons([preflight,preflight],{'observed_leader_affinities':[CPUS]},CPUS),'capability_check_only':True};assert not records['host_counter_preflight']['reasons'],'Required host counters unavailable before measurement'
 textcmd(['git','apply','--check',str(P/'candidate.patch')],C);textcmd(['git','apply',str(P/'candidate.patch')],C)
 for d in ['cgo','thirdparties']:
  assert git(B,'rev-parse','HEAD:'+d)==git(C,'rev-parse','HEAD:'+d)==git(H,'rev-parse','HEAD:'+d)
 alt=env.copy();alt['GIT_ALTERNATE_OBJECT_DIRECTORIES']=str(B/'.git/objects')+os.pathsep+str(H/'.git/objects');idx=D/'source.index';alt['GIT_INDEX_FILE']=str(idx)
 textcmd(['git','read-tree',REG['published_head']],C,alt);textcmd(['git','apply','--cached',str(P/'candidate.patch')],C,alt);assert git(C,'write-tree',e=alt)==REG['candidate_tree'];assert set(git(C,'diff','--name-only',REG['baseline'],REG['candidate_tree'],e=alt).splitlines())=={p['path'] for p in M['paths']};idx.unlink();check_source()
 for name,cmd in [('native-thirdparties',['make','-C','thirdparties']),('native-cgo',['make','-C','cgo','-j2'])]:
  print('Building',name,flush=True);r=run(cmd,name,B);records['builds'].append({'name':name,**r});assert r['exit_code']==0
 for root in [C,H]:
  shutil.copytree(B/'thirdparties/install',root/'thirdparties/install',symlinks=True)
  for name in ['libmo.so','libmo.a']:shutil.copyfile(B/'cgo'/name,root/'cgo'/name)
 native={'input_trees':{d:git(B,'rev-parse','HEAD:'+d) for d in ['cgo','thirdparties']},'files':{str(p.relative_to(B)):sha(p) for p in [B/'cgo/libmo.so',B/'cgo/libmo.a',*(B/'thirdparties/install/lib').glob('*')] if p.is_file()},'onnx':'SHA-pinned prebuiltONNX Runtime1.26.0 perthirdpartiesMakefile, othernativeoutputs builtinthisVM'}
 for root in [C,H]:
  for name,h in native['files'].items():assert sha(root/name)==h
 ne=env.copy();ne['LD_LIBRARY_PATH']=str(B/'cgo')+':'+str(B/'thirdparties/install/lib');native['ldd_libmo']=textcmd(['ldd',str(B/'cgo/libmo.so')],e=ne);assert 'not found' not in native['ldd_libmo'];save(R/'native.json',native);NATIVE=native;records['native_sha256']=sha(R/'native.json');bins={}
 for name in ['base','fix','genfix','genhead']:
  root=ROOTS[name];fixture=P/('generated_benchmark_test.go' if name.startswith('gen') else 'normal_benchmark_test.go');overlay=D/(name+'-overlay.json');save(overlay,{'Replace':{str(root/'pkg/tests/dml/ordered_odku_benchmark_test.go'):str(fixture)}});binary=D/(name+'.test');cmd=[str(root/'.agents/skills/mo-dev/scripts/mo-cgo-test'),'-vet=off','-p=2',f'-overlay={overlay}','-c','-o',str(binary),'./pkg/tests/dml'];print('Building',name,flush=True);r=run(cmd,'build-'+name,root);records['builds'].append({'name':name,**r});assert r['exit_code']==0;bins[name]={'path':str(binary),'sha256':sha(binary),'fixture_sha256':sha(fixture),'overlay':json.loads(overlay.read_text())}
 check_source();save(R/'build.json',{'builds':records['builds'],'binary_manifest':bins,'native_sha256':sha(R/'native.json')});records['build_receipt_sha256']=sha(R/'build.json');runtime=env.copy();runtime.pop('GOMEMLIMIT',None);runtime.pop('GODEBUG',None);runtime.update(GOMAXPROCS='2',OMP_NUM_THREADS='2',TMPDIR=str(D/'cluster-tmp'))
 for stage in REG['studies']:
  st={'registration_sha256':records['registration_sha256'],'build_receipt_sha256':sha(R/'build.json'),'stage':stage['name'],'normal_user_uid':os.getuid(),'selected_cpus':CPUS,'runs':[],'complete':False};stagefile=R/(stage['name']+'-receipt.json')
  try:
   for pair in stage['pairs']:
    for item in pair['runs']:
     check_source();assert sha(Path(bins[item['binary']]['path']))==bins[item['binary']]['sha256'];assert shutil.disk_usage(D).free>4*1024**3;root=ROOTS[item['binary']];runtime['LD_LIBRARY_PATH']=str(root/'cgo')+':'+str(root/'thirdparties/install/lib');name=stage['name']+'-'+pair['id']+'-'+item['role'];cmd=['python3',str(P/'isolated-run.py'),'python3',str(P/'measure.py'),str(R/(name+'.time.json')),','.join(map(str,CPUS)),bins[item['binary']]['path'],'-test.run=^$',f'-test.bench={stage["bench"]}',f'-test.benchtime={stage["count"]}x','-test.count=1','-test.v','-test.timeout=15m'];samples=[snapshot()];observer_errors=[];stop=threading.Event()
     def observe():
      try:
       while not stop.wait(1):samples.append(snapshot())
      except Exception as e:observer_errors.append(repr(e))
     thread=threading.Thread(target=observe,daemon=True);thread.start();before=set((D/'cluster-tmp').glob('mo-cluster-test-*'));print('Running',name,len(st['runs'])+1,'/24',flush=True)
     r=None;attempt_error=None
     attempt={'name':name,'pair_id':pair['id'],'kind':pair['kind'],'block':pair['block'],**item,'binary_sha256':bins[item['binary']]['sha256'],'normal_user_uid':os.getuid(),'complete':False,'command':cmd}
     try:r=run(cmd,name,root/'pkg/tests/dml',runtime,900)
     except BaseException as e:attempt_error=e;attempt['interruption']=repr(e)
     finally:
      stop.set();thread.join(5)
      if thread.is_alive():observer_errors.append('observer did not terminate')
      try:samples.append(snapshot())
      except Exception as e:observer_errors.append(repr(e))
      save(R/(name+'-host.json'),samples);attempt.update(host_file=name+'-host.json',host_sha256=sha(R/(name+'-host.json')),observer_errors=observer_errors)
      if (R/(name+'.log')).exists():attempt.update(log=name+'.log',log_sha256=sha(R/(name+'.log')))
      if (R/(name+'.time.json')).exists():
       attempt.update(timing_file=name+'.time.json',timing_sha256=sha(R/(name+'.time.json')))
       try:attempt['measurement']=json.loads((R/(name+'.time.json')).read_text())
       except (ValueError,OSError) as e:
        attempt['timing_parse_error']=repr(e)
        if attempt_error is None:attempt_error=e
      if r is not None:attempt.update(r)
      st['runs'].append(attempt)
      if not records['cleanup_errors']:
       for p in set((D/'cluster-tmp').glob('mo-cluster-test-*'))-before:
        assert p.parent==D/'cluster-tmp' and p.is_dir() and not p.is_symlink();shutil.rmtree(p)
     if attempt_error is not None:raise attempt_error
     assert r['exit_code']==0 and 'measurement' in attempt and not observer_errors
     attempt['host_invalid_reasons']=host_reasons(samples,attempt['measurement'],CPUS);attempt['complete']=True
     save(R/'progress.json',{'stage':stage['name'],'completed':len(st['runs']),'last_exit':r['exit_code']})
   st['complete']=True
  finally:
   stop_child();assert not stagefile.exists();save(stagefile,st);records['stage_receipts'].append({'path':stagefile.name,'sha256':sha(stagefile)})
  analysis=run(['python3',str(P/'analyze.py'),str(R),stage['name']],'analyze-'+stage['name'],WORK);assert analysis['exit_code']==0,'Stage failed/inconclusive; STOP all later stages'
 records['status']='NUMERICAL_GATES_PASS_PENDING_COMPARABILITY_AND_FINAL_REVIEW'
except BaseException as e:
 records['error']=repr(e);records['traceback']=traceback.format_exc();print(records['traceback'],flush=True)
finally:
 stop_child()
 if records['cleanup_errors']:records['status']='INCOMPLETE'
 records['ended']=time.time();records['harness_manifest_sha256']=sha(P/'inputs-manifest.json');records['files']={str(p.relative_to(R)):sha(p) for p in sorted(R.rglob('*')) if p.is_file() and p!=R/'run-receipt.json'};save(R/'run-receipt.json',records)
raise SystemExit(0 if records['status'].startswith('NUMERICAL_GATES_PASS') else 1)
