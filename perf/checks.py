import sys
sys.dont_write_bytecode=True
if not __debug__:raise SystemExit('Refuse optimized Python: required validation checks must execute')

def host_reasons(samples,measurement,cpus):
 reasons=[]
 if len(samples)<2:return ['missing host snapshots']
 if measurement.get('observed_leader_affinities')!=[cpus]:reasons.append('binary leader affinity changed or missing')
 def vals(value):return {k:int(x) for k,x in (line.split() for line in value.splitlines())}
 for h in samples:
  if set(h.get('cgroups',{}))!=set(samples[0].get('cgroups',{})):reasons.append('cgroup hierarchy changed during observation')
  for path,initial in samples[0].get('cgroups',{}).items():
   current=h.get('cgroups',{}).get(path,{})
   for limit in ['cpu.max','memory.max']:
    if (limit in initial)!=(limit in current) or (limit in initial and current[limit]!=initial[limit]):reasons.append(path+'/'+limit+' changed or disappeared')
  if any(k not in h.get('swap',{}) for k in ['pswpin','pswpout']):reasons.append('required swap counters unavailable')
  mem=h.get('pressure',{}).get('memory',{})
  if any('total' not in mem.get(k,{}) for k in ['some','full']):reasons.append('required memory PSI counters unavailable')
  if not h.get('cgroups'):reasons.append('required cgroup hierarchy unavailable')
  for path,state in h.get('cgroups',{}).items():
   for limit,stat,keys in [('cpu.max','cpu.stat',['nr_throttled']),('memory.max','memory.events',['oom','oom_kill','high','max'])]:
    if limit not in state:
     # The actual cgroup-v2 root has no resource-limit interface. Record this
     # as not applicable, never as a measured zero. Non-root loss is invalid.
     if not state.get('root'):reasons.append(path+'/'+limit+' unavailable')
     continue
    if stat not in state or any(k not in vals(state[stat]) for k in keys):reasons.append(path+'/'+stat+' required counters unavailable')
 first,last=samples[0],samples[-1]
 for k in ['pswpin','pswpout']:
  if k in first.get('swap',{}) and k in last.get('swap',{}) and last['swap'][k]>first['swap'][k]:reasons.append(k+' increased')
 for path,state in first.get('cgroups',{}).items():
  end=last.get('cgroups',{}).get(path)
  if end is None:reasons.append('cgroup hierarchy changed');continue
  for limit,field,keys in [('cpu.max','cpu.stat',['nr_throttled']),('memory.max','memory.events',['oom','oom_kill','high','max'])]:
   if limit not in state:continue
   a,b=vals(state.get(field,'')),vals(end.get(field,''))
   for k in keys:
    if k in a and k in b and b[k]>a[k]:reasons.append(path+'/'+field+':'+k+' increased')
 for kind,limit in [('full',0),('some',100000)]:
  a=first.get('pressure',{}).get('memory',{}).get(kind,{});b=last.get('pressure',{}).get('memory',{}).get(kind,{})
  if 'total' in a and 'total' in b and b['total']-a['total']>limit:reasons.append('memory PSI '+kind+' exceeded')
 for cpu,a in first.get('cpu',{}).items():
  if cpu not in last.get('cpu',{}):reasons.append('selected CPU accounting disappeared');continue
  b=last['cpu'][cpu];d=[y-x for x,y in zip(a[:8],b[:8])]
  if len(d)>7 and sum(d)>0 and d[7]/sum(d)>.01:reasons.append(cpu+' steal>1%')
 return sorted(set(reasons))
