import sys
sys.dont_write_bytecode=True
if not __debug__:raise SystemExit('Refuse optimized Python: required validation checks must execute')
import json,os,resource,subprocess,sys,time,pathlib
out=pathlib.Path(sys.argv[1]);cpus={int(v) for v in sys.argv[2].split(',')};start=time.monotonic();seen=[]
p=subprocess.Popen(sys.argv[3:],preexec_fn=lambda:os.sched_setaffinity(0,cpus))
while True:
 try:seen.append(sorted(os.sched_getaffinity(p.pid)))
 except ProcessLookupError:pass
 try:code=p.wait(timeout=1);break
 except subprocess.TimeoutExpired:continue
r=resource.getrusage(resource.RUSAGE_CHILDREN);out.write_text(json.dumps({'exit_code':code,'elapsed_s':time.monotonic()-start,'max_rss_kib':r.ru_maxrss,'user_cpu_s':r.ru_utime,'system_cpu_s':r.ru_stime,'major_faults':r.ru_majflt,'minor_faults':r.ru_minflt,'binary_pid':p.pid,'normal_user_uid':os.getuid(),'selected_cpus':sorted(cpus),'observed_leader_affinities':sorted({tuple(v) for v in seen})},indent=2)+'\n');raise SystemExit(code)
