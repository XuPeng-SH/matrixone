import sys
sys.dont_write_bytecode=True
if not __debug__:raise SystemExit('Refuse optimized Python: required validation checks must execute')
import fcntl,os,pathlib,subprocess,sys
# Reserve lower ranges only in this task's private TMPDIR, without touching any
# other cluster or the machine-wide lease directory. Both perf variants use 26000.
root=pathlib.Path(os.environ['TMPDIR'])/'mo-test-port-leases';root.mkdir(parents=True,exist_ok=True)
leases=[]
for base in range(10000,26000,1000):
 f=open(root/f'{base}.lock','a+')
 try:fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
 except BlockingIOError:f.close();continue
 leases.append(f)
print('Task-private cluster lease floor: 26000',flush=True)
try:raise SystemExit(subprocess.call(sys.argv[1:]))
finally:
 for f in leases:f.close()
