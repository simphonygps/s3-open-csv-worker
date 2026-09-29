#!/usr/bin/env python3
"""Prepare exact public dependency wheels for a clean CSV/NDJSON parser candidate.

No app build/deploy or live credential mounts. Downloads binary wheels only
through a pinned clean Python image; records public wheel hashes, not secrets.
"""
import argparse
import datetime as dt
import json
from pathlib import Path
import shlex
import subprocess

BASE = 'python@sha256:70e8e937c1da72df1688e883a2108b0feff03381187fa78eca6d9b10df411320'
BASE_ID = 'sha256:a2a75bf6184fd4e4f996711e12df827be50bdb51b1c99a68027760d5a7629360'
INPUTS = 'docs/3-runtime-testing-and-operations/server-secret-store/evidence/release-inputs-v1.json'

REMOTE = r'''
import datetime,fcntl,hashlib,json,os,re,stat,subprocess,sys,uuid,zipfile
from pathlib import Path
payload=json.load(sys.stdin)
assert set(payload)=={'apply','base','base_id','packages'} and type(payload['apply']) is bool
assert payload['base']=='python@sha256:70e8e937c1da72df1688e883a2108b0feff03381187fa78eca6d9b10df411320'
assert payload['base_id']=='sha256:a2a75bf6184fd4e4f996711e12df827be50bdb51b1c99a68027760d5a7629360'
packages=payload['packages'];assert 0<len(packages)<200
for name,version in packages.items():
 assert re.fullmatch(r'[a-z0-9-]+',name) and re.fullmatch(r'[A-Za-z0-9_.+!-]+',version)
def run(args,timeout=30):
 p=subprocess.run(args,capture_output=True,timeout=timeout)
 if p.returncode:raise ValueError('dependency_preparation_command_failed')
 return p.stdout
def public_file(path,data):
 if path.exists() or path.is_symlink():
  info=path.lstat();assert stat.S_ISREG(info.st_mode) and info.st_uid==0 and info.st_nlink==1 and stat.S_IMODE(info.st_mode)==0o400
  assert path.read_bytes()==data;return
 fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o400)
 try:
  with os.fdopen(fd,'wb',closefd=False) as stream:stream.write(data);stream.flush();os.fsync(fd)
 finally:os.close(fd)
def directory(path):
 if not path.parent.exists():directory(path.parent)
 for parent in reversed(path.parents):
  info=parent.lstat();assert stat.S_ISDIR(info.st_mode) and info.st_uid==0 and not stat.S_IMODE(info.st_mode)&0o022
 if path.exists() or path.is_symlink():
  info=path.lstat();assert stat.S_ISDIR(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o700
 else:path.mkdir(mode=0o700)
report={};phase='base-preflight';container=None
try:
 os.umask(0o077);assert os.geteuid()==0
 record=json.loads(run(['docker','image','inspect',payload['base']]))[0]
 assert record['Id']==payload['base_id']
 assert not any(re.search(r'PASSWORD|SECRET|TOKEN|CREDENTIAL|KEYRING',row.split('=',1)[0]) for row in record['Config']['Env'])
 material=json.dumps({'base':payload['base'],'packages':packages},sort_keys=True,separators=(',',':')).encode()
 artifact=hashlib.sha256(material).hexdigest()
 root=Path('/opt/amberping/dev/build-inputs')/artifact
 before=sorted(run(['docker','ps','--no-trunc','--format','{{.ID}}']).decode().splitlines())
 assert len(before)==24
 report={'schema_version':1,'artifact_id':artifact,'base':payload['base'],'base_id':payload['base_id'],
  'packages':len(packages),'applied':False,'preflight_passed':True,'live_source_built':False,'live_consumers_changed':False}
 if payload['apply']:
  phase='backup-exclusion'
  lock=os.open('/srv/amberping/backups/.global-execution.lock',os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC)
  info=os.fstat(lock);assert stat.S_ISREG(info.st_mode) and info.st_uid==0 and info.st_nlink==1 and stat.S_IMODE(info.st_mode)==0o600
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  phase='public-inputs'
  directory(root);directory(root/'wheels')
  public_file(root/'input.json',material)
  versions=''.join(name+'=='+version+'\n' for name,version in sorted(packages.items())).encode()
  public_file(root/'requirements.versions.txt',versions)
  if not (root/'wheels.json').exists():
   phase='binary-wheel-download'
   container='amberping-sss-wheels-'+str(uuid.uuid4())
   run(['docker','run','--rm','--name',container,'--read-only','--cap-drop','ALL',
    '--security-opt','no-new-privileges','--pids-limit','64','--memory','768m','--cpus','1',
    '--tmpfs','/tmp:rw,noexec,nosuid,size=256m,mode=1777',
    '--mount','type=bind,src='+str(root/'requirements.versions.txt')+',dst=/requirements.txt,readonly',
    '--mount','type=bind,src='+str(root/'wheels')+',dst=/wheels',
    payload['base'],'python','-m','pip','--isolated','--disable-pip-version-check','download',
    '--index-url','https://pypi.org/simple','--only-binary=:all:','--no-deps',
    '--no-cache-dir','--dest','/wheels','-r','/requirements.txt'],timeout=600)
   container=None
  phase='wheel-verification'
  wheels={};resolved={}
  for path in sorted((root/'wheels').iterdir()):
   info=path.lstat()
   assert stat.S_ISREG(info.st_mode) and info.st_uid==0 and info.st_nlink==1 and path.suffix=='.whl' and info.st_size<300000000
   with zipfile.ZipFile(path) as archive:
    # pip/setuptools can vendor other distributions' metadata. Select only
    # the wheel's own top-level distribution, never a nested vendor record.
    names=[name for name in archive.namelist()
           if len(Path(name).parts)==2 and name.endswith('.dist-info/METADATA')]
    assert len(names)==1 and archive.getinfo(names[0]).file_size<3000000
    from email.parser import BytesParser
    metadata=BytesParser().parsebytes(archive.read(names[0]))
    name=re.sub(r'[-_.]+','-',metadata['Name']).lower();version=metadata['Version']
   assert name in packages and packages[name]==version and name not in resolved
   digest=hashlib.sha256(path.read_bytes()).hexdigest()
   wheels[path.name]={'name':name,'version':version,'sha256':digest,'bytes':info.st_size}
   resolved[name]=digest;path.chmod(0o400)
  assert set(resolved)==set(packages)
  locked=''.join(name+'=='+packages[name]+' --hash=sha256:'+resolved[name]+'\n' for name in sorted(packages))
  public_file(root/'requirements.lock',locked.encode())
  public_file(root/'wheels.json',json.dumps(wheels,sort_keys=True,separators=(',',':')).encode())
  report.update(applied=True,wheel_count=len(wheels),binary_only=True,
    public_wheels=wheels,hash_locked_requirements=True,root=str(root))
 report['live_container_ids_unchanged']=before==sorted(run(['docker','ps','--no-trunc','--format','{{.ID}}']).decode().splitlines())
 assert report['live_container_ids_unchanged']
except BlockingIOError:
 report={'status':'deferred_backup_busy','applied':False,'phase':phase}
except Exception:
 report={'ok':False,'reason':'dependency_preparation_requires_review','phase':phase,'raw_output_withheld':True}
finally:
 if container:
  # Exact randomly named download fixture, with no private/live mounts.
  subprocess.run(['docker','rm','-f',container],capture_output=True,timeout=30)
 report.update(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),secret_values_exported=False)
print(json.dumps(report,sort_keys=True))
sys.exit(0 if report.get('preflight_passed') and report.get('live_container_ids_unchanged') else 1)
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true');args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    observation = json.loads((root/INPUTS).read_text())['inputs'][0]
    assert observation['container'] == 'dev-s3-open-csv-worker-1' and observation['python_version'] == '3.11.15'
    payload = {'apply': args.apply, 'base': BASE, 'base_id': BASE_ID, 'packages': observation['packages']}
    p = subprocess.run(['ssh', '-F', '/Users/isabininhotmail.com/.ssh/amberping_vps_recovery.config',
                        '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=12', '-o', 'ServerAliveInterval=15',
                        'myvps-recovery', 'sudo -n python3 -I -B -c ' + shlex.quote(REMOTE)],
                       input=json.dumps(payload), text=True, capture_output=True, timeout=720)
    try:
        report = json.loads(p.stdout)
    except ValueError:
        report = {'ok': False, 'reason': 'dependency_preparation_output_withheld'}
    target = root/'docs/3-runtime-testing-and-operations/server-secret-store/evidence'
    target /= 'dependency-inputs-' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.json'
    with target.open('x') as stream:
        stream.write(json.dumps(report, sort_keys=True, indent=2) + '\n')
    # Wheel hashes are public artifact identities; details stay in the receipt.
    print(json.dumps({key: value for key, value in report.items() if key != 'public_wheels'}, sort_keys=True))
    print('Public dependency receipt: ' + str(target))
    return p.returncode


if __name__ == '__main__':
    raise SystemExit(main())
