#!/usr/bin/env python3
"""Build, but never deploy, a pinned CSV/NDJSON parser candidate from reviewed public inputs.

The source allowlist is independent of Docker's context filtering. Only tracked,
unchanged runtime files enter; never a repository tarball, old app image, host
configuration or secret mount. Known live ENV values are checked in VPS memory
before source is written into a build context. No service is stopped or replaced.
"""
import argparse
import base64
import datetime as dt
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shlex
import subprocess

BASE = 'python@sha256:70e8e937c1da72df1688e883a2108b0feff03381187fa78eca6d9b10df411320'
BASE_ID = 'sha256:a2a75bf6184fd4e4f996711e12df827be50bdb51b1c99a68027760d5a7629360'
DEPENDENCIES = '99f5bafab669cd6288d7ed7f15cbcd6b575a7ea930e38115f0706d513ebf8a21'
RUNTIME = ('app/__init__.py','app/config.py','app/csv_processor.py','app/db.py','app/main.py','app/ndjson_processor.py','app/retention_worker.py','app/s3_client.py','app/sss_reader.py')
WORKER_ENTRYPOINTS = []


def allowed_source(name):
    return name in ('app/__init__.py','app/config.py','app/csv_processor.py','app/db.py','app/main.py','app/ndjson_processor.py','app/retention_worker.py','app/s3_client.py','app/sss_reader.py')


DOCKERFILE = f'''FROM {BASE}
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 SSS_ENFORCED=1 UVICORN_DATE_HEADER=false
COPY requirements.lock /build/requirements.lock
COPY wheels/ /build/wheels/
RUN python -m pip --isolated --disable-pip-version-check install --no-index --no-cache-dir --no-deps --require-hashes --find-links=/build/wheels -r /build/requirements.lock && python -m pip check
COPY source/ /app/
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8080"]
'''

REMOTE = r'''
import base64,datetime,fcntl,hashlib,json,os,re,shutil,stat,subprocess,sys,uuid
from pathlib import Path,PurePosixPath
payload=json.load(sys.stdin)
assert set(payload)=={'apply','source_commit','files','dockerfile','base','base_id','dependencies'}
assert type(payload['apply']) is bool and re.fullmatch('[0-9a-f]{40}',payload['source_commit'])
assert payload['base']=='python@sha256:70e8e937c1da72df1688e883a2108b0feff03381187fa78eca6d9b10df411320'
assert payload['base_id']=='sha256:a2a75bf6184fd4e4f996711e12df827be50bdb51b1c99a68027760d5a7629360'
assert payload['dependencies']=='99f5bafab669cd6288d7ed7f15cbcd6b575a7ea930e38115f0706d513ebf8a21'
def command(args,timeout=60):return subprocess.run(args,capture_output=True,timeout=timeout)
def run(args,timeout=60):
 p=command(args,timeout)
 if p.returncode:raise ValueError('candidate_command_failed')
 return p.stdout
def safe_file(path):
 info=path.lstat()
 assert stat.S_ISREG(info.st_mode) and info.st_uid==0 and info.st_nlink==1 and stat.S_IMODE(info.st_mode)==0o400
 return path.read_bytes()
def put(path,data):
 path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
 if path.exists() or path.is_symlink():assert safe_file(path)==data;return
 fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o400)
 with os.fdopen(fd,'wb') as stream:stream.write(data);stream.flush();os.fsync(stream.fileno())
def safe_root(path):
 if not path.exists():path.mkdir(mode=0o700)
 for part in (path,*path.parents):
  info=part.lstat();assert stat.S_ISDIR(info.st_mode) and info.st_uid==0 and not stat.S_IMODE(info.st_mode)&0o022
def live_boundary():
 rows=sorted(run(['docker','ps','--no-trunc','--format','{{.ID}}']).decode().splitlines())
 assert len(rows)==24
 policy=Path('/etc/amberping/dev/config.d/paused-consumers-v1.json')
 for parent in policy.parents:
  info=parent.lstat();assert stat.S_ISDIR(info.st_mode) and info.st_uid==0 and not stat.S_IMODE(info.st_mode)&0o022
 info=policy.lstat();assert stat.S_ISREG(info.st_mode) and info.st_uid==0 and info.st_nlink==1 and stat.S_IMODE(info.st_mode)==0o600
 identity='94f9bdc18b5ad1a90fc3f4fcb8892cbe446b6f493a43046d07fba5abfd2a2e23'
 image='sha256:12be06c76ca8880127429b10eb5c479de368060b94856e5e32376624bf5a66de'
 assert json.loads(policy.read_bytes())=={'schema_version':1,'environment':'dev','paused':{'dev-etl-1':{
  'container_id':identity,'image_id':image,'reason_code':'owner_suspended_unused_mqtt','restart_policy':'no',
  'resume_requires_explicit_owner_request':True}}}
 paused=json.loads(run(['docker','inspect',identity]))[0]
 assert paused['Image']==image and paused['Name']=='/dev-etl-1' and paused['State']['Status']=='exited'
 assert all(paused['State'][key] is False for key in ('Running','Restarting','Paused'))
 assert paused['HostConfig']['RestartPolicy']['Name']=='no' and identity not in rows
 return rows
report={};phase='input-validation';fixture=None
try:
 os.umask(0o077);assert os.geteuid()==0
 records=json.loads(run(['docker','image','inspect',payload['base']]))
 assert records[0]['Id']==payload['base_id']
 dependency=Path('/opt/amberping/dev/build-inputs')/payload['dependencies'];safe_root(dependency)
 wheels=json.loads(safe_file(dependency/'wheels.json'));assert len(wheels)==34
 requirements=safe_file(dependency/'requirements.lock')
 assert len(payload['files'])<600 and sum(len(x) for x in payload['files'].values())<30000000
 sources={}
 for name,encoded in payload['files'].items():
  path=PurePosixPath(name)
  assert not path.is_absolute() and str(path)==name and '..' not in path.parts
  assert name in ('app/__init__.py','app/config.py','app/csv_processor.py','app/db.py','app/main.py','app/ndjson_processor.py','app/retention_worker.py','app/s3_client.py','app/sss_reader.py')
  assert all(not x.startswith('.') and x not in ('private','secrets','secrets.d','credentials','__pycache__') for x in path.parts)
  data=base64.b64decode(encoded,validate=True);assert len(data)<4000000
  if name.endswith('.py'):compile(data,name,'exec')
  assert not any(x in data for x in (b'-----BEGIN OPENSSH PRIVATE KEY-----',b'-----BEGIN PRIVATE KEY-----',b'AGE-SECRET-KEY-1'))
  sources[name]=data
 assert set(sources)==set(('app/__init__.py','app/config.py','app/csv_processor.py','app/db.py','app/main.py','app/ndjson_processor.py','app/retention_worker.py','app/s3_client.py','app/sss_reader.py'))
 phase='known-runtime-material-scan'
 # No live material is exported or passed into the build daemon/context.
 # This is bounded known-ENV matching, not a claim of a universal secret scan.
 live=json.loads(run(['docker','inspect','dev-fastapi-1','dev-analytics-scheduler-worker-1',
  'dev-subscription-reminder-worker-1','dev-transactional-email-worker-1','ingestion-worker','s3-service-api','dev-s3-open-csv-worker-1']))
 seeds=[]
 def structured(value):
  if isinstance(value,dict):
   for key,item in value.items():
    if isinstance(item,str) and key in ('password','secret','token','signing','proof','request','rate','envelope','access_key','secret_key'):
     if len(item)>=12:seeds.append(('structured-'+key,item.encode()))
    elif isinstance(item,(dict,list)):structured(item)
  elif isinstance(value,list):
   for item in value:structured(item)
 for row in live:
  for entry in row['Config']['Env']:
   key,separator,value=entry.partition('=')
   if re.search('PASSWORD|SECRET|KEYRING|TOKEN|CREDENTIAL|DSN',key) and key not in ('TURNSTILE_SECRET_KEY',):
    if len(value)>=12:seeds.append((key,value.encode()))
    if value.startswith('{'):
     try:structured(json.loads(value))
     except ValueError:pass
    if '://' in value:
     from urllib.parse import urlsplit,unquote
     try:
      password=urlsplit(value).password
      if password and len(password)>=8:seeds.append((key+'-password',unquote(password).encode()))
     except ValueError:pass
 phase='known-runtime-material-matching'
 matches=[{'source':name,'environment_name':key} for key,secret in seeds for name,data in sources.items() if secret in data]
 assert not matches
 del seeds,live
 phase='public-manifest'
 manifest={'schema_version':1,'source_commit':payload['source_commit'],'base':payload['base'],
  'dependencies':payload['dependencies'],'dockerfile_sha256':hashlib.sha256(payload['dockerfile'].encode()).hexdigest(),
  'files':{name:hashlib.sha256(data).hexdigest() for name,data in sources.items()}}
 encoded=json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()
 artifact=hashlib.sha256(encoded).hexdigest();tag='amberping-dev-csv:sss-'+artifact[:24]
 phase='live-boundary'
 before=live_boundary()
 report={'schema_version':1,'source_commit':payload['source_commit'],'artifact_id':artifact,
  'source_files':len(sources),'base':payload['base'],'dependency_artifact':payload['dependencies'],
  'preflight_passed':True,'applied':False,'live_consumers_changed':False,
  'known_env_material_matches':0,'secret_values_exported':False,
  'limitations':['Known ENV scan does not cover every native file or unknown provider credential.',
                'This image is not live-adopted or full-feature/startup/recovery-qualified.']}
 if payload['apply']:
  phase='backup-exclusion'
  lock=os.open('/srv/amberping/backups/.global-execution.lock',os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC)
  info=os.fstat(lock);assert stat.S_ISREG(info.st_mode) and info.st_uid==0 and info.st_nlink==1 and stat.S_IMODE(info.st_mode)==0o600
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  phase='public-context'
  parent=Path('/opt/amberping/dev/build-candidates');safe_root(parent)
  root=parent/artifact;safe_root(root);safe_root(root/'wheels');safe_root(root/'source')
  put(root/'manifest.json',encoded);put(root/'Dockerfile',payload['dockerfile'].encode())
  put(root/'requirements.lock',requirements)
  for filename,record in wheels.items():
   assert PurePosixPath(filename).name==filename and filename.endswith('.whl')
   data=safe_file(dependency/'wheels'/filename)
   assert hashlib.sha256(data).hexdigest()==record['sha256'] and len(data)==record['bytes']
   put(root/'wheels'/filename,data)
  for name,data in sources.items():put(root/'source'/name,data)
  # Context contains only these generated allowlisted inputs. Do not rely on
  # .dockerignore for a prepacked tar transport.
  expected={'manifest.json','Dockerfile','requirements.lock'}
  expected.update('wheels/'+name for name in wheels)
  expected.update('source/'+name for name in sources)
  observed=set()
  for path in root.rglob('*'):
   info=path.lstat()
   if stat.S_ISDIR(info.st_mode):
    assert info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o700
   else:
    assert stat.S_ISREG(info.st_mode) and info.st_uid==0 and info.st_nlink==1 and stat.S_IMODE(info.st_mode)==0o400
    observed.add(str(path.relative_to(root)))
  assert observed==expected
  phase='offline-build'
  image=run(['docker','build','--network=none','--pull=false','--memory=1g',
    '--cpu-period=100000','--cpu-quota=100000','-q','-t',tag,str(root)],timeout=600).decode().strip()
  assert re.fullmatch('sha256:[0-9a-f]{64}',image)
  row=json.loads(run(['docker','image','inspect',image]))[0]
  assert row['Id']==image and 'SSS_ENFORCED=1' in row['Config']['Env']
  assert not row['Config'].get('Volumes') and row['Config']['WorkingDir']=='/app'
  assert not any(re.search('PASSWORD|SECRET|KEYRING|TOKEN|CREDENTIAL|DSN',x.split('=',1)[0]) for x in row['Config']['Env'])
  phase='installed-dependencies'
  run(['docker','run','--rm','--network','none','--read-only','--cap-drop','ALL',
   '--security-opt','no-new-privileges','--memory','512m','--pids-limit','64',image,'python','-m','pip','check'])
  report.update(applied=True,image=image,tag=tag,build_network='none',
   dependency_check_passed=True,default_sss_enforced=True,public_source_manifest=str(root/'manifest.json'))
 report['live_container_ids_unchanged']=before==live_boundary()
 assert report['live_container_ids_unchanged']
except BlockingIOError:
 report={'status':'deferred_backup_busy','applied':False,'phase':phase}
except Exception:
 report={'ok':False,'phase':phase,'reason':'candidate_build_requires_review','raw_output_withheld':True,
         'matching_source_names':locals().get('matches',[])}
report['observed_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
print(json.dumps(report,sort_keys=True))
sys.exit(0 if report.get('preflight_passed') and report.get('live_container_ids_unchanged') else 1)
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true');args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    tracked = subprocess.check_output(['git', 'ls-files', '-z', '--', *RUNTIME, *WORKER_ENTRYPOINTS], cwd=root).decode().split('\0')
    files = {}
    for name in filter(None, tracked):
        if not allowed_source(name):
            raise SystemExit('candidate_source_path_requires_review')
        path = root/name
        if path.is_symlink() or not path.is_file():
            raise SystemExit('candidate_source_type_requires_review')
        data = path.read_bytes()
        if data != subprocess.check_output(['git', 'show', 'HEAD:' + name], cwd=root):
            raise SystemExit('candidate_source_uncommitted_changes_rejected')
        deployed_name = 'qualification/entry.py' if name == 'deploy/b1-5/entry.py' else name
        files[deployed_name] = base64.b64encode(data).decode()
    payload = {'apply': args.apply, 'source_commit': commit, 'files': files,
               'dockerfile': DOCKERFILE, 'base': BASE, 'base_id': BASE_ID, 'dependencies': DEPENDENCIES}
    p = subprocess.run(['ssh', '-F', '/Users/isabininhotmail.com/.ssh/amberping_vps_recovery.config',
        '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=12', '-o', 'ServerAliveInterval=15', 'myvps-recovery',
        'sudo -n python3 -I -B -c ' + shlex.quote(REMOTE)], input=json.dumps(payload),
        text=True, capture_output=True, timeout=720)
    try:report = json.loads(p.stdout)
    except ValueError:report = {'ok': False, 'reason': 'candidate_build_output_withheld'}
    target = root/'docs/3-runtime-testing-and-operations/server-secret-store/evidence'/('csv-candidate-' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.json')
    with target.open('x') as stream:stream.write(json.dumps(report, sort_keys=True, indent=2) + '\n')
    print(json.dumps(report, sort_keys=True));print('Metadata-only receipt: ' + str(target))
    return p.returncode


if __name__ == '__main__':
    raise SystemExit(main())

