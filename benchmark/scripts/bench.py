#!/usr/bin/env python3
"""Isolated NearHub benchmark orchestration. Never connects to development resources."""
import argparse
import collections
import contextlib
import csv
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
import zipfile

from stats import read_jtl, window, reconcile

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / 'benchmark'
RUNTIME = BASE / '.runtime'
PROJECT = 'nearhub-benchmark'
DBNAME = 'nearhub_benchmark'
GROUP = 'order-consumer-group'
TOPIC = 'order-seckill-topic'


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def save(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str) + '\n')
    os.replace(tmp, path)


def read(path):
    return json.loads(Path(path).read_text())


def command(args, timeout=30, check=True, **kwargs):
    p = subprocess.run([str(a) for a in args], cwd=ROOT, text=True, capture_output=True,
                       timeout=timeout, **kwargs)
    if check and p.returncode:
        # Do not echo env/config or SQL containing credentials/tokens.
        raise RuntimeError(f'{args[0]} failed ({p.returncode}): {p.stderr[-1500:]}')
    return p


def config():
    result = {}
    for line in (BASE / '.env').read_text().splitlines():
        if line.strip() and not line.lstrip().startswith('#'):
            k, v = line.split('=', 1)
            result[k.strip()] = v.strip()
    return result


def project_name():
    name = config().get('BENCH_PROJECT', PROJECT)
    require(re.fullmatch(r'nearhub-benchmark(?:-[a-z0-9-]+)?', name), 'Benchmark project prefix required')
    return name


def compose(*args, **kw):
    # A developer's exported .env must not override this namespace's ports/passwords.
    env = os.environ.copy()
    env.update(config())
    kw.setdefault('env', env)
    return command(['docker', 'compose', '--project-name', project_name(), '--env-file', BASE / '.env',
                    '-f', BASE / 'compose.yaml', *args], **kw)


def container_ids():
    return compose('ps', '-a', '-q').stdout.split()


def containers():
    ids = container_ids()
    return json.loads(command(['docker', 'inspect', *ids]).stdout) if ids else []


def owned_containers():
    items = containers()
    require(items, 'Isolated Compose environment is not running; env.sh up first')
    for c in items:
        require(c['Config']['Labels'].get('com.docker.compose.project') == project_name(), 'Foreign container')
    return items


def db():
    import pymysql
    c = config()
    return pymysql.connect(host='127.0.0.1', port=int(c['MYSQL_PORT']), user='benchmark',
                           password=c['MYSQL_PASSWORD'], database=DBNAME, charset='utf8mb4',
                           autocommit=True, connect_timeout=5, read_timeout=30, write_timeout=30,
                           cursorclass=pymysql.cursors.DictCursor)


def redis_client():
    import redis
    return redis.Redis(host='127.0.0.1', port=int(config()['REDIS_PORT']), db=0,
                       decode_responses=True, socket_timeout=10, socket_connect_timeout=5)


def query(sql, args=()):
    with db() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def identity():
    return read(RUNTIME / 'environment.json')['id']


def guard():
    items = owned_containers()
    c = config()
    for service, port in [('mysql', c['MYSQL_PORT']), ('redis', c['REDIS_PORT']), ('namesrv', c['NAMESRV_PORT'])]:
        match = [x for x in items if x['Config']['Labels']['com.docker.compose.service'] == service]
        require(len(match) == 1 and match[0]['State']['Running'], service + ' is not running')
        bindings = match[0]['HostConfig']['PortBindings']
        require(any(b['HostIp'] == '127.0.0.1' and b['HostPort'] == port for v in bindings.values() for b in v), 'Port ownership mismatch')
    row = query('SELECT environment_id FROM benchmark_environment WHERE id=1')
    require(row and row[0]['environment_id'] == identity(), 'MySQL environment marker mismatch')
    require(redis_client().get('benchmark:environment') == identity(), 'Redis environment marker mismatch')


def init():
    RUNTIME.mkdir(parents=True, exist_ok=True)
    if not (BASE / '.env').exists():
        text = (BASE / 'config/benchmark.env.example').read_text().replace('replace_with_random_password', secrets.token_hex(24)).replace('replace_with_another_random_password', secrets.token_hex(24))
        (BASE / '.env').write_text(text)
        os.chmod(BASE / '.env', 0o600)
    if not (RUNTIME / 'environment.json').exists():
        save(RUNTIME / 'environment.json', dict(id=str(uuid.uuid4()), created_at=time.time()))
    print('Local configuration initialized; secrets are ignored by Git.')


def mqadmin(*args):
    # mqadmin is diagnostic only. Limit its heap, not the application's or Broker's.
    try:
        p = compose('exec', '-T', '-e', 'JAVA_OPT_EXT=-Xms32m -Xmx128m -Xmn32m', 'broker',
                    'sh', 'mqadmin', *args, timeout=35, check=False)
    except (OSError, subprocess.SubprocessError) as e:
        return dict(ok=False, output='', error=str(e))
    return dict(ok=p.returncode == 0 and 'Exception' not in p.stdout + p.stderr,
                output=p.stdout, error=p.stderr)


def up():
    init()
    p = compose('up', '-d', '--wait', '--wait-timeout', '240', timeout=270)
    print(p.stdout[-1500:])
    # Only this project's fresh, dedicated MySQL schema is initialized by Compose.
    query('CREATE TABLE IF NOT EXISTS benchmark_environment (id INT PRIMARY KEY, environment_id VARCHAR(36) NOT NULL)')
    query('INSERT IGNORE INTO benchmark_environment VALUES (1,%s)', (identity(),))
    redis_client().set('benchmark:environment', identity(), nx=True)
    guard()
    query('''CREATE TABLE IF NOT EXISTS benchmark_runs (
        seq INT AUTO_INCREMENT PRIMARY KEY, run_id VARCHAR(80) UNIQUE NOT NULL,
        voucher_id BIGINT UNSIGNED UNIQUE NOT NULL, user_count INT NOT NULL, stock INT NOT NULL,
        state VARCHAR(24) NOT NULL DEFAULT 'PREPARING')''')
    metadata = read(RUNTIME/'environment.json')
    if not query('SELECT seq FROM benchmark_runs LIMIT 1'):
        query('ALTER TABLE benchmark_runs AUTO_INCREMENT = '+str(int(metadata.get('user_sequence_start', 1))))
        query('ALTER TABLE tb_voucher AUTO_INCREMENT = '+str(int(metadata.get('voucher_id_start', 10))))
    for topic in [TOPIC, 'canal-binlog-topic']:
        r = mqadmin('updateTopic', '-n', 'namesrv:9876', '-b', '127.0.0.1:30911', '-t', topic, '-r', '4', '-w', '4')
        require(r['ok'], 'Topic creation failed: ' + r['error'][-1000:])
    print('Isolated MySQL / Redis / RocketMQ ready.')


def app_home():
    custom = config().get('APP_JAVA_HOME')
    choices = [Path(custom)] if custom else [ROOT / '.local/jdk8/Contents/Home', ROOT / '.local/jdk8']
    for home in choices:
        if (home / 'bin/java').exists():
            require('"1.8.' in command([home / 'bin/java', '-version']).stderr, 'Application build requires JDK 8 (existing repository requirement)')
            return home
    raise RuntimeError('Set APP_JAVA_HOME in benchmark/.env to a JDK 8 home')


def source_hash():
    h = hashlib.sha256()
    for p in sorted([ROOT / 'pom.xml', * (ROOT / 'src/main').rglob('*')]):
        if p.is_file():
            h.update(str(p.relative_to(ROOT)).encode()); h.update(p.read_bytes())
    return h.hexdigest()


def jvm_snapshot(meta):
    home = Path(meta['java_home'])
    source = BASE / 'tools/JvmSnapshot.java'
    classes = RUNTIME / 'monitor-classes'
    classes.mkdir(exist_ok=True)
    compiled = classes / 'JvmSnapshot.class'
    if not compiled.exists() or compiled.stat().st_mtime < source.stat().st_mtime:
        command([home/'bin/javac', '-cp', home/'lib/tools.jar', '-d', classes, source])
    p = command([home/'bin/java', '-cp', str(classes)+os.pathsep+str(home/'lib/tools.jar'),
                 'JvmSnapshot', str(meta['pid'])], timeout=15)
    result = json.loads(p.stdout)
    result['sampled_at'] = time.time()
    return result


def app_process():
    path = RUNTIME / 'app.json'
    if not path.exists():
        return None
    meta = read(path)
    p = command(['ps', '-p', str(meta['pid']), '-o', 'command='], check=False)
    return meta if p.returncode == 0 and str(RUNTIME / 'app.jar') in p.stdout else None


def build_overlay(home):
    build = RUNTIME/'create-only-overlay'
    lib = build/'lib'; classes = build/'classes'
    lib.mkdir(parents=True, exist_ok=True); classes.mkdir(exist_ok=True)
    with zipfile.ZipFile(RUNTIME/'app.jar') as archive:
        for name in archive.namelist():
            if name.startswith('BOOT-INF/lib/') and name.endswith('.jar'):
                (lib/Path(name).name).write_bytes(archive.read(name))
    sources = sorted((BASE/'overlay').rglob('*.java'))
    cp = str(ROOT/'target/classes')+os.pathsep+str(lib/'*')
    command([home/'bin/javac', '-cp', cp, '-d', classes, *sources], timeout=60)
    test = command([home/'bin/java','-cp',str(classes)+os.pathsep+cp,
                    'nearhub.benchmark.CreateOnlyBenchmarkTest'], timeout=30)
    (build/'test.log').write_text(test.stdout+test.stderr)
    jar = build/'create-only.jar'
    command([home/'bin/jar','cf',jar,'-C',classes,'.'])
    return jar


def start():
    guard()
    require(not app_process(), 'Benchmark app is already running')
    c = config()
    with socket.socket() as sock:
        require(sock.connect_ex(('127.0.0.1', int(c['SERVER_PORT']))) != 0, 'Application port is already occupied')
    home = app_home()
    env = os.environ.copy()
    # Do not inherit development datasource or Spring command-line overrides.
    for k in list(env):
        if k.startswith(('SPRING_', 'MYSQL_', 'REDIS_', 'ROCKETMQ_', 'SERVER_', 'SECKILL_', 'ORDER_')) or k in ('JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS'):
            env.pop(k)
    env.update(JAVA_HOME=str(home), PATH=str(home / 'bin') + os.pathsep + env['PATH'])
    with (RUNTIME / 'build.log').open('w') as log:
        p = subprocess.run(['mvn', '-B', '-DskipTests', 'package'], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    require(p.returncode == 0, 'Build failed; see benchmark/.runtime/build.log')
    shutil.copy2(ROOT / 'target/hmdp-pro-0.0.1-SNAPSHOT.jar', RUNTIME / 'app.jar')
    env.update(MYSQL_HOST='127.0.0.1', MYSQL_PORT=c['MYSQL_PORT'], MYSQL_DATABASE=DBNAME,
               MYSQL_USER='benchmark', MYSQL_PASSWORD=c['MYSQL_PASSWORD'], REDIS_HOST='127.0.0.1',
               REDIS_PORT=c['REDIS_PORT'], REDIS_PASSWORD='', SERVER_PORT=c['SERVER_PORT'],
               ROCKETMQ_NAME_SERVER='127.0.0.1:' + c['NAMESRV_PORT'])
    args = [str(home / 'bin/java'), *shlex.split(c.get('APP_JAVA_OPTS', '')), '-jar', str(RUNTIME / 'app.jar')]
    create_only = c.get('BENCH_CREATE_ONLY','false').lower() == 'true'
    overlay_sha = None
    if create_only:
        overlay = build_overlay(home)
        overlay_sha = hashlib.sha256(overlay.read_bytes()).hexdigest()
        env['NAMESRV_PORT_FOR_BENCHMARK'] = c['NAMESRV_PORT']
        args = [str(home/'bin/java'), *shlex.split(c.get('APP_JAVA_OPTS','')),
                '-Dnearhub.benchmark.createOnly=true', '-Dloader.path='+str(overlay),
                '-Dloader.main=nearhub.benchmark.CreateOnlyBenchmark', '-cp', str(RUNTIME/'app.jar'),
                'org.springframework.boot.loader.PropertiesLauncher']
    with (RUNTIME / 'app.log').open('a') as log:
        p = subprocess.Popen(args, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    save(RUNTIME / 'app.json', dict(pid=p.pid, environment_id=identity(), started_at=time.time(),
                                  source_hash=source_hash(), jar_sha256=hashlib.sha256((RUNTIME / 'app.jar').read_bytes()).hexdigest(), java_home=str(home), java_options=c.get('APP_JAVA_OPTS', ''),
                                  create_only=create_only, overlay_sha256=overlay_sha, compose_project=project_name()))
    for _ in range(120):
        require(p.poll() is None, 'Application exited; see benchmark/.runtime/app.log')
        try:
            code, _ = http('/user/me')
            if code == 401:
                print(f'Benchmark Spring Boot running on 127.0.0.1:{c["SERVER_PORT"]}, PID {p.pid}')
                return
        except OSError:
            pass
        time.sleep(1)
    raise RuntimeError('Application readiness timeout; log retained')


def fresh(create_only=False):
    require(not app_process(), 'Stop the old application before creating a fresh environment')
    old = owned_containers()
    require(not any(c['State']['Running'] for c in old), 'Stop old containers first; old volumes must stay sealed')
    archive = RUNTIME/'environments'/identity()
    archive.mkdir(parents=True, exist_ok=True)
    for source in [BASE/'.env', RUNTIME/'environment.json', RUNTIME/'app.json']:
        if source.exists(): shutil.copy2(source, archive/source.name)
    save(archive/'volumes.json', dict(project=project_name(), volumes=sorted({m['Name'] for c in old for m in c['Mounts'] if m['Type']=='volume'})))
    manifests = [read(p) for p in (BASE/'data').glob('*/manifest.json')]
    sequence = max([int((m['user_last']-7000000000000)//1000000)+1 for m in manifests]+[1])
    voucher = max([m['voucherId']+1 for m in manifests]+[10])
    c=config(); c['BENCH_PROJECT']='nearhub-benchmark-'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d%H%M%S')
    c['BENCH_CREATE_ONLY']='true' if create_only else 'false'
    (BASE/'.env').write_text(''.join(k+'='+v+'\n' for k,v in c.items()))
    save(RUNTIME/'environment.json',dict(id=str(uuid.uuid4()),created_at=time.time(),project=c['BENCH_PROJECT'],
                                       user_sequence_start=sequence,voucher_id_start=voucher))
    print('Fresh namespace configured: '+c['BENCH_PROJECT']+'; old volumes preserved in '+str(archive))


def stop_app():
    meta = app_process()
    if meta:
        os.kill(meta['pid'], signal.SIGTERM)
        for _ in range(40):
            if not app_process():
                return
            time.sleep(.5)
        raise RuntimeError('Application did not stop; resources retained')


def http(path, token=None, method='GET'):
    headers = {'authorization': token} if token else {}
    req = urllib.request.Request(f'http://127.0.0.1:{config()["SERVER_PORT"]}' + path, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=12) as r:
            return r.status, json.loads(r.read() or b'{}')
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            body = json.loads(raw or b'{}')
        except ValueError:
            body = {}
        return e.code, body


def environment(out):
    def probe(args):
        try:
            p = command(args, check=False, timeout=15)
            return dict(ok=p.returncode == 0, output=(p.stdout + p.stderr).strip())
        except Exception as e:
            return dict(ok=False, error=str(e))
    result = dict(recorded_at=dt.datetime.now(dt.timezone.utc).isoformat(), architecture=platform.machine(),
                  host=probe(['sysctl', '-n', 'machdep.cpu.brand_string', 'hw.ncpu', 'hw.memsize']) if sys.platform == 'darwin' else probe(['lscpu']),
                  host_memory_linux=probe(['cat', '/proc/meminfo']) if sys.platform != 'darwin' else None,
                  docker=probe(['docker', 'info', '--format', '{{json .NCPU}} {{json .MemTotal}} {{json .ServerVersion}}']),
                  git=probe(['git', 'rev-parse', 'HEAD']), dirty=probe(['git', 'status', '--short']),
                  source_hash=source_hash(), java=probe(['java', '-version']),
                  emulated_rocketmq=platform.machine() in ('arm64', 'aarch64'))
    try:
        result['containers'] = [dict(name=c['Name'], image=c['Config']['Image'], image_id=c['Image'], platform=c.get('Platform'),
                                    cpu_quota=c['HostConfig']['CpuQuota'], nano_cpus=c['HostConfig']['NanoCpus'],
                                    memory=c['HostConfig']['Memory'], running=c['State']['Running'],
                                    service=c['Config']['Labels'].get('com.docker.compose.service')) for c in containers()]
        result['other_running_containers'] = probe(['docker', 'ps', '--format', '{{.Names}} {{.Image}}'])
    except Exception as e:
        result['container_error'] = str(e)
    meta = app_process()
    result['app'] = meta
    result['spring_boot_instances_managed'] = 1 if meta else 0
    result['broker_instances'] = sum(x['service'] == 'broker' and x['running'] for x in result.get('containers', []))
    try:
        result['mysql_version'] = query('SELECT VERSION() version')[0]['version']
        result['redis_version'] = redis_client().info('server')['redis_version']
    except Exception as e:
        result['middleware_version_error'] = str(e)
    if meta:
        home = Path(meta['java_home'])
        result['app_java'] = probe([home / 'bin/java', '-version'])
        result['jvm_flags'] = probe([home / 'bin/jcmd', str(meta['pid']), 'VM.flags'])
        result['jvm_heap'] = probe([home / 'bin/jcmd', str(meta['pid']), 'GC.heap_info'])
        try:
            result['jvm_platform'] = jvm_snapshot(meta)
        except Exception as e:
            result['jvm_platform_error'] = str(e)
    save(out, result)
    return result


def check():
    out = BASE / 'results/environment.json'
    environment(out)
    guard()
    require(app_process(), 'Start the isolated application first')
    require(http('/user/me')[0] == 401, 'Unauthenticated route must return 401')
    require(Path(jmeter()).is_file(), 'Run bootstrap.sh to install JMeter')
    print('Environment checks passed; ' + str(out))


def run_path(run_id):
    require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', run_id), 'Invalid run ID')
    return BASE / 'data' / run_id


def load_manifest(run_id):
    m = read(run_path(run_id) / 'manifest.json')
    require(m['environment_id'] == identity(), 'Run belongs to a different environment')
    return m


def prepare(run_id, users, stock):
    guard()
    require(0 < users <= 1000000 and users <= stock <= 2147483647, 'Require 0 < users <= 1,000,000 and stock >= users')
    folder = run_path(run_id)
    folder.mkdir(parents=True, exist_ok=True)
    owner_file = folder / 'environment.json'
    if (folder / 'manifest.json').exists():
        require(read(folder / 'manifest.json')['environment_id'] == identity(), 'Run ID belongs to a retired environment; choose a NEW run ID')
    if owner_file.exists():
        require(read(owner_file)['environment_id'] == identity(), 'Run directory belongs to another environment')
    else:
        require(not (folder / 'tokens.csv').exists() or (folder / 'manifest.json').exists(), 'Unowned CSV; choose a NEW run ID')
        save(owner_file, dict(environment_id=identity()))
    rows = query('SELECT * FROM benchmark_runs WHERE run_id=%s', (run_id,))
    if not rows:
        with db() as conn, conn.cursor() as cur:
            conn.begin()
            cur.execute('INSERT INTO tb_voucher(shop_id,title,pay_value,actual_value,type,status) VALUES (1,%s,1,100,1,1)', ('BENCH:' + run_id,))
            vid = cur.lastrowid
            cur.execute('INSERT INTO tb_seckill_voucher(voucher_id,stock,initial_stock,begin_time,end_time) VALUES (%s,%s,%s,NOW()-INTERVAL 1 MINUTE,NOW()+INTERVAL 2 HOUR)', (vid, stock, stock))
            cur.execute('INSERT INTO benchmark_runs(run_id,voucher_id,user_count,stock) VALUES(%s,%s,%s,%s)', (run_id, vid, users, stock))
            conn.commit()
        rows = query('SELECT * FROM benchmark_runs WHERE run_id=%s', (run_id,))
    row = rows[0]
    require(row['user_count'] == users and row['stock'] == stock, 'Existing run parameters differ')
    require(row['state'] in ('PREPARING', 'READY'), 'Run already used; prepare a NEW run ID')
    vid, seq = row['voucher_id'], row['seq']
    require(seq < 9999, 'Synthetic phone namespace exhausted')
    r = redis_client()
    require(not query('SELECT id FROM tb_voucher_order WHERE voucher_id=%s LIMIT 1', (vid,)), 'Voucher has orders')
    require(not r.exists(f'seckill:claim-owner:{vid}'), 'Voucher already has reservations')
    tokens = folder / 'tokens.csv'
    if not tokens.exists():
        tmp = tokens.with_suffix('.tmp')
        with tmp.open('w', newline='') as f:
            writer = csv.writer(f); writer.writerow(['token', 'userId'])
            for i in range(1, users+1):
                writer.writerow([uuid.uuid4().hex, 7000000000000 + seq * 1000000 + i])
        os.replace(tmp, tokens)
    identities = list(csv.DictReader(tokens.open()))
    require(len(identities) == users and len({x['token'] for x in identities}) == users, 'CSV integrity failure')
    require(all(int(x['userId']) == 7000000000000 + seq*1000000 + i+1 for i,x in enumerate(identities)), 'CSV identity allocation mismatch')
    # TTL is derived from the checked-out real login constant (minutes), not a guessed default.
    constants = (ROOT / 'src/main/java/com/hmdp/utils/RedisConstants.java').read_text()
    ttl = int(re.search(r'LOGIN_USER_TTL\s*=\s*(\d+)L', constants).group(1)) * 60
    for start_index in range(0, users, 1000):
        batch = identities[start_index:start_index+1000]
        with db() as conn, conn.cursor() as cur:
            # All VALUES slots must be placeholders for PyMySQL's multi-row fast path.
            cur.executemany('INSERT IGNORE INTO tb_user(id,phone,nick_name,icon) VALUES(%s,%s,%s,%s)',
                            [(int(x['userId']), '9' + str(seq*1000000+start_index+i+1).zfill(10), 'bench_' + str(seq), '') for i,x in enumerate(batch)])
            cur.execute('SELECT COUNT(*) n FROM tb_user WHERE id BETWEEN %s AND %s AND nick_name=%s', (int(batch[0]['userId']), int(batch[-1]['userId']), 'bench_' + str(seq)))
            require(cur.fetchone()['n'] == len(batch), 'Test identity collision')
        pipe = r.pipeline(transaction=False)
        for x in batch:
            pipe.hset('login:token:' + x['token'], mapping={'id': x['userId'], 'nickName': 'bench_' + str(seq), 'icon': ''})
            pipe.expire('login:token:' + x['token'], ttl)
        pipe.execute()
    r.set(f'seckill:stock:{vid}', stock, nx=True)
    require(r.get(f'seckill:stock:{vid}') == str(stock) and r.ttl(f'seckill:stock:{vid}') == -1, 'Never reset used Redis inventory')
    v = query('SELECT begin_time,end_time FROM tb_seckill_voucher WHERE voucher_id=%s', (vid,))[0]
    manifest = dict(run_id=run_id, environment_id=identity(), voucherId=vid, stock=stock, user_count=users,
                    user_first=int(identities[0]['userId']), user_last=int(identities[-1]['userId']),
                    token_sha256=hashlib.sha256(tokens.read_bytes()).hexdigest(), token_ttl_seconds=ttl,
                    prepared_at=time.time(), begin_time=str(v['begin_time']), end_time=str(v['end_time']))
    save(folder / 'manifest.json', manifest)
    query("UPDATE benchmark_runs SET state='READY' WHERE run_id=%s", (run_id,))
    print(f'Prepared {run_id}: voucherId={vid}, users={users}, stock={stock}; tokens kept private.')
    return manifest


def preflight(m, total_seconds):
    guard()
    meta = app_process()
    require(meta and meta['environment_id'] == identity(), 'Isolated app not running')
    require(meta['source_hash'] == source_hash(), 'Source changed since app build; restart before measuring')
    if config().get('BENCH_CREATE_ONLY','false').lower() == 'true':
        require(meta.get('create_only') and jvm_snapshot(meta).get('create_only'), 'CREATE-only interception is not active')
    row = query('SELECT * FROM benchmark_runs WHERE run_id=%s', (m['run_id'],))[0]
    require(row['state'] == 'READY', 'Tokens cannot be reused: prepare a new run ID')
    r = redis_client(); vid = m['voucherId']
    require(not query('SELECT id FROM tb_voucher_order WHERE voucher_id=%s LIMIT 1', (vid,)), 'Baseline orders must be zero')
    require(not r.exists(f'seckill:claim-owner:{vid}'), 'Baseline claims must be empty')
    require(r.get(f'seckill:stock:{vid}') == str(m['stock']) and r.ttl(f'seckill:stock:{vid}') == -1, 'Invalid Redis baseline')
    v = query('SELECT stock,initial_stock,begin_time<=NOW() AND end_time>NOW()+INTERVAL %s SECOND AS valid FROM tb_seckill_voucher WHERE voucher_id=%s', (total_seconds+180, vid))[0]
    require(v['stock'] == v['initial_stock'] == m['stock'] and v['valid'], 'Invalid voucher stock/time')
    tokens = run_path(m['run_id']) / 'tokens.csv'
    require(hashlib.sha256(tokens.read_bytes()).hexdigest() == m['token_sha256'], 'CSV was modified')
    identities = list(csv.DictReader(tokens.open()))
    for i in range(0, len(identities), 1000):
        batch = identities[i:i+1000]; pipe = r.pipeline(transaction=False)
        for x in batch:
            pipe.hget('login:token:' + x['token'], 'id'); pipe.ttl('login:token:' + x['token'])
        values = pipe.execute()
        require(all(values[j*2] == x['userId'] and values[j*2+1] > total_seconds + 180 for j,x in enumerate(batch)), 'Missing/expired/mismatched login token')
    for x in [identities[0], identities[len(identities)//2], identities[-1]]:
        code, body = http('/user/me', x['token'])
        require(code == 200 and str(body.get('data', {}).get('id')) == x['userId'], 'Real interceptor authentication failed')
    require(http('/user/me')[0] == 401, 'Authentication unexpectedly bypassed')
    # Every round uses a new voucher; delayed closure is outside the measurement scope.
    connections = mqadmin('consumerConnection', '-n', 'namesrv:9876', '-g', GROUP)
    clients = re.findall(r'^\S+\s+\S+\s+JAVA\s+V\S+', connections['output'], re.MULTILINE)
    require(connections['ok'] and len(clients) == 1, 'Require exactly one online order consumer instance')
    return identities


def mq_snapshot():
    progress = mqadmin('consumerProgress', '-n', 'namesrv:9876', '-g', GROUP)
    topics = mqadmin('topicList', '-n', 'namesrv:9876')
    snapshot = dict(recorded_at=time.time(), progress=progress, topics=topics)
    # Fail closed on CLI format changes, missing groups or inaccessible Broker.
    match = re.search(r'Diff\s+Total\s*:\s*(\d+)', progress['output'])
    snapshot['delivered_backlog'] = int(match.group(1)) if progress['ok'] and match else None
    tps = re.search(r'Consume\s+TPS\s*:\s*([\d.]+)', progress['output'])
    snapshot['consume_tps'] = float(tps.group(1)) if progress['ok'] and tps else None
    for kind, name in [('retry', '%RETRY%' + GROUP), ('dlq', '%DLQ%' + GROUP)]:
        if topics['ok'] and name not in topics['output'].split():
            snapshot[kind + '_messages'] = 0
        else:
            status = mqadmin('topicStatus', '-n', 'namesrv:9876', '-t', name)
            snapshot[kind] = status
            offsets = []
            for line in status['output'].splitlines():
                fields = line.split()
                if len(fields) >= 4 and fields[1].isdigit() and fields[2].isdigit() and fields[3].isdigit():
                    offsets.append(int(fields[3]) - int(fields[2]))
            # Retry history is deliberately conservative: any retained retry invalidates a clean run.
            snapshot[kind + '_messages'] = sum(offsets) if status['ok'] and offsets else None
    snapshot['broker'] = mqadmin('brokerStatus', '-n', 'namesrv:9876', '-b', '127.0.0.1:30911')
    # consumerProgress covers delivered CREATE and TIMEOUT together, not delayed/half messages.
    snapshot['scope'] = 'delivered group offsets; excludes half messages and not-yet-delivered scheduled TIMEOUT'
    snapshot['clean_delivered'] = all(snapshot.get(k) == 0 for k in ('delivered_backlog', 'retry_messages', 'dlq_messages'))
    return snapshot


def sample(m):
    vid = m['voucherId']; r = redis_client()
    row = query('SELECT COUNT(*) n, SUM(status=4) cancelled FROM tb_voucher_order WHERE voucher_id=%s', (vid,))[0]
    redis_info = r.info()
    mysql = query("SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running','Questions','Slow_queries','Innodb_row_lock_waits','Innodb_row_lock_time','Innodb_buffer_pool_reads','Innodb_buffer_pool_read_requests')")
    claims = r.hlen(f'seckill:claim-owner:{vid}')
    return dict(time=time.time(), persisted=row['n'], cancelled=int(row['cancelled'] or 0),
                claims=claims, unpersisted_claims_estimate=max(0, claims-row['n']), stock=r.get(f'seckill:stock:{vid}'),
                redis={k:redis_info.get(k) for k in ('used_memory','used_memory_peak','connected_clients','blocked_clients','instantaneous_ops_per_sec','total_commands_processed','evicted_keys','rejected_connections','used_cpu_sys','used_cpu_user')},
                mysql={x['Variable_name']:x['Value'] for x in mysql})


def monitor(m, folder, stop_event):
    interval = float(config().get('SAMPLE_SECONDS', 5))
    with (folder / 'metrics.jsonl').open('a') as f:
        while not stop_event.is_set():
            try:
                point = sample(m)
            except Exception as e:
                point = dict(time=time.time(), error=str(e))
            f.write(json.dumps(point) + '\n'); f.flush()
            stop_event.wait(interval)


def heavy_monitor(folder, stop_event):
    interval = float(config().get('MQ_SAMPLE_SECONDS', 30))
    while not stop_event.is_set():
        stamp = str(int(time.time()))
        try:
            save(folder / 'mq' / (stamp + '.json'), mq_snapshot())
            ids = container_ids()
            p = command(['docker', 'stats', '--no-stream', '--format', '{{json .}}', *ids], timeout=20, check=False)
            (folder / ('docker-' + stamp + '.jsonl')).write_text(p.stdout + p.stderr)
            meta = app_process()
            if meta:
                home = Path(meta['java_home'])
                try:
                    save(folder / ('jvm-platform-' + stamp + '.json'), jvm_snapshot(meta))
                except Exception as e:
                    save(folder / ('jvm-platform-' + stamp + '.json'), dict(error=str(e)))
                for name, args in [('jvm', [home / 'bin/jstat', '-gcutil', str(meta['pid'])]),
                                   ('process', ['ps', '-p', str(meta['pid']), '-o', 'pid,pcpu,rss,vsz'])]:
                    p = command(args, check=False, timeout=10)
                    (folder / (name + '-' + stamp + '.txt')).write_text(p.stdout + p.stderr)
                # jstat's perf-data discovery is unavailable on some macOS/JDK combinations.
                heap = command([home/'bin/jcmd', str(meta['pid']), 'GC.heap_info'], check=False, timeout=10)
                (folder / ('heap-' + stamp + '.txt')).write_text(heap.stdout + heap.stderr)
        except Exception as e:
            save(folder / ('monitor-error-' + stamp + '.json'), dict(error=str(e)))
        stop_event.wait(interval)


def jmeter():
    return str(RUNTIME / 'apache-jmeter-5.6.3/bin/jmeter')


def process_tree(root_pid):
    # JMeter's Unix launcher is a shell that starts Java as a child (it does not exec).
    p = command(['ps', '-axo', 'pid=,ppid=,pcpu=,rss=,vsz=,comm='], check=False)
    rows = []
    for line in p.stdout.splitlines():
        fields = line.split(None, 5)
        if len(fields) == 6:
            rows.append(dict(pid=int(fields[0]), ppid=int(fields[1]), cpu_percent=float(fields[2]),
                             rss_kib=int(fields[3]), vsz_kib=int(fields[4]), executable=fields[5]))
    selected = {root_pid}
    while True:
        expanded = selected | {r['pid'] for r in rows if r['ppid'] in selected}
        if expanded == selected:
            break
        selected = expanded
    return [r for r in rows if r['pid'] in selected]


def summarize(folder, m, params, rows):
    require(rows, 'JMeter produced no HTTP samples')
    start = min(int(x['timeStamp']) for x in rows)
    end = max(int(x['timeStamp']) + int(x['elapsed']) for x in rows) + 1
    # JMeter Thread Group starts before first sample. setup.groovy records its epoch in a sidecar.
    anchor_file = folder / 'window-start.txt'
    anchor = int(anchor_file.read_text().strip()) if anchor_file.exists() else start
    stable_start = anchor + params['rampup'] * 1000
    stable_end = stable_start + params['duration'] * 1000
    total = window(rows, start, end)
    steady = window(rows, stable_start, stable_end)
    truncated = len(rows) >= m['user_count'] and end < stable_end - 1000
    unique = len({x['userId'] for x in rows}) == len(rows)
    bins_file = folder / 'mysql-create-time.csv'
    db_bins = list(csv.DictReader(bins_file.open())) if bins_file.exists() else query('SELECT UNIX_TIMESTAMP(create_time) second, COUNT(*) persisted FROM tb_voucher_order WHERE voucher_id=%s GROUP BY create_time ORDER BY create_time', (m['voucherId'],))
    with (folder / 'mysql-create-time.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['second','persisted']); w.writeheader(); w.writerows(db_bins)
    # DB create_time is second resolution and assigned before commit; use observed committed count deltas for TPS.
    points = [json.loads(x) for x in (folder / 'metrics.jsonl').read_text().splitlines()]
    deltas = []
    last = None
    for p in points:
        if 'persisted' not in p:
            continue
        if last and p['time'] > last['time']:
            deltas.append(dict(start=last['time'], end=p['time'], orders=p['persisted']-last['persisted'],
                               observed_tps=(p['persisted']-last['persisted'])/(p['time']-last['time'])))
        last = p
    save(folder / 'mysql-observed-tps.json', deltas)
    stable_deltas = [d for d in deltas if d['start']*1000 >= stable_start and d['end']*1000 <= stable_end]
    observed_seconds = sum(d['end']-d['start'] for d in stable_deltas)
    steady['mysql_observed_tps'] = sum(d['orders'] for d in stable_deltas)/observed_seconds if observed_seconds else None
    steady['mysql_observed_seconds'] = observed_seconds
    result = dict(run_id=m['run_id'], parameters=params, all=total, steady=steady,
                  stable_start_ms=stable_start, stable_end_ms=stable_end,
                  token_exhausted_early=truncated, unique_users=unique,
                  steady_window_covered=end >= stable_end-1000,
                  performance_window_valid=not params['smoke'] and not truncated and unique and end >= stable_end-1000 and all(x['category']=='accepted' for x in rows),
                  warning='A publishable duration does not prove stable capacity; inspect backlog trends, errors, client CPU, and repeat runs.')
    save(folder / 'summary.json', result)
    return result


def snapshot_reconcile(m, rows):
    vid = m['voucherId']; r = redis_client()
    orders = query('SELECT id,user_id,status,UNIX_TIMESTAMP(create_time) created_epoch FROM tb_voucher_order WHERE voucher_id=%s', (vid,))
    claims = dict(r.hscan_iter(f'seckill:claim-owner:{vid}', count=1000))
    stock = query('SELECT stock FROM tb_seckill_voucher WHERE voucher_id=%s', (vid,))[0]['stock']
    value = r.get(f'seckill:stock:{vid}')
    result = reconcile(m, rows, orders, claims, stock, int(value) if value is not None else -1, [], {}, audit_refunds=False)
    if r.ttl(f'seckill:stock:{vid}') != -1 or (claims and r.ttl(f'seckill:claim-owner:{vid}') != -1):
        result['problems'].append('nonpermanent_business_keys'); result['data_consistent'] = False; result['create_consistent'] = False
    result['oldest_order_age_seconds'] = time.time() - min(float(o['created_epoch']) for o in orders) if orders else 0
    result['snapshot_at'] = time.time()
    return result


def verify(run_id, wait_seconds=180):
    phase = 'create'
    guard()
    m = load_manifest(run_id); folder = BASE / 'results' / run_id
    rows = read_jtl(folder / 'requests.jtl')
    deadline = time.monotonic() + wait_seconds
    consecutive = 0
    while True:
        try:
            result = snapshot_reconcile(m, rows)
        except Exception as e:
            result = dict(data_consistent=False, create_consistent=False, problems=['snapshot_unavailable'], error=str(e),
                          orders=None, claims=None, active=None, cancelled=None, unresolved=[],
                          snapshot_at=time.time())
        mq = mq_snapshot()
        result.update(mq=mq, phase=phase,
                      boundary='Observed data consistency and delivered offsets only; not a proof that transaction half/delayed messages no longer exist.')
        # Persistence is the acceptance boundary; refunds/closure remain diagnostic only.
        valid = result['create_consistent']
        consecutive = consecutive + 1 if valid else 0
        result['consecutive_clean_samples'] = consecutive
        result['passed'] = consecutive >= 3
        result['state'] = 'CREATE_PERSISTED' if result['passed'] else 'WAITING_OR_UNRESOLVED'
        save(folder / ('verification-' + phase + '.json'), result)
        if result['passed'] or time.monotonic() >= deadline:
            break
        time.sleep(min(5, max(0, deadline-time.monotonic())))
    # Every unresolved HTTP outcome remains explicit; absence in MySQL/Hash is never called a definite failure.
    with (folder / ('unresolved-' + phase + '.csv')).open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['userId','orderId','category','state']); w.writeheader(); w.writerows(result['unresolved'])
    # Preserve exact identities for independent auditing, without exporting tokens.
    for name, records, fields in [
        ('orders', query('SELECT id,user_id,voucher_id,status,create_time FROM tb_voucher_order WHERE voucher_id=%s', (m['voucherId'],)), ['id','user_id','voucher_id','status','create_time']),
        ('claims', [dict(user_id=u,order_id=o) for u,o in redis_client().hscan_iter(f'seckill:claim-owner:{m["voucherId"]}')], ['user_id','order_id'])]:
        with (folder / (name + '-' + phase + '.csv')).open('w', newline='') as f:
            w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(records)
    print(f'Verify {run_id}/{phase}: {result["state"]}; orders={result["orders"]}, claims={result["claims"]}, unresolved={len(result["unresolved"])}')
    return result


def write_report(folder, summary, verification):
    s = summary['steady']
    text = f'''# NearHub 压测记录：{summary['run_id']}

用途：{'仅冒烟验证，不能作为性能结论' if summary['parameters']['smoke'] else '性能候选样本，需结合稳定性与客户端负载审阅'}。

性能窗口有效性：{summary['performance_window_valid']}；CREATE 验收通过：{verification['passed']}。任一为 False 都不能作为稳定容量成绩。

| 稳定窗口指标 | 实测值 |
|---|---:|
| JMeter 尝试吞吐（含发送前失败） | {s['attempt_qps']} |
| 收到 HTTP 响应 QPS | {s['http_qps']} |
| 明确预扣成功 QPS | {s['accepted_qps']} |
| MySQL 可见提交采样 TPS | {s['mysql_observed_tps']} |
| 平均响应 ms | {s['avg_ms']} |
| P95 ms | {s['p95_ms']} |
| P99 ms | {s['p99_ms']} |
| 异常率 | {s['anomaly_rate']} |

业务分类：`{json.dumps(s['categories'], ensure_ascii=False)}`。

观测校验：{verification['state']}；MySQL 订单 {verification['orders']}；归属 {verification['claims']}；未决 {len(verification['unresolved'])}。

完整窗口覆盖：{summary['steady_window_covered']}；提前耗尽身份：{summary['token_exhausted_early']}。
MySQL 提交可见 TPS 见 mysql-observed-tps.json（采样区间平均，不是精确提交时间）；数据库创建秒桶另存 mysql-create-time.csv。
JMeter HTML 的 Error% 是非 accepted 比例，不能当作异常率。202 未计入预扣成功 QPS，事后确认另列。
本报告 HTTP 平均耗时/P95/P99 只统计收到 HTTP 响应的请求；完整采样百分位另见 summary.json 的 attempt_*，避免快速连接失败掩盖响应延迟。

MQ 观测范围为已投递的 CREATE/TIMEOUT 组位点；不包含未投递延迟消息和事务半消息。
本文件不证明 15 分钟关单、后续补偿或所有消息生命周期已经完成。
原始环境与 JVM 记录见 environment.json；请求参数见 parameters.json；完整诊断见 verification-create.json。

验收范围止于 MySQL 成功创建订单；延迟关单和补偿不作为本轮完成条件。
'''
    text = ('CREATE-only (TIMEOUT sends / cancellations disabled): ' + str(summary['parameters'].get('create_only', False)) + '\n\n') + text
    (folder / 'report.md').write_text(text)
    fields = ['run_id','threads','target_rps','http_qps','accepted_qps','mysql_observed_tps','p95_ms','p99_ms','anomaly_rate','orders','verification','performance_window_valid']
    row = dict(run_id=summary['run_id'], threads=summary['parameters']['threads'], target_rps=summary['parameters']['target_rps'],
               **{k:s[k] for k in ['http_qps','accepted_qps','mysql_observed_tps','p95_ms','p99_ms','anomaly_rate']}, orders=verification['orders'],
               verification=verification['state'], performance_window_valid=summary['performance_window_valid'])
    with (folder / 'summary.csv').open('w', newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerow(row)
    # Rebuild aggregate deterministically; never duplicate a rerun of verification.
    with (BASE / 'results/summary.csv').open('w', newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader()
        for p in sorted((BASE / 'results').glob('*/summary.csv')):
            w.writerows(csv.DictReader(p.open()))


def run(run_id, threads, rampup, duration, target_rps=0, smoke=False):
    require(threads > 0 and rampup >= 0 and duration > 0 and target_rps >= 0, 'Invalid load parameters')
    if smoke:
        require(threads <= 5 and duration <= 30, 'Smoke is limited to <=5 threads and <=30 seconds')
    m = load_manifest(run_id)
    if smoke:
        require(m['user_count'] <= 100, 'Smoke requires <=100 users')
    preflight(m, rampup+duration)
    folder = BASE / 'results' / run_id
    folder.mkdir(parents=True, exist_ok=True)
    require(not (folder / 'requests.jtl').exists(), 'Existing JTL is immutable; prepare a new run')
    params = dict(threads=threads,rampup=rampup,duration=duration,target_rps=target_rps,smoke=smoke,
                  create_only=bool(app_process().get('create_only')))
    save(folder / 'parameters.json', params); save(folder / 'manifest.json', m)
    inputs = folder/'inputs'; inputs.mkdir(exist_ok=True)
    for source in (BASE/'jmeter').iterdir():
        if source.is_file(): shutil.copy2(source, inputs/source.name)
    environment(folder / 'environment.json')
    baseline = mq_snapshot(); save(folder / 'mq-baseline.json', baseline)
    require(baseline['progress']['ok'], 'MQ progress unavailable; inspect Broker/consumer')
    query("UPDATE benchmark_runs SET state='RUNNING' WHERE run_id=%s", (run_id,))
    env=os.environ.copy(); c=config()
    if c.get('JMETER_JAVA_HOME'):
        env['JAVA_HOME']=c['JMETER_JAVA_HOME']; env['PATH']=c['JMETER_JAVA_HOME']+'/bin:'+env['PATH']
    env['HEAP']=c.get('JMETER_HEAP','-Xms256m -Xmx1g')
    args = [jmeter(), '-n', '-t', str(BASE / 'jmeter/nearhub-seckill.jmx'), '-q', str(BASE / 'jmeter/results.properties'),
            '-l', str(folder/'requests.jtl'), '-j', str(folder/'jmeter.log'),
            f'-Jthreads={threads}', f'-Jrampup={rampup}', f'-Jduration={duration}', f'-JtargetRps={target_rps}',
            '-Jhost=127.0.0.1', '-Jport='+c['SERVER_PORT'], '-JvoucherId='+str(m['voucherId']),
            '-Jtokens='+str(run_path(run_id)/'tokens.csv'), '-Jclassifier='+str(BASE/'jmeter/classify.groovy'),
            '-JwindowFile='+str(folder/'window-start.txt')]
    stop_event=threading.Event()
    observers=[threading.Thread(target=monitor,args=(m,folder,stop_event)),threading.Thread(target=heavy_monitor,args=(folder,stop_event))]
    for t in observers: t.start()
    log_offset=(RUNTIME/'app.log').stat().st_size
    verification=None
    run_error=None
    try:
        with (folder/'jmeter-console.log').open('w') as log:
            p=subprocess.Popen(args,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            save(folder/'jmeter-process.json',dict(pid=p.pid,heap=env['HEAP'],java_home=env.get('JAVA_HOME','PATH default')))
            next_jvm_sample = 0
            while p.poll() is None:
                tree=process_tree(p.pid)
                with (folder/'jmeter-process.jsonl').open('a') as f:
                    f.write(json.dumps(dict(time=time.time(),processes=tree))+'\n')
                clients=[x for x in tree if Path(x['executable']).name=='java']
                if clients and time.time() >= next_jvm_sample:
                    next_jvm_sample=time.time()+float(c.get('MQ_SAMPLE_SECONDS',30))
                    try:
                        # The JDK8 helper can use the target JVM's local platform connector.
                        point=jvm_snapshot(dict(pid=clients[0]['pid'],java_home=str(app_home())))
                    except Exception as e:
                        point=dict(error=str(e),sampled_at=time.time())
                    save(folder/('jmeter-jvm-'+str(int(time.time()))+'.json'),point)
                time.sleep(2)
        require(p.returncode == 0, 'JMeter failed; see jmeter-console.log and jmeter.log')
        require('ERROR' not in (folder/'jmeter.log').read_text(), 'JMeter logged ERROR; inspect retained log')
        verification=verify(run_id,180)
    except Exception as e:
        run_error=str(e)
        save(folder/'execution-error.json',dict(error=run_error,at=time.time()))
        verification=dict(state='OBSERVATION_UNAVAILABLE', passed=False, orders=None, claims=None, unresolved=[],
                          scope='No persistence conclusion; verification failed to run', error=run_error)
        save(folder/'verification-create.json',verification)
    finally:
        if 'p' in locals() and p.poll() != 0:
            # Terminate the complete launcher/Java group, including an orphan if the shell failed.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(p.pid,signal.SIGTERM)
            p.wait(timeout=20)
        stop_event.set()
        for t in observers: t.join(timeout=150)
        try:
            query("UPDATE benchmark_runs SET state='USED' WHERE run_id=%s",(run_id,))
        except Exception as e:
            save(folder/'finalization-error.json',dict(error=str(e),at=time.time()))
        with (RUNTIME/'app.log').open('rb') as f:
            f.seek(log_offset); (folder/'app.log').write_bytes(f.read())
    rows=read_jtl(folder/'requests.jtl')
    # Persist HTTP evidence even if the database is temporarily inaccessible.
    try:
        summary=summarize(folder,m,params,rows)
    except Exception as e:
        save(folder/'database-report-error.json',dict(error=str(e),at=time.time()))
        (folder/'mysql-create-time.csv').write_text('second,persisted\n')
        summary=summarize(folder,m,params,rows)
    with (folder/'html-generation.log').open('w') as log:
        p=subprocess.run([jmeter(),'-g',str(folder/'requests.jtl'),'-q',str(BASE/'jmeter/results.properties'),'-o',str(folder/'html'),'-j',str(folder/'html-jmeter.log')],cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT)
    require(p.returncode == 0 and (folder/'html/index.html').exists(),'HTML report failed; raw evidence retained')
    write_report(folder,summary,verification)
    require(run_error is None,'Run/observation failed; reports retained: '+str(run_error))
    require(verification['passed'],'Consistency / MQ unresolved; later rounds stopped')
    require(summary['all']['anomaly_rate']==0 and not summary['all']['categories'].get('pending'), 'HTTP anomalies/pending outcomes observed; later rounds stopped')
    require(smoke or summary['performance_window_valid'],'Insufficient users/window; sample invalid and later rounds stopped')
    print('Reports: '+str(folder))
    return summary


def cleanup(destroy=False, confirmation=None):
    guard()
    status=dict(environment_id=identity(),at=time.time(),runs=query('SELECT * FROM benchmark_runs'),
                unpaid=query('SELECT COUNT(*) n FROM tb_voucher_order WHERE status=1'),
                compensation=query('SELECT status,COUNT(*) n FROM tb_order_stock_compensation GROUP BY status'),mq=mq_snapshot())
    save(BASE/'results/retirement.json',status)
    if destroy:
        require(confirmation==identity(),'Destruction requires --confirm with environment ID from .runtime/environment.json')
    stop_app()
    compose('stop', timeout=120)
    if not destroy:
        print('Isolated application and middleware stopped; ALL volumes/evidence retained. No business records deleted.')
        return
    # Retire the entire sealed namespace, including its Broker store, at once. Never row-delete beneath live MQ.
    require(not any(c['State']['Running'] for c in owned_containers()),'All isolated containers must be stopped')
    compose('down','--volumes',timeout=120)
    (RUNTIME/'environment.json').unlink()
    # Old result folders remain as evidence; new bootstrap has a new environment ID.
    print('Isolated namespace retired (pending messages discarded, NOT reported as drained). Evidence/tokens retained locally.')


def report(run_id):
    run_path(run_id)  # Validate ID; reports remain readable after environment retirement.
    folder = BASE / 'results' / run_id
    summary = summarize(folder, read(folder/'manifest.json'), read(folder/'parameters.json'), read_jtl(folder/'requests.jtl'))
    write_report(folder, summary, read(folder/'verification-create.json'))
    print('Summary regenerated from saved evidence: ' + str(folder/'report.md'))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='cmd',required=True)
    for name in ('init','up','start','check','stop'):
        sub.add_parser(name)
    p=sub.add_parser('prepare'); p.add_argument('--run-id',required=True); p.add_argument('--users',type=int,default=100000); p.add_argument('--stock',type=int,default=100000)
    p=sub.add_parser('run'); p.add_argument('--run-id',required=True); p.add_argument('--threads',type=int,default=100); p.add_argument('--rampup',type=int,default=30); p.add_argument('--duration',type=int,default=60); p.add_argument('--target-rps',type=float,default=0); p.add_argument('--smoke',action='store_true')
    p=sub.add_parser('verify'); p.add_argument('--run-id',required=True); p.add_argument('--wait',type=int,default=180)
    p=sub.add_parser('cleanup'); p.add_argument('--destroy-isolated',action='store_true'); p.add_argument('--confirm')
    p=sub.add_parser('report'); p.add_argument('--run-id',required=True)
    p=sub.add_parser('fresh'); p.add_argument('--create-only',action='store_true')
    p=sub.add_parser('suite'); p.add_argument('--prefix',required=True); p.add_argument('--levels',default='100,200,500,800,1000'); p.add_argument('--users',type=int,default=100000); p.add_argument('--stock',type=int,default=100000); p.add_argument('--rampup',type=int,default=30); p.add_argument('--duration',type=int,default=60); p.add_argument('--target-rps',type=float,default=0)
    args=parser.parse_args()
    os.umask(0o077); RUNTIME.mkdir(parents=True,exist_ok=True)
    # One mutating orchestration at a time. Observations are allowed concurrently.
    with (RUNTIME/'operation.lock').open('w') as lock:
        if args.cmd not in ('check','verify'):
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if args.cmd=='init': init()
        elif args.cmd=='up': up()
        elif args.cmd=='start': start()
        elif args.cmd=='stop': stop_app()
        elif args.cmd=='check': check()
        elif args.cmd=='prepare': prepare(args.run_id,args.users,args.stock)
        elif args.cmd=='run': run(args.run_id,args.threads,args.rampup,args.duration,args.target_rps,args.smoke)
        elif args.cmd=='verify':
            require(verify(args.run_id,args.wait)['passed'],'Verification unresolved; inspect result JSON')
        elif args.cmd=='cleanup': cleanup(args.destroy_isolated,args.confirm)
        elif args.cmd=='report': report(args.run_id)
        elif args.cmd=='fresh': fresh(args.create_only)
        elif args.cmd=='suite':
            for level in [int(x) for x in args.levels.split(',')]:
                run_id=f'{args.prefix}-{level}'
                prepare(run_id,args.users,args.stock)
                run(run_id,level,args.rampup,args.duration,args.target_rps)
                print('CREATE persisted; proceeding to the next fresh voucher without waiting for closure.',flush=True)


if __name__=='__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted; preserve this run and inspect in-flight orders before any new run.', file=sys.stderr)
        sys.exit(130)
    except (RuntimeError, OSError, subprocess.SubprocessError) as e:
        print('ERROR: '+str(e),file=sys.stderr)
        sys.exit(1)
