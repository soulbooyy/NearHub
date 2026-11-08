#!/usr/bin/env python3
"""Recompute public HTTP/TPS metrics offline; never sends requests or accesses middleware."""
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / 'scripts'))
from stats import window


def read(name):
    return json.loads((HERE / name).read_text())


def main():
    for line in (HERE / 'SHA256SUMS').read_text().splitlines():
        digest, name = line.split('  ', 1)
        assert hashlib.sha256((HERE / name).read_bytes()).hexdigest() == digest, name
    with gzip.open(HERE / 'samples.csv.gz', 'rt', newline='') as f:
        rows = list(csv.DictReader(f))
    assert [int(r['sample_index']) for r in rows] == list(range(1, len(rows) + 1))
    assert len(rows) == 147309
    assert not ({'token', 'userId', 'orderId', 'URL'} & set(rows[0]))
    saved = read('summary.json')
    start = min(int(r['timeStamp']) for r in rows)
    end = max(int(r['timeStamp']) + int(r['elapsed']) for r in rows) + 1
    for name, left, right in [('all', start, end), ('steady', saved['stable_start_ms'], saved['stable_end_ms'])]:
        measured = window(rows, left, right)
        for key, value in measured.items():
            target = saved[name][key]
            assert math.isclose(value, target, rel_tol=1e-10, abs_tol=1e-9) if isinstance(value, (int, float)) else value == target, (name, key)
    points = [json.loads(line) for line in (HERE / 'metrics.jsonl').read_text().splitlines()]
    valid = [p for p in points if 'persisted' in p]
    intervals = [(a, b) for a, b in zip(valid, valid[1:]) if a['time'] * 1000 >= saved['stable_start_ms'] and b['time'] * 1000 <= saved['stable_end_ms']]
    seconds = sum(b['time'] - a['time'] for a, b in intervals)
    tps = sum(b['persisted'] - a['persisted'] for a, b in intervals) / seconds
    assert math.isclose(tps, saved['steady']['mysql_observed_tps'])
    assert math.isclose(seconds, saved['steady']['mysql_observed_seconds'])
    verification = read('verification.json')
    assert verification['orders'] == verification['claims'] == 147231
    assert verification['mysql_stock'] == verification['redis_stock'] == 500000 - 147231
    assert verification['accepted_missing_count'] == 0 and verification['resolved_uncertain_count'] == 100
    assert verification['unresolved_count'] == 78 and verification['passed'] is False
    with (HERE / 'mysql-create-time.csv').open() as f:
        assert sum(int(r['persisted']) for r in csv.DictReader(f)) == 147231
    pending = read('pending-78.json')
    assert len(pending) == 78
    assert {c['sample_index'] for c in pending} == {int(r['sample_index']) for r in rows if r['category'] == 'pending'}
    for c in pending:
        r = rows[c['sample_index'] - 1]
        assert r['responseCode'] == '202' and int(r['elapsed']) == c['http_elapsed_ms']
        assert 'CODE: 2' in c['cause'] and 'TIMEOUT_CLEAN_QUEUE' in c['cause']
        assert c['mysql_matches'] == c['redis_claim_matches'] == c['commitlog_matches'] == 0
    audit = read('commitlog-audit.json')
    assert audit['records'] == audit['body_crc_checked'] == 441708 and audit['pending_hits'] == []
    for topic in ['RMQ_SYS_TRANS_HALF_TOPIC', 'order-seckill-topic']:
        assert audit['positive_control_coverage'][topic]['persisted_orders_matched'] == 147231
    assert all(q['min_offset'] == q['max_offset'] == 0 for q in read('scheduled-queues.json')['queues'])
    print('PASS: 147309 published timing samples reproduce all/steady QPS, latency, categories and sampled MySQL TPS.')
    print('PASS: published persistence/forensic summaries are internally consistent; original identity and CommitLog audits remain private.')


if __name__ == '__main__':
    main()
