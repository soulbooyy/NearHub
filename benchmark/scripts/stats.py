"""Pure report calculations; completion-time windows, no inferred business success."""
import collections
import csv
import math

EXPECTED = {'accepted', 'sold_out', 'duplicate', 'rate_limited', 'pending'}


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)] if values else None


def read_jtl(path):
    with open(path, newline='') as f:
        return [r for r in csv.DictReader(f) if r['label'] == 'POST seckill']


def window(rows, start_ms, end_ms):
    selected = [r for r in rows if start_ms <= int(r['timeStamp']) + int(r['elapsed']) < end_ms]
    counts = collections.Counter(r['category'] for r in selected)
    elapsed = [int(r['elapsed']) for r in selected]
    responses = [r for r in selected if r.get('responseCode', '200').isdigit()]
    http_elapsed = [int(r['elapsed']) for r in responses]
    seconds = (end_ms - start_ms) / 1000
    return dict(seconds=seconds, completed=len(selected), http_completed=len(responses), categories=dict(counts),
                attempt_qps=len(selected) / seconds if seconds else None,
                http_qps=len(responses) / seconds if seconds else None,
                accepted_qps=counts['accepted'] / seconds if seconds else None,
                avg_ms=sum(http_elapsed) / len(http_elapsed) if http_elapsed else None,
                p95_ms=percentile(http_elapsed, .95), p99_ms=percentile(http_elapsed, .99),
                attempt_avg_ms=sum(elapsed) / len(elapsed) if elapsed else None,
                attempt_p95_ms=percentile(elapsed, .95), attempt_p99_ms=percentile(elapsed, .99),
                anomaly_rate=sum(v for k, v in counts.items() if k not in EXPECTED) / len(selected) if selected else None,
                nonaccepted_rate=1 - counts['accepted'] / len(selected) if selected else None)


def reconcile(manifest, rows, orders, claims, db_stock, redis_stock, tasks, markers, audit_refunds=True):
    """Compare exact (user, voucher, order), including uncertainty and cancellations."""
    stock = manifest['stock']
    order_map = {str(o['id']): o for o in orders}
    by_user = collections.defaultdict(list)
    for o in orders:
        by_user[str(o['user_id'])].append(o)
    problems = []
    if any(len(v) > 1 for v in by_user.values()):
        problems.append('duplicate_user_voucher')
    if len(orders) > stock or len(claims) > stock or min(db_stock, redis_stock) < 0:
        problems.append('oversell')
    for uid, oid in claims.items():
        o = order_map.get(oid)
        if not o or str(o['user_id']) != uid:
            problems.append('claim_without_matching_order:' + oid)
    for o in orders:
        if claims.get(str(o['user_id'])) != str(o['id']):
            problems.append('order_without_matching_claim:' + str(o['id']))
    accepted_missing, uncertain, resolved = [], [], []
    not_sent = 0
    for r in rows:
        oid, uid = r.get('orderId', ''), r['userId']
        o = order_map.get(oid)
        matches = o is not None and str(o['user_id']) == uid
        preconnect_failure = (r['category'] == 'transport_error' and r.get('sentBytes') == '0'
                              and r.get('responseCode') in {
                                  'Non HTTP response code: java.net.BindException',
                                  'Non HTTP response code: org.apache.http.conn.ConnectTimeoutException'})
        if preconnect_failure:
            not_sent += 1
            if uid in claims or uid in by_user:
                problems.append('preconnect_failure_has_order_evidence:' + uid)
            continue
        if r['category'] == 'accepted' and (not matches or claims.get(uid) != oid):
            accepted_missing.append(oid)
        elif r['category'] in {'pending', 'transport_error', 'server_error', 'unexpected'}:
            # If a response was lost, the unique user/voucher identity can find its original order.
            candidates = by_user.get(uid, [])
            if len(candidates) == 1 and claims.get(uid) == str(candidates[0]['id']) and (not oid or str(candidates[0]['id']) == oid):
                resolved.append(dict(userId=uid, orderId=str(candidates[0]['id']), category=r['category']))
            else:
                uncertain.append(dict(userId=uid, orderId=oid, category=r['category'], state='UNRESOLVED'))
        elif r['category'] in {'sold_out', 'duplicate', 'rate_limited'} and uid in claims:
            problems.append('rejected_identity_has_claim:' + uid)
    active = sum(o['status'] != 4 for o in orders)
    task_map = {str(t['order_id']): t for t in tasks}
    returned = 0
    for o in orders:
        oid = str(o['id'])
        marker = markers.get(oid)
        if o['status'] == 4:
            t = task_map.get(oid)
            if not t or t['status'] != 'DONE':
                problems.append('cancel_compensation_incomplete:' + oid)
            if marker == f"{manifest['voucherId']}:{o['user_id']}:{oid}":
                returned += 1
            else:
                problems.append('cancel_marker_missing_or_invalid:' + oid)
        elif marker:
            problems.append('active_order_has_refund_marker:' + oid)
    if any(t['status'] != 'DONE' for t in tasks):
        problems.append('unfinished_compensation')
    if db_stock != stock - active:
        problems.append('mysql_stock_mismatch')
    if redis_stock != stock - len(claims) + returned:
        problems.append('redis_stock_mismatch')
    if redis_stock != db_stock:
        problems.append('cross_store_stock_mismatch')
    if accepted_missing:
        problems.append('accepted_missing')
    if uncertain:
        problems.append('unresolved_http_outcomes')
    if not rows:
        problems.append('no_http_evidence')
    attempted = {r['userId'] for r in rows}
    if set(claims) - attempted or set(by_user) - attempted:
        problems.append('identities_without_http_evidence')
    # A cancelled row still proves creation. Refund lag must not block a CREATE-only benchmark.
    refund_prefixes = ('cancel_compensation_', 'cancel_marker_', 'unfinished_compensation', 'active_order_has_refund_marker')
    stock_checks = {'mysql_stock_mismatch', 'redis_stock_mismatch', 'cross_store_stock_mismatch'}
    creation_problems = [p for p in problems if not p.startswith(refund_prefixes)
                         and not (len(orders) != active and p in stock_checks)]
    if not audit_refunds:
        problems = creation_problems
    return dict(refund_audit_performed=audit_refunds, create_consistent=not creation_problems, creation_problems=creation_problems,
                data_consistent=not problems, problems=problems, accepted_missing=accepted_missing,
                unresolved=uncertain, resolved_uncertain=resolved, confirmed_not_sent=not_sent,
                orders=len(orders), claims=len(claims),
                active=active, cancelled=len(orders)-active, mysql_stock=db_stock, redis_stock=redis_stock,
                compensation_status=dict(collections.Counter(t['status'] for t in tasks)))
