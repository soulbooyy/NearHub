import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from stats import reconcile, window


class ReportsTest(unittest.TestCase):
    def test_create_acceptance_does_not_wait_for_refunds(self):
        args=list(self.case()); args[2][0]['status']=4; args[4]=3
        result=reconcile(*args)
        self.assertTrue(result['create_consistent'])
        self.assertFalse(result['data_consistent'])
        create_only=reconcile(*args,audit_refunds=False)
        self.assertTrue(create_only['data_consistent'])
        self.assertFalse(create_only['refund_audit_performed'])

    def case(self, category='accepted'):
        return ({'stock': 3, 'voucherId': 9},
                [{'userId':'1','orderId':'100','category':category}],
                [{'id':100,'user_id':1,'status':1}], {'1':'100'}, 2, 2, [], {})

    def test_exact_identity(self):
        self.assertTrue(reconcile(*self.case())['data_consistent'])
        args=list(self.case()); args[3]={'1':'101'}
        r=reconcile(*args)
        self.assertFalse(r['data_consistent'])
        self.assertEqual(r['accepted_missing'], ['100'])

    def test_pending_is_not_accepted_but_can_resolve(self):
        r=reconcile(*self.case('pending'))
        self.assertTrue(r['data_consistent'])
        self.assertEqual(len(r['resolved_uncertain']),1)
        args=list(self.case('pending')); args[2]=[]; args[3]={}; args[4]=args[5]=3
        r=reconcile(*args)
        self.assertEqual(r['unresolved'][0]['state'],'UNRESOLVED')
        self.assertFalse(r['data_consistent'])

    def test_transport_timeout_resolves_by_unique_user(self):
        args=list(self.case('transport_error')); args[1][0]['orderId']=''
        self.assertTrue(reconcile(*args)['data_consistent'])

    def test_zero_byte_connect_failure_is_not_an_uncertain_submission(self):
        args=list(self.case('transport_error'))
        args[1][0].update(orderId='', sentBytes='0', responseCode='Non HTTP response code: java.net.BindException')
        args[2]=[]; args[3]={}; args[4]=args[5]=3
        result=reconcile(*args)
        self.assertEqual(result['confirmed_not_sent'],1)
        self.assertEqual(result['unresolved'],[])
        # A generic/read timeout is still uncertain, even with no recorded sent-byte counter.
        args[1][0]['responseCode']='Non HTTP response code: java.net.SocketTimeoutException'
        self.assertEqual(len(reconcile(*args)['unresolved']),1)

    def test_partial_redis_write_is_detected(self):
        args=list(self.case('pending')); args[2]=[]; args[3]={}; args[4]=3
        r=reconcile(*args)
        self.assertIn('redis_stock_mismatch', r['problems'])

    def test_cancellation_retains_claim_and_requires_refund_evidence(self):
        args=list(self.case()); args[2][0]['status']=4; args[4]=args[5]=3
        args[6]=[{'order_id':100,'status':'DONE'}]; args[7]={'100':'9:1:100'}
        self.assertTrue(reconcile(*args)['data_consistent'])
        args[7]={}
        self.assertFalse(reconcile(*args)['data_consistent'])

    def test_oversell_and_duplicate_user(self):
        args=list(self.case()); args[0]['stock']=1
        args[2].append({'id':101,'user_id':1,'status':1})
        r=reconcile(*args)
        self.assertIn('oversell',r['problems']); self.assertIn('duplicate_user_voucher',r['problems'])

    def test_completion_window_and_exception_rate(self):
        rows=[dict(timeStamp='0',elapsed='100',category='accepted'),
              dict(timeStamp='100',elapsed='200',category='pending'),
              dict(timeStamp='300',elapsed='100',category='sold_out'),
              dict(timeStamp='500',elapsed='100',category='server_error')]
        r=window(rows,100,600)
        self.assertEqual(r['completed'],3)
        self.assertEqual(r['accepted_qps'],2)
        self.assertEqual(r['anomaly_rate'],0)
        self.assertEqual(window(rows,0,1000)['anomaly_rate'],.25)

    def test_preconnect_errors_do_not_inflate_http_qps_or_hide_latency(self):
        rows=[dict(timeStamp='0',elapsed='500',category='accepted',responseCode='200'),
              dict(timeStamp='0',elapsed='0',category='transport_error',responseCode='Non HTTP response code: java.net.BindException')]
        result=window(rows,0,1000)
        self.assertEqual(result['attempt_qps'],2)
        self.assertEqual(result['http_qps'],1)
        self.assertEqual(result['avg_ms'],500)

    def test_no_requests_never_passes(self):
        args=list(self.case()); args[1]=[]
        self.assertFalse(reconcile(*args)['data_consistent'])


if __name__=='__main__': unittest.main()
