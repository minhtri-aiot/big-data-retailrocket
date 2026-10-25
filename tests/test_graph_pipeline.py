"""New primary Graph path: numerical, temporal, artifact, API and scheduler contracts."""
import copy
from contextlib import closing
import importlib.util
import json
import os
from pathlib import Path
import runpy
import sqlite3
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from pipeline.config import atomic_json
from pipeline.graph_training import CONFIG, checksum, measure
from service.app import create_app
from service.store import BundleStore, Settings


class GraphContracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.serving = self.root / "runs/g1/serving"
        self.serving.mkdir(parents=True)
        with closing(sqlite3.connect(self.serving/"session_neighbors.sqlite")) as db:
            db.execute("CREATE TABLE neighbors(seed,candidate,score)")
            db.executemany("INSERT INTO neighbors VALUES(?,?,?)", [(1,2,1.0),(1,3,.25)])
            db.execute("CREATE TABLE transitions(seed,candidate,score)")
            db.execute("INSERT INTO transitions VALUES(1,4,2)")
            db.execute("CREATE TABLE metadata(key,value)")
            db.execute("INSERT INTO metadata VALUES('model_version','\"g1-v1\"')")
            db.execute("CREATE TABLE user_context(visitorid,itemid,timestamp,event)")
            db.execute("INSERT INTO user_context VALUES(7,1,1,'view')")
            db.commit()
        with closing(sqlite3.connect(self.serving/"seen_items.sqlite")) as db:
            db.execute("CREATE TABLE seen_items(visitorid,itemid)")
            db.execute("INSERT INTO seen_items VALUES(7,1)"); db.commit()
        self.bundle = {"schema_version":2, "run_id":"g1", "algorithm":"graph_session", "model_version":"g1-v1",
            "metric_model_version":"g1-eval", "trained_at_utc":"2026-10-24T00:00:00+00:00",
            "recommendations":{}, "seen_items":{}, "seen_items_store":"seen_items.sqlite", "sample_users":[7],
            "popular_items":[{"itemid":i,"score":1.0} for i in [1,9,8]], "metrics":{"heldout_test":{"hit_rate_at_k":.5}},
            "session_model":{"index_file":"session_neighbors.sqlite", "model_version":"g1-v1", "metric_model_version":"g1-eval",
                "configuration":copy.deepcopy(CONFIG), "metrics":{}},
            "artifact_hashes":{name:checksum(self.serving/name) for name in ['session_neighbors.sqlite','seen_items.sqlite']}}
        self.write()
        atomic_json(self.root/"latest_run.json", {"run_id":"g1"})
        self.client = TestClient(create_app(Settings(output_root=self.root, require_graph=True)))
        self.addCleanup(self.client.close)

    def write(self):
        atomic_json(self.serving/"model_bundle.json", self.bundle)

    def test_graph_primary_and_static_endpoint_use_same_scorer(self):
        payload = {"events":[{"itemid":1,"timestamp":1}],"k":3}
        a = self.client.post('/api/session/recommendations',json=payload).json()
        b = self.client.get('/api/recommendations/7?k=3').json()
        self.assertEqual([r['itemid'] for r in a['items']], [2,3,4])
        self.assertEqual(a['items'],b['items'])
        self.assertEqual(a['algorithm'],'graph_session')
        self.assertEqual(a['model_version'],a['session_model_version'])
        self.assertEqual(self.client.get('/api/metrics').json()['model_metrics'],self.bundle['metrics'])

    def test_cold_user_cold_item_and_empty_context_fallback(self):
        for payload in [{"events":[]},{"events":[{"itemid":2147483647}]}]:
            r=self.client.post('/api/session/recommendations',json=payload)
            self.assertEqual(r.status_code,200)
            self.assertEqual(r.json()['source'],'popular_fallback')
        self.assertEqual(self.client.get('/api/recommendations/999').json()['source'],'popular_fallback')

    def test_runtime_refuses_legacy_als(self):
        self.bundle['algorithm']='als_implicit'; self.write()
        self.assertEqual(self.client.get('/health/ready').status_code,503)

    def test_wrong_version_and_modified_index_fail_closed(self):
        self.bundle['session_model']['model_version']='wrong'; self.write()
        self.assertEqual(self.client.get('/health/ready').status_code,503)
        self.bundle['session_model']['model_version']='g1-v1'; self.write()
        self.assertEqual(self.client.get('/health/ready').status_code,200)
        with closing(sqlite3.connect(self.serving/'session_neighbors.sqlite')) as db:
            db.execute('UPDATE neighbors SET score=8'); db.commit()
        self.assertEqual(self.client.get('/health/ready').status_code,503)

    def test_invalid_future_and_duplicate_context(self):
        self.assertEqual(self.client.post('/api/session/recommendations',json={'events':[{'itemid':1,'timestamp':9999999999999}]}).status_code,422)
        r=self.client.post('/api/session/recommendations',json={'events':[{'itemid':1},{'itemid':1}]}).json()
        ids=[row['itemid'] for row in r['items']]
        self.assertEqual(len(ids),len(set(ids))); self.assertNotIn(1,ids)

    def test_malformed_graph_configuration_is_not_ready(self):
        self.bundle['session_model']['configuration']='invalid'; self.write()
        self.assertEqual(self.client.get('/health/ready').status_code,503)

    def test_hitrate_is_not_multilabel_recall(self):
        m=measure([2,9],[2,3],10)
        self.assertEqual(m['precision_at_k'],.1)
        self.assertEqual(m['recall_at_k'],.5)
        self.assertEqual(m['hit_rate_at_k'],1)


@unittest.skipUnless(os.getenv('BIGDATA_RUN_GRAPH_TESTS')=='1','Enable real Spark Graph tests explicitly')
class GraphSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark=SparkSession.builder.master('local[2]').appName('graph-contract').config('spark.sql.shuffle.partitions','2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def events(self):
        return self.spark.createDataFrame([(1,1000,1,'view'),(1,2000,2,'view'),(1,2000,3,'view'),
            (1,3000,1,'transaction'),(1,4000,4,'view'),(1,2000000,5,'view'),
            (2,1000,1,'view'),(2,2000,2,'view')], 'visitorid int,timestamp long,itemid int,event string')

    def test_relations_conditional_support_ties_and_gap(self):
        from pipeline.graph_training import build_relations
        co,tr=build_relations(self.events())
        try:
            c={(r.seed,r.candidate):r.score for r in co.collect()}
            t={(r.seed,r.candidate):r.score for r in tr.collect()}
            self.assertAlmostEqual(c[(1,2)],.4) # both of two users; shrink 2/(2+3)
            self.assertAlmostEqual(c[(1,3)],.125) # one of two; shrink 1/(1+3)
            self.assertEqual(t[(1,2)],2)
            self.assertNotIn((2,3),t) # equal timestamp
            self.assertNotIn((3,1),t) # target already seen
            self.assertNotIn((4,5),t) # >30 min inactivity
        finally:
            co.unpersist();tr.unpersist()

    def test_session_cohort_contains_only_past_and_all_novel_remaining_labels(self):
        from pipeline.graph_training import make_queries
        rows,profile=make_queries(self.events(),1500,2000000)
        user=next(r for r in rows if r['visitorid']==1)
        self.assertEqual(user['actual'],[2,3,4])
        self.assertEqual(user['seen'],[1])
        self.assertEqual(len(user['context']),1)
        self.assertTrue(all(e['timestamp']<user['target_timestamp'] for e in user['context']))

    def test_invalid_ids_rejected_and_duplicate_events_do_not_inflate(self):
        from pipeline.graph_training import normalize_events
        events=self.events()
        self.assertEqual(normalize_events(events.union(events)).count(),events.count())
        bad=self.spark.createDataFrame([(1,-1,2,'view')],'visitorid int,timestamp long,itemid int,event string')
        with self.assertRaises(ValueError): normalize_events(bad)

    def test_missing_event_is_rejected_instead_of_silently_dropped(self):
        from pipeline.graph_training import normalize_events
        invalid = self.spark.createDataFrame(
            [(1, 1000, 1, None), (1, 2000, 2, 'view')],
            'visitorid int,timestamp long,itemid int,event string')
        with self.assertRaisesRegex(ValueError, 'Invalid timestamp/ID/event'):
            normalize_events(invalid)


class SchedulerContracts(unittest.TestCase):
    def test_worker_forbids_other_algorithms(self):
        from pipeline.job_server import app
        with TestClient(app) as client:
            self.assertEqual(client.post('/jobs/retrain',json={'request_id':'x','algorithm':'als_implicit'}).status_code,422)

    def test_worker_records_graph_version_and_rejects_non_graph_result(self):
        from pipeline import job_server as worker
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ,{'OUTPUT_ROOT':directory}):
            report={'run_id':'r','elapsed_seconds':1,'stages':{'training':{'algorithm':'graph_session','model_version':'v1'}}}
            job={'job_id':'abc','run_id':'r','source_run_id':'s','quick':True}
            with patch('pipeline.runner.execute',return_value=report): worker.run_job(job)
            self.assertEqual(job['result']['model_version'],'v1')
            report['stages']['training']['algorithm']='als_implicit'
            with patch('pipeline.runner.execute',return_value=report): worker.run_job(job)
            self.assertEqual(job['status'],'failed')

    def test_dag_schedule_dependencies_and_http_graph_contract(self):
        # A faithful import stub checks DAG wiring without claiming an Airflow run.
        calls={}
        class Dag:
            def __init__(self,**kw):calls['dag']=kw
            def __enter__(self):return self
            def __exit__(self,*a):pass
        class Task:
            def __init__(self,**kw): self.id=kw['task_id'];calls[self.id]=kw
            def __rshift__(self,other):calls['edge']=(self.id,other.id)
        def module(**kw):
            m=ModuleType('stub');m.__dict__.update(kw);return m
        modules={'pendulum':module(datetime=lambda *a,**k:(a,k)),'airflow':module(DAG=Dag),
            'airflow.exceptions':module(AirflowException=RuntimeError),
            'airflow.operators':module(),'airflow.operators.python':module(PythonOperator=Task),
            'airflow.sensors':module(),'airflow.sensors.python':module(PythonSensor=Task)}
        with patch.dict(sys.modules,modules):
            dag=runpy.run_path(str(Path(__file__).resolve().parents[1]/'orchestration/dags/retrain_recommendations.py'))
        self.assertEqual(calls['dag']['schedule'],'0 2 * * *')
        self.assertEqual(calls['edge'],('submit_retraining','wait_for_evaluated_model'))
        self.assertEqual(calls['dag']['start_date'][1]['tz'],'Asia/Ho_Chi_Minh')
        class Response:
            def __init__(self,value):self.value=json.dumps(value)
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def read(self):return self.value
        with patch('urllib.request.urlopen',return_value=Response({'status':'queued','job_id':'abc'})) as url:
            self.assertEqual(dag['submit'](dag_run=SimpleNamespace(run_id='scheduled-x')),'abc')
            self.assertEqual(json.loads(url.call_args.args[0].data)['algorithm'],'graph_session')
        context={'ti':SimpleNamespace(xcom_pull=lambda **kw:'abc')}
        for algorithm,expected in [('graph_session',True),('als_implicit',False)]:
            with patch('urllib.request.urlopen',return_value=Response({'status':'completed','result':{'algorithm':algorithm}})):
                if expected:self.assertTrue(dag['completed'](**context))
                else:
                    with self.assertRaises(RuntimeError):dag['completed'](**context)


if __name__=='__main__':unittest.main()
