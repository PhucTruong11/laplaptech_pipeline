"""Characterize late data with real loaders/dbt on an isolated fixture DB.

Requires DEV_DATABASE_URL and --reset-fixture. Never accepts the live warehouse.
Expected limitations are reported separately from supported cases and failures.
"""
import argparse
from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
from time import perf_counter
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text

from ingestion import clickhouse_to_postgres as ingestion
from scripts.dev_fixtures import FixtureClickHouse
from scripts.run_dev_pipeline import validated_dev_url

ROOT = Path(__file__).resolve().parents[1]
TS = 'event_received_on_server_timestamp'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reset-fixture', action='store_true', required=True,
                        help='Explicitly allow reset of four fixture schemas in the test DB')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    url = validated_dev_url(os.environ.get('DEV_DATABASE_URL'), source='fixture', reset_fixture=True)
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    engine = create_engine(url, pool_size=2, max_overflow=0)
    report = {'started_at_utc':datetime.now(timezone.utc).isoformat(),
              'scope':'Synthetic characterization; real PostgreSQL loaders and dbt; not a production throughput benchmark',
              'database':url.database, 'cases':[], 'builds':[], 'ingestion_runs':[], 'status':'failed'}
    env = {'POSTGRES_HOST':url.host,'POSTGRES_PORT':str(url.port or 5432),
           'POSTGRES_USER':url.username,'POSTGRES_PASSWORD':url.password,
           'POSTGRES_DATABASE':url.database,'POSTGRES_CREATE_DATABASE':'false',
           'POSTGRES_SSLMODE':'prefer','INGESTION_LOOKBACK_SECONDS':'7200',
           'DBT_SEND_ANONYMOUS_USAGE_STATS':'false','DBT_THREADS':'2'}
    source = FixtureClickHouse()
    anchor = int(datetime.now(timezone.utc).timestamp())

    def event(event_id, stamp, *, session=None, page='DeviceDetail'):
        return {'id':event_id,'session_id':session or f's{event_id}','event_name':'pageview',
                'event_local_timestamp':stamp-10,TS:stamp,
                'event_data':json.dumps({'page_name':page,'device_id':'1'}),
                'device':json.dumps({'os_name':'Windows','device_type':'Desktop'})}

    source.tables['user_event_tracking'] = pd.DataFrame([
        event(1,anchor-14400), event(2,anchor-7200), event(3,anchor)])

    def append(row):
        source.tables['user_event_tracking'] = pd.concat([
            source.tables['user_event_tracking'],pd.DataFrame([row])],ignore_index=True)

    def replace(row):
        frame=source.tables['user_event_tracking']
        source.tables['user_event_tracking']=pd.concat([frame[frame.id!=row['id']],pd.DataFrame([row])],ignore_index=True)

    def scalar(query, **params):
        with engine.connect() as connection:
            return connection.execute(text(query),params).scalar_one()

    def has(schema, event_id):
        table='user_event_tracking' if schema=='raw' else 'silver_user_event_tracking'
        return scalar(f'SELECT count(*) FROM {schema}.{table} WHERE id=:id',id=event_id)==1

    def snapshot():
        with engine.connect() as connection:
            raw=connection.execute(text(f'SELECT id,{TS},session_id,event_data FROM raw.user_event_tracking ORDER BY id')).all()
            silver=connection.execute(text('SELECT id,extract(epoch FROM server_timestamp),session_id,event_data FROM public_silver.silver_user_event_tracking ORDER BY id')).all()
            gold=connection.execute(text('SELECT event_date,total_events,total_sessions FROM public_gold.gold_daily_site_kpis ORDER BY event_date')).all()
        return {'raw':[tuple(r) for r in raw],'silver':[tuple(r) for r in silver],'gold':[tuple(r) for r in gold]}

    def case(name, outcome, **evidence):
        report['cases'].append({'name':name,'outcome':outcome,**evidence})
        print(name+': '+outcome,flush=True)

    def ingest(name, **kwargs):
        start=perf_counter()
        with patch.object(ingestion,'get_clickhouse_client',return_value=source):
            ingestion.run_pipeline(**kwargs)
        report['ingestion_runs'].append({'name':name,'seconds':round(perf_counter()-start,4)})

    def dbt(name, *, full=False, expect_failure=False, freshness=False):
        from dbt.cli.main import dbtRunner
        target=out/name
        arguments=(['source','freshness'] if freshness else ['build']+(['--full-refresh'] if full else []))
        log=io.StringIO(); start=perf_counter()
        with redirect_stdout(log), redirect_stderr(log):
            result=dbtRunner().invoke(arguments+['--project-dir',str(ROOT/'dbt'),'--profiles-dir',str(ROOT/'dbt'),
                '--target-path',str(target/'target'),'--log-path',str(target/'logs')])
        safe_log=log.getvalue().replace(url.password,'[REDACTED]')
        target.mkdir(parents=True,exist_ok=True)
        (target/'console.log').write_text(safe_log,encoding='utf-8')
        counts=Counter(); failures=[]
        if result.result is not None and not freshness:
            for item in result.result.results:
                status=getattr(item.status,'value',str(item.status)); counts[status]+=1
                if status in ('fail','error','runtime error'): failures.append(item.node.name)
        report['builds'].append({'name':name,'seconds':round(perf_counter()-start,4),'success':result.success,
            'statuses':dict(counts),'failures':failures,'exception_type':type(result.exception).__name__ if result.exception else None})
        if expect_failure:
            assert not result.success and 'assert_raw_to_silver' in failures, f'{name}: expected cross-layer test failure'
        else:
            assert result.success, f'{name}: unexpected dbt failure; inspect sanitized logs'

    def reconciled():
        valid=scalar('SELECT count(*) FROM raw.user_event_tracking WHERE event_name IS NOT NULL AND session_id IS NOT NULL')
        actual=scalar('SELECT count(*) FROM public_silver.silver_user_event_tracking')
        gold=scalar('SELECT coalesce(sum(total_events),0) FROM public_gold.gold_daily_site_kpis')
        assert valid==actual==gold, 'Raw/Silver/Gold totals mismatch'
        expected=Counter(); sessions={}
        with engine.connect() as connection:
            rows=connection.execute(text(f'SELECT session_id,{TS} FROM raw.user_event_tracking WHERE event_name IS NOT NULL AND session_id IS NOT NULL'))
            for session, stamp in rows:
                day=datetime.fromtimestamp(float(stamp),ZoneInfo('Asia/Ho_Chi_Minh')).date()
                expected[day]+=1
                sessions.setdefault(day,set()).add(session)
            daily=connection.execute(text('SELECT event_date,total_events,total_sessions FROM public_gold.gold_daily_site_kpis')).all()
        assert {r[0]:int(r[1]) for r in daily}==dict(expected), 'Daily event attribution mismatch'
        assert {r[0]:int(r[2]) for r in daily}=={d:len(s) for d,s in sessions.items()}, 'Daily session attribution mismatch'
        return {'valid_raw':int(valid),'silver':int(actual),'gold_events':int(gold),'daily_metric_mismatches':0}

    try:
        with patch.dict(os.environ,env):
            with engine.begin() as connection:
                for schema in ('public_gold','public_silver','public_bronze','raw'):
                    connection.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
            ingest('baseline'); dbt('freshness',freshness=True); dbt('baseline',full=True)
            case('baseline','supported',**reconciled())

            append(event(10,anchor-3600)); ingest('inside_window'); dbt('inside_window')
            assert has('raw',10) and has('public_silver',10)
            case('late_inside_2h','supported',**reconciled())

            append(event(11,anchor-7200)); ingest('exact_boundary'); dbt('exact_boundary')
            assert has('raw',11) and has('public_silver',11)
            case('late_exact_2h_boundary','supported',**reconciled())

            append(event(12,anchor-7201)); ingest('outside_window'); dbt('outside_window')
            assert not has('raw',12) and not has('public_silver',12)
            case('late_outside_2h_normal_run','limitation_confirmed',raw_received=False,silver_received=False)

            batch=ingestion.extract_incremental(source,'user_event_tracking',TS,engine,
                start_epoch=anchor-7201,end_epoch=anchor-7200)
            assert batch.id.tolist()==[12]
            case('bounded_range_start_inclusive_end_exclusive','supported',selected_ids=batch.id.tolist())
            ingest('bounded_backfill',start_epoch=anchor-7201,end_epoch=anchor-7200)
            dbt('backfill_then_incremental',expect_failure=True)
            assert has('raw',12) and not has('public_silver',12)
            case('raw_backfill_then_incremental_silver','limitation_detected_by_test',raw_received=True,silver_received=False)
            dbt('backfill_full_recovery',full=True)
            assert has('public_silver',12)
            case('backfill_then_full_rebuild','supported',**reconciled())

            replace(event(1,anchor-10800,page='Home'))
            ingest('old_correction_normal')
            assert scalar(f'SELECT {TS} FROM raw.user_event_tracking WHERE id=1')==anchor-14400
            ingest('old_correction_backfill',start_epoch=anchor-10800,end_epoch=anchor-10799)
            dbt('old_correction_incremental',expect_failure=True)
            dbt('old_correction_full_recovery',full=True)
            assert scalar('SELECT extract(epoch FROM server_timestamp) FROM public_silver.silver_user_event_tracking WHERE id=1')==anchor-10800
            case('old_correction_recovered_by_backfill_and_rebuild','supported',**reconciled())

            invalid=event(10,anchor-3500); invalid['session_id']=None; replace(invalid)
            ingest('valid_to_invalid_correction'); dbt('invalid_correction_incremental',expect_failure=True)
            assert has('public_silver',10)
            case('valid_to_invalid_incremental','limitation_detected_by_test',stale_silver_row=True)
            dbt('invalid_correction_recovery',full=True)
            assert not has('public_silver',10)
            case('valid_to_invalid_full_rebuild','supported',**reconciled())

            before=snapshot(); ingest('replay'); dbt('replay')
            assert snapshot()==before
            case('replay_ids_versions_payload_and_daily_totals','supported',**reconciled())

            before=snapshot()
            ingestion.load_incremental_to_postgres(pd.DataFrame([event(3,anchor-1,page='Home')]),'user_event_tracking',engine)
            assert snapshot()==before
            case('older_version_cannot_overwrite','supported')
            try:
                ingestion.load_incremental_to_postgres(pd.DataFrame([event(3,anchor,page='Home')]),'user_event_tracking',engine)
            except ValueError:
                assert snapshot()==before
            else: raise AssertionError('Same-version conflict must fail')
            case('same_version_conflict_rolls_back','supported')

            append(event(13,anchor-9000))
            with patch.dict(os.environ,{'INGESTION_LOOKBACK_SECONDS':'10800'}):
                ingest('raw_3h_silver_2h'); dbt('lookback_config_mismatch',expect_failure=True)
            assert has('raw',13) and not has('public_silver',13)
            case('raw_3h_silver_fixed_2h','limitation_detected_by_test',raw_received=True,silver_received=False)
            dbt('final_full_recovery',full=True)

            delayed=event(14,anchor+1)
            delayed['event_local_timestamp']=anchor-86400
            append(delayed); ingest('old_client_time_new_server_time'); dbt('old_client_time_new_server_time')
            assert has('raw',14) and has('public_silver',14)
            assert scalar('SELECT extract(epoch FROM event_timestamp) FROM public_silver.silver_user_event_tracking WHERE id=14')==anchor-86400
            case('client_time_1_day_old_server_time_new','supported',**reconciled())
            report['final_counts']=reconciled()
            report['status']='completed_with_known_limitations'
    except Exception as exc:
        report['error_type']=type(exc).__name__
        raise
    finally:
        engine.dispose()
        report['finished_at_utc']=datetime.now(timezone.utc).isoformat()
        (out/'report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
        print('ASSESSMENT_REPORT:',out/'report.json',flush=True)


if __name__=='__main__':
    main()
