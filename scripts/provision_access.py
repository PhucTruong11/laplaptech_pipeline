"""Provision only LapLapTech roles/objects; admin config is mounted for this job only."""
import json
import os
from pathlib import Path
import sys

from dotenv import dotenv_values
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import encrypt_password

DATABASE = 'laplaptech_pipeline_dev'
PIPELINE = 'laplaptech_pipeline'
DASHBOARD = 'laplaptech_dashboard'
SCHEMAS = ('raw', 'public_bronze', 'public_silver', 'public_gold')


def configure_access(connection, passwords):
    """One transaction; no DROP/REASSIGN OWNED, no password rotation on rerun."""
    with connection:
        with connection.cursor() as cursor:
            cursor.execute('SET LOCAL lock_timeout = \'15s\'')
            cursor.execute('SELECT pg_advisory_xact_lock(74621, 991)')
            cursor.execute('SELECT current_database(), current_user')
            if cursor.fetchone() != (DATABASE, 'laplaptech'):
                raise RuntimeError('Provisioning target must be the local LapLapTech database/admin')
            for role, limit in ((PIPELINE, 8), (DASHBOARD, 4)):
                cursor.execute('SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls FROM pg_roles WHERE rolname=%s', (role,))
                flags = cursor.fetchone()
                if flags is None:
                    verifier = encrypt_password(passwords[role], role, connection, algorithm='scram-sha-256')
                    cursor.execute(sql.SQL('CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT {} PASSWORD %s').format(sql.Identifier(role), sql.Literal(limit)), (verifier,))
                elif any(flags):
                    raise RuntimeError('Existing project role has unexpected admin privileges')
                else:
                    # Verify existing credentials without resetting a role owned elsewhere.
                    params = connection.get_dsn_parameters()
                    probe = psycopg2.connect(host=params['host'], port=params['port'], dbname=DATABASE,
                                             user=role, password=passwords[role], connect_timeout=10)
                    probe.close()
                cursor.execute('SELECT 1 FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=%s)', (role,))
                if cursor.fetchone():
                    raise RuntimeError('Project login must not inherit another role')

            cursor.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(sql.Identifier(DATABASE)))
            cursor.execute(sql.SQL('GRANT CONNECT, TEMPORARY, CREATE ON DATABASE {} TO {}').format(sql.Identifier(DATABASE), sql.Identifier(PIPELINE)))
            cursor.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(sql.Identifier(DATABASE), sql.Identifier(DASHBOARD)))
            cursor.execute('REVOKE CREATE ON SCHEMA public FROM PUBLIC')
            cursor.execute(sql.SQL('REVOKE ALL ON SCHEMA public FROM {}').format(sql.Identifier(DASHBOARD)))
            cursor.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(sql.Identifier(PIPELINE)))
            for schema in SCHEMAS:
                cursor.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}').format(sql.Identifier(schema), sql.Identifier(PIPELINE)))
                cursor.execute(sql.SQL('ALTER SCHEMA {} OWNER TO {}').format(sql.Identifier(schema), sql.Identifier(PIPELINE)))
                cursor.execute(sql.SQL('REVOKE ALL ON SCHEMA {} FROM PUBLIC, {}').format(sql.Identifier(schema), sql.Identifier(DASHBOARD)))
                cursor.execute('SELECT c.relname,c.relkind,pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relkind IN (\'r\',\'p\',\'v\',\'m\',\'S\',\'f\')', (schema,))
                for name, kind, owner in cursor.fetchall():
                    if owner not in ('laplaptech', PIPELINE):
                        raise RuntimeError('Unexpected owner in project schema; investigate before migration')
                    command = {'v': 'VIEW', 'm': 'MATERIALIZED VIEW', 'S': 'SEQUENCE', 'f': 'FOREIGN TABLE'}.get(kind, 'TABLE')
                    cursor.execute(sql.SQL('ALTER '+command+' {}.{} OWNER TO {}').format(sql.Identifier(schema), sql.Identifier(name), sql.Identifier(PIPELINE)))
                cursor.execute(sql.SQL('REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM PUBLIC, {}').format(sql.Identifier(schema), sql.Identifier(DASHBOARD)))
                cursor.execute(sql.SQL('REVOKE ALL ON ALL SEQUENCES IN SCHEMA {} FROM PUBLIC, {}').format(sql.Identifier(schema), sql.Identifier(DASHBOARD)))
            cursor.execute(sql.SQL('GRANT USAGE ON SCHEMA public_gold TO {}').format(sql.Identifier(DASHBOARD)))
            cursor.execute(sql.SQL('GRANT SELECT ON ALL TABLES IN SCHEMA public_gold TO {}').format(sql.Identifier(DASHBOARD)))
            cursor.execute(sql.SQL('ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA public_gold GRANT SELECT ON TABLES TO {}').format(sql.Identifier(PIPELINE), sql.Identifier(DASHBOARD)))
            cursor.execute(sql.SQL("ALTER ROLE {} IN DATABASE {} SET default_transaction_read_only = on").format(sql.Identifier(DASHBOARD), sql.Identifier(DATABASE)))
            # Small, bounded pools; project roles never get the admin password.
            for role, limit in ((PIPELINE, 8), (DASHBOARD, 4)):
                cursor.execute(sql.SQL('ALTER ROLE {} CONNECTION LIMIT {}').format(sql.Identifier(role), sql.Literal(limit)))


def main():
    platform = dotenv_values('/platform.env', interpolate=False)
    source = dotenv_values('/import.env', interpolate=False)
    passwords = {PIPELINE: platform.get('LAPLAPTECH_PIPELINE_PASSWORD'),
                 DASHBOARD: platform.get('LAPLAPTECH_DASHBOARD_PASSWORD')}
    if any(not value or value.startswith(('GENERATE_', 'REPLACE_')) for value in passwords.values()):
        raise RuntimeError('Missing private project passwords')
    if not source.get('CLICKHOUSE_PASSWORD') or source['CLICKHOUSE_PASSWORD'].startswith(('REPLACE_', 'your_')):
        raise RuntimeError('Fill the public ClickHouse source login first')
    connection = psycopg2.connect(host='warehouse-laplaptech', port=5432, dbname=DATABASE,
                                 user='laplaptech', password=platform['WAREHOUSE_PASSWORD'], connect_timeout=10)
    try:
        configure_access(connection, passwords)
    finally:
        connection.close()
    config = {k: v for k, v in source.items() if k.startswith(('CLICKHOUSE_', 'INGESTION_')) and v is not None}
    config.update(POSTGRES_HOST='warehouse-laplaptech', POSTGRES_PORT='5432', POSTGRES_USER=PIPELINE,
                  POSTGRES_PASSWORD=passwords[PIPELINE], POSTGRES_DATABASE=DATABASE,
                  POSTGRES_SSLMODE='prefer', POSTGRES_CREATE_DATABASE='false', DBT_THREADS=os.environ['DBT_THREADS'])
    directory = Path('/run/laplaptech')
    directory.mkdir(parents=True, exist_ok=True)
    quote = lambda value: "'" + value.replace('\\', '\\\\').replace("'", "\\'") + "'"
    temporary = directory / 'pipeline.env.new'
    temporary.write_text('\n'.join(k+'='+quote(v) for k, v in config.items())+'\n', encoding='utf-8')
    os.chown(temporary, 10001, 10001)
    temporary.chmod(0o400)
    temporary.replace(directory/'pipeline.env')
    os.chown('/artifacts', 10001, 10001)
    print(json.dumps({'access': 'configured', 'database': DATABASE, 'pipeline': PIPELINE,
                      'dashboard': DASHBOARD, 'passwords_printed': False}))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Database exceptions can contain query literals/connection details.
        print('Access setup failed: '+type(exc).__name__+'. Inspect target/permissions; credentials were not printed.', file=sys.stderr)
        raise SystemExit(1)
