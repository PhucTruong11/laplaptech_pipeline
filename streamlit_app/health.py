"""Check the HTTP server and the presence of every dashboard Gold relation."""
import os
import sys
from urllib.request import urlopen

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from streamlit_app.constants import GOLD_NAMES


def main() -> int:
    engine = None
    try:
        with urlopen('http://127.0.0.1:8501/_stcore/health', timeout=5) as response:
            if response.status != 200:
                return 1
        url = os.getenv('DASHBOARD_DATABASE_URL') or URL.create(
            'postgresql+psycopg2', username=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'], host=os.environ['POSTGRES_HOST'],
            port=int(os.getenv('POSTGRES_PORT', '5432')), database=os.environ['POSTGRES_DATABASE'],
            query={'sslmode': os.getenv('POSTGRES_SSLMODE', 'prefer')},
        )
        engine = create_engine(url, connect_args={'connect_timeout': 5})
        with engine.connect() as connection:
            database, role, admin = connection.execute(text(
                'SELECT current_database(), current_user, rolsuper FROM pg_roles WHERE rolname=current_user'
            )).one()
            if database == 'laplaptech_pipeline_dev' and (role != 'laplaptech_dashboard' or admin):
                return 1
            tables = set(connection.execute(text(
                'SELECT table_name FROM information_schema.tables WHERE table_schema=:schema'
            ), {'schema': os.getenv('GOLD_SCHEMA', 'public_gold')}).scalars())
        return 0 if set(GOLD_NAMES).issubset(tables) else 1
    except Exception as exc:
        print(f'Dashboard not ready: {type(exc).__name__}', file=sys.stderr)
        return 1
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == '__main__':
    raise SystemExit(main())
