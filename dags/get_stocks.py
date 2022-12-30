from airflow.decorators import dag, task
from airflow.macros import ds_add
import pendulum
from os.path import join
from pathlib import Path
from yfinance import Ticker

tickers = [
  'AAPL',
  'MSFT',
  'GOOG',
  'TSLA'
]

@task()
def get_history(ticker: str, ds=None, ds_nodash=None):
  file_path = join(Path(__file__).parents[1], 'stocks', ticker, f'{ticker}_{ds_nodash}.csv')
  Path(file_path).parent.mkdir(parents=True, exist_ok=True)
  Ticker(ticker).history(
    period='1d',
    interval='1h',
    start=ds_add(ds, -1),
    end=ds,
    prepost=True
  ).to_csv(file_path)

@dag(
  schedule_interval='0 0 * * 2-6',
  start_date=pendulum.datetime(2022, 1, 1, tz='UTC'),
  catchup=True
)
def get_stocks_dag():
  for ticker in tickers:
    get_history.override(task_id=ticker)(ticker)

dag = get_stocks_dag()
