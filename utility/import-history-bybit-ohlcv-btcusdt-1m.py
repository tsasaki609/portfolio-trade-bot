# ByBitのOHLCVデータをMySQLに蓄積する

import logging
from dotenv import load_dotenv
import sys
import os
import pymysql
from datetime import datetime, timedelta, timezone
import ccxt

# Lambda上ではロガーに対してログレベル設定しないと出力されず、
# ローカル環境ではロギングに対して設定しないと出力されずの謎挙動がある
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

load_dotenv()

connection = pymysql.connect(
    host=os.environ.get('DB_HOST_HISTORY'),
    user=os.environ.get('DB_USER_HISTORY'),
    password=os.environ.get('DB_PASSWORD_HISTORY'),
    database='crypto_history',
    cursorclass=pymysql.cursors.DictCursor)
connection.autocommit(False)
logger.info('success create connection')

exchange = ccxt.bybit()

def _get_target_day():
    logger.info(f'start {sys._getframe().f_code.co_name}')
    with connection.cursor() as cursor:
        sql = '''SELECT DISTINCT date(time) AS time
        FROM bybit_ohlcv_btcusdt_1m
        ORDER BY time DESC
        LIMIT 1'''
        cursor.execute(sql)
        row = cursor.fetchone()
    if row is None:
        # データがないときは2020-04-01から取得する
        target_day_list = []
        now = datetime.now(timezone(timedelta(hours=0)))
        start_day = datetime(2020, 4, 1, 0, 0, 0, 0, timezone(timedelta(hours=0)))
        end_day = now - timedelta(days=1)
        for offset in range(0, (end_day - start_day).days): # 昨日から５日分を取得する
            day = start_day + timedelta(days=offset)
            target_day_list.append(day.replace(hour=0, minute=0, second=0, microsecond=0))
        return target_day_list

    base_day = datetime.strptime(f'{row["time"]} 00:00:00+0000', '%Y-%m-%d %H:%M:%S%z')
    now = datetime.now(timezone(timedelta(hours=0)))

    # 既に保存してある日の翌日〜前日を返却する
    nodata_period = now - (base_day + timedelta(days=1))
    target_day_list = []
    for offset in range(0, nodata_period.days):
        target_day_list.append(base_day + timedelta(days=1 + offset))
    logger.info(f'end {sys._getframe().f_code.co_name}')
    return target_day_list

def _get_ohlcv(date):
    logger.info(f'start {sys._getframe().f_code.co_name}')
    # 指定された期間のOHLCVをタブルのリストで返す

    # Bybitは３時間単位での1分足を取得するのがキリの良い限界なので、1440/180=8分割して取得する
    record_list = []
    for offset in range(0, 8):
        response = exchange.fetch_ohlcv(
            'BTC/USDT',
            timeframe='1m',
            since=int((date + timedelta(hours=(3 * offset))).timestamp())*1000,
            limit=180)
        
        if len(response) != 180:
            error_message = f'failure to get 180 ohlcv / actual {len(response)} ohlcv'
            logger.error(error_message)
            raise Exception(error_message)
        
        for record in response:
            time = datetime.fromtimestamp(record[0] / 1000, timezone(timedelta(hours=0)))
            record_list.append((time, *record[1:]))

    logger.info(f'end {sys._getframe().f_code.co_name}')
    return record_list

def _bulk_import(ohlcv_records):
    logger.info(f'start {sys._getframe().f_code.co_name}')
    connection.begin()

    with connection.cursor() as cursor:
        sql = '''INSERT INTO bybit_ohlcv_btcusdt_1m (
            time,
            open,
            high,
            low,
            close,
            volume
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        ) ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close),
            volume = VALUES(volume)'''
        # for record in ohlcv_records:
        #     logger.info(f'insert time={record[0]}')
        #     cursor.execute(sql, record)
        cursor.executemany(sql, ohlcv_records)

    logger.info(f'success import {len(ohlcv_records)} records / {str(ohlcv_records[0][0].date())}')
    connection.commit()
    logger.info(f'end {sys._getframe().f_code.co_name}')

def main(event=None, context=None):
    # DBから既存データを読み込んで足りない分を補完する
    # 既存データの最新を取得して、実行日との差分を埋めていく
    # APIの制約により無限の過去を取得することはできないので適当な時点を起点とする
    # 歯抜けは別のチェック処理で発生してないことを担保する

    logger.info(f'start {sys._getframe().f_code.co_name}')
    target_day = _get_target_day()

    for d in sorted(target_day):
        # 初回インポート時はわりと多くの対象データがあるので日単位で入れる
        ohlcv_records = _get_ohlcv(d)

        if len(ohlcv_records) == 0:
            logger.info(f'not found records / {str(d.date())}')
        else:
            _bulk_import(ohlcv_records)

    logger.info(f'end {sys._getframe().f_code.co_name}')
    return {
        'statusCode': 200
    }

if __name__ == '__main__':
    main()
