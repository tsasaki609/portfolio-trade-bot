# BitFlyerFXのOHLCVデータをMySQLに蓄積する

import logging
from dotenv import load_dotenv
import sys
import os
import pymysql
from datetime import datetime, timedelta, timezone
import requests

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

def _get_target_day():
    logger.info(f'start {sys._getframe().f_code.co_name}')
    with connection.cursor() as cursor:
        sql = '''SELECT DISTINCT date(time) AS time
        FROM bitflyerfx_ohlcv_btcjpy_1m
        ORDER BY time DESC
        LIMIT 1'''
        cursor.execute(sql)
        row = cursor.fetchone()
    if row is None:
        target_day_list = []
        now = datetime.now(timezone(timedelta(hours=9)))
        base_day = now - timedelta(days=1)
        for offset in range(0, 5): # 昨日から５日分を取得する
            day = base_day - timedelta(days=offset)
            target_day_list.append(day.replace(hour=0, minute=0, second=0, microsecond=0))
        return target_day_list

    # 取得できた日付が５日前より古いときはなんらか対処考えないといけないので、とりあえず異常終了
    # 厳密には６日前を指定しても取れるデータはあるけど、実行時間によっては丸一日分取得できないことが大半なのでやめておく
    # 日本の取引所なのでJSTを基準として取り扱う
    base_day = datetime.strptime(f'{row["time"]} 00:00:00+0900', '%Y-%m-%d %H:%M:%S%z')
    now = datetime.now(timezone(timedelta(hours=9)))
    max_allowed_period = 7 # 当日も含めた取得可能な最大期間
    if ((now - base_day).days > (max_allowed_period - 2)):
        raise Exception(f'nodata period is too long / base_day:{base_day}')

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
    response = requests.get('https://min-api.cryptocompare.com/data/v2/histominute', params={
       'fsym': "BTC",
       'tsym': "JPY",
       'limit': 1440,
       'e': "bitFlyerFX",
       'toTs': int((date + timedelta(days=1)).timestamp())
       })
    if response.status_code != 200:
        raise Exception(f'unexpected status code {response.status_code}')
    
    record_list = []
    d = response.json()['Data']
    logger.info(f'from {d["TimeFrom"]} / to {d["TimeTo"]}')
    for record in response.json()['Data']['Data']:
        time = datetime.fromtimestamp(record['time'], timezone(timedelta(hours=9)))
        if time.date() != date.date():
            continue
        record_list.append((
            time,
            record['open'],
            record['high'],
            record['low'],
            record['close'],
            record['volumefrom']))
    logger.info(f'end {sys._getframe().f_code.co_name}')
    return record_list

def _bulk_import(ohlcv_records):
    logger.info(f'start {sys._getframe().f_code.co_name}')
    connection.begin()

    with connection.cursor() as cursor:
        sql = '''INSERT INTO bitflyerfx_ohlcv_btcjpy_1m (
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
        cursor.executemany(sql, ohlcv_records)

    logger.info(f'success import {len(ohlcv_records)} records')
    connection.commit()
    logger.info(f'end {sys._getframe().f_code.co_name}')

def main(event=None, context=None):
    # DBから既存データを読み込んで足りない分を補完する
    # 既存データの最新を取得して、実行日との差分を埋めていく
    # APIの制約により無限の過去を取得することはできないので上限超えてたら異常終了する
    # 歯抜けは別のチェック処理で発生してないことを担保する

    logger.info(f'start {sys._getframe().f_code.co_name}')
    target_day = _get_target_day()

    ohlcv_records = []
    for d in sorted(target_day):
        ohlcv_records.extend(_get_ohlcv(d))

    if len(ohlcv_records) == 0:
        logger.info('not found records')
    else:
        _bulk_import(ohlcv_records)

    logger.info(f'end {sys._getframe().f_code.co_name}')
    return {
        'statusCode': 200
    }

if __name__ == '__main__':
    main()
