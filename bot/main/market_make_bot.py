# なんとなく枠だけ作ってみたものの、勝ち筋が見出せず細部を作り込むモチベもないのでポートフォリオとして再利用してみる
# PoCとしてコンパクトに１ファイルで完結させたかったのでグローバル変数など、あえて使用してます

from loguru import logger
import websocket
import time
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
import ccxt

def str2datetime(value):
    return datetime.strptime(value[:-2] + '+0000', '%Y-%m-%dT%H:%M:%S.%f%z').astimezone(timezone(timedelta(hours=9)))

spread_min_threshold = 0.0001
lot = 0.01

leverage = 2
margin_rate = 1 / leverage
fee = 0
board_fx_btc_jpy = {}
ticker_fx_btc_jpy = {}
ticker_fx_btc_jpy_history_df = pd.DataFrame([])
executions_fx_btc_jpy = {}
executions_fx_btc_jpy_history_df = pd.DataFrame([])
exchange = ccxt.bitflyer({}) # TODO シークレット情報を環境変数から埋め込む
# ASK 指値（メイカー）の売り、成行（テイカー）の買い/BID　指値の買い、成行の売り
# TODO 注文は両建てできるか反対取引になるのか要検証
active_order = {
    'ask': [],
    'bid': []
}

# LONG 買い建玉/SHORT 売り建玉
# 積み増しできるので注文は複数ありえる
# 買いポジ＝プラス、売りポジ＝マイナス
position = {
    'long': [],
    'short': []
}

balance = 0.0

def has_position():
    return len(position['long']) > 0 or len(position['short']) > 0

def calc_effective_ticker(ignore_size = 0.1):
    # 板から実質的な最良気配値を探す

    bid = board_fx_btc_jpy['bids'][0]
    cumulative_bid_size = 0.0
    for b in board_fx_btc_jpy['bids']:
        if cumulative_bid_size + b['size'] > ignore_size:
            # 前までの累積＋今回が無視できないサイズなら直前のBIDを採用する
            break
        else:
            bid = b
            cumulative_bid_size += b['size']

    ask = board_fx_btc_jpy['asks'][-1]
    cumulative_ask_size = 0.0
    for a in reversed(board_fx_btc_jpy['asks']):
        if cumulative_ask_size + a['size'] > ignore_size:
            # 前までの累積＋今回が無視できないサイズなら直前のASKを採用する
            break
        else:
            ask = a
            cumulative_ask_size += a['size']

    return (bid, ask)

def calc_spread(bid=None, ask=None):
    # 引数なしのときは最良気配からのスプレッド
    if not bid:
        bid = board_fx_btc_jpy['bids'][0]
    if not ask:
        ask = board_fx_btc_jpy['asks'][-1]

    # (ASK / BID) - 1
    return (ask['price'] / bid['price']) - 1

def limit_order(side, price, amount):
    logger.debug(f'limit order {side} {price} {amount}')
    # TODO 取引所のAPI叩く
    pass

def cancel_order(order):
    # 注文が通らない＝すれ違いで約定した
    logger.debug(f'cancel order {order}')
    # TODO 取引所のAPI叩く
    pass

def execute_strategy():
    effective_bid, effective_ask = calc_effective_ticker()
    effective_spread = calc_spread(effective_bid, effective_ask)

    # 大前提：リアルタイムに約定情報が反映されている
    # TODO 基本的にはそうなんだけど、万が一ズレると大変なのでフォワードテストを念入りにやる

    # 前提条件：ポジションなし
    # 注文を決める

    # 前提条件：ポジションあり
    # 注文が全て約定：次の注文を決める
    # 注文が部分約定：注文を見直す
    # 注文が約定なし：注文を見直す
    if has_position():
        # アクティブスプレッドを考慮すると約定期待値が低い注文が残ってればキャンセルする
        # 市場が実際どちらに動くかは神のみぞ知るところなので、ひたすら試行回数を増やす
        if active_order['bid'] and active_order['bid']['price'] < effective_bid['price']:
            cancel_order(active_order['bid'])

        if active_order['ask'] and active_order['ask']['price'] > effective_ask['price']:
            cancel_order(active_order['ask'])
    

    if effective_spread <= spread_min_threshold:
        # 実効スプレッドが閾値以下なら欲張らず在庫整理だけする
        # 損切りも兼ねる
        if position['long']:
            limit_order('sell', effective_ask['price'], sum([lo.amount for lo in position['long']]))
        if position['short']:
            limit_order('buy', effective_bid['price'], sum([so.amount for so in position['short']]))
        return

    # ポジションとアクティブな注文を考慮して両サイドの注文を決める
    # ここがデザインできれば苦労しない

    # TODO 在庫戦略、つまりポジション積み増しできるかは余力による
    # 現時点の証拠金比率が予め定めた閾値より低ければ、ロットを積み増すし、高ければ積み増さない
    amount_bid = lot + sum([so.amount for so in position['short']]) - sum([b['size'] for b in active_order['bid']])
    limit_order('buy', effective_bid['price'], amount_bid)

    amount_ask = lot + sum([lo.amount for lo in position['long']]) - sum([a['size'] for a in active_order['ask']])
    limit_order('sell', effective_ask['price'], amount_ask)

def on_message_board_snapshot_fx_btc_jpy(ws, message):
    logger.debug('call snap board')
    board_data = message['params']['message']
    board_fx_btc_jpy['mid_price'] = board_data['mid_price']
    board_fx_btc_jpy['bids'] = sorted(board_data['bids'], key=lambda x: x['price'], reverse=True)
    board_fx_btc_jpy['asks'] = sorted(board_data['asks'], key=lambda x: x['price'], reverse=True)
    execute_strategy()

def on_message_board_fx_btc_jpy(ws, message):
    if not board_fx_btc_jpy:
        # 差分を反映するためにはスナップショットが必要
        logger.info('board snapshot is empty...')
        return

    logger.debug('call board')
    board_data = message['params']['message']
    board_fx_btc_jpy['mid_price'] = board_data['mid_price']
    for bid in board_data['bids']:
        if list(filter(lambda x: x['price'] == bid['price'], board_fx_btc_jpy['bids'])):
            # 板に既にあるとき
            i = [i for i, x in enumerate(board_fx_btc_jpy['bids']) if x['price'] == bid['price']][0]
            if bid['size'] == 0:
                # 板から消す
                del board_fx_btc_jpy['bids'][i]
            else:
                # 置き換える
                board_fx_btc_jpy['bids'][i] = bid
        else:
            # 新しく突っ込む
            board_fx_btc_jpy['bids'].append(bid)
            board_fx_btc_jpy['bids'] = sorted(board_fx_btc_jpy['bids'], key=lambda x: x['price'], reverse=True)
    for ask in board_data['asks']:
        if list(filter(lambda x: x['price'] == ask['price'], board_fx_btc_jpy['asks'])):
            # 板に既にあるとき
            i = [i for i, x in enumerate(board_fx_btc_jpy['asks']) if x['price'] == ask['price']][0]
            if ask['size'] == 0:
                # 板から消す
                del board_fx_btc_jpy['asks'][i]
            else:
                # 置き換える
                board_fx_btc_jpy['asks'][i] = ask
        else:
            # 新しく突っ込む
            board_fx_btc_jpy['asks'].append(ask)
            board_fx_btc_jpy['asks'] = sorted(board_fx_btc_jpy['asks'], key=lambda x: x['price'], reverse=True)
    execute_strategy()

def on_message_ticker_fx_btc_jpy(ws, message):
    global ticker_fx_btc_jpy, ticker_fx_btc_jpy_history_df

    ticker_fx_btc_jpy = message['params']['message']

    index = str2datetime(ticker_fx_btc_jpy['timestamp'])
    # TODO リークは良くないので古い行を削除したいが、とりあえずある程度の時間動かしてみて様子を見る
    ticker_fx_btc_jpy_history_df = pd.concat([ticker_fx_btc_jpy_history_df, pd.DataFrame(ticker_fx_btc_jpy, index=[index])])

def on_message_executions_fx_btc_jpy(ws, message):
    global executions_fx_btc_jpy, executions_fx_btc_jpy_history_df

    executions_fx_btc_jpy = message['params']['message']
    executions_fx_btc_jpy_history_df = pd.concat([executions_fx_btc_jpy_history_df, pd.DataFrame(executions_fx_btc_jpy).set_index('id', drop=False)])

# FX:BTC/JPYの情報を全部受け取っておく
subscribe_channels = {
    'lightning_board_snapshot_FX_BTC_JPY': on_message_board_snapshot_fx_btc_jpy,
    'lightning_board_FX_BTC_JPY': on_message_board_fx_btc_jpy,
    'lightning_ticker_FX_BTC_JPY': on_message_ticker_fx_btc_jpy,
    'lightning_executions_FX_BTC_JPY': on_message_executions_fx_btc_jpy
}

def on_message(ws, message):
    try:
        logger.debug('receive message')
        parsed_message = json.loads(message)
        channel = parsed_message['params']['channel']
        message_handler = subscribe_channels[channel]
        message_handler(ws, parsed_message)
    except Exception:
        logger.exception('unexpected error')
        # TODO 緊急停止処理を入れる（発注時にストップ＆逆ストップ指値入れてればざっくり終了でも良さげ？

def on_open(ws):
    for channel in subscribe_channels.keys():
            ws.send(json.dumps({
                'method': 'subscribe',
                'params': {
                    'channel': channel
                }
            }))
            logger.info(f'subscribe {channel}')

def main():
    # TODO 遅延を考慮する
    # TODO バグ・障害などにより一定期間呼び出されないとき、逆指値以外のシステムトラップが必要か検討する
    #      （フォワードテストを信頼しすぎるのは危険）

    # メンテナンスで切断されて終了してしまうので、無限に回す
    while True:
        try:
            ws = websocket.WebSocketApp(
                'wss://ws.lightstream.bitflyer.com/json-rpc',
                on_message=on_message,
                on_open=on_open,
                on_error=lambda x, e: logger.exception(e),
                on_close=None,
                on_ping=None,
                on_pong=None,
                on_cont_message=None,
                on_data=None)
            ws.run_forever()
        except KeyboardInterrupt:
            # TODO これで拾えるか未検証
            logger.info('terminate bot')
            return
        except Exception as e:
            logger.exception(e)
            time.sleep(1)


if __name__ == '__main__':
    main()
