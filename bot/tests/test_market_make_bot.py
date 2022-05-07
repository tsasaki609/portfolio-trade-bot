import pytest
from main import market_make_bot
import pandas as pd
import ccxt

@pytest.fixture(autouse=True)
def setup_module():
    market_make_bot.force_stop = False
    market_make_bot.board_fx_btc_jpy = {}
    market_make_bot.ticker_fx_btc_jpy = {}
    market_make_bot.ticker_fx_btc_jpy_history_df = pd.DataFrame([]) # TODO replace mock
    market_make_bot.exchange = ccxt.bitflyer({}) # TODO replace mock
    yield


def test_on_message_board_snapshot_fx_btc_jpy_01():
    message = {
        'params': {
            'message': {
                'mid_price': 1.1,
                'asks': [
                    {
                        'price': 8888.88,
                        'size': 8.8
                    },
                    {
                        'price': 9999.99,
                        'size': 9.9
                    }
                ],
                'bids': [
                    {
                        'price': 6666.66,
                        'size': 6.6
                    },
                    {
                        'price': 7777.77,
                        'size': 7.7
                    }
                ]
            }
        }
    }
    market_make_bot.on_message_board_snapshot_fx_btc_jpy(None, message)
    assert market_make_bot.board_fx_btc_jpy == {
        'mid_price': 1.1,
        'asks': [
            {
                'price': 9999.99,
                'size': 9.9
            },
            {
                'price': 8888.88,
                'size': 8.8
            }
        ],
        'bids': [
            {
                'price': 7777.77,
                'size': 7.7
            },
            {
                'price': 6666.66,
                'size': 6.6
            }
        ]
    }

    message = {
        'params': {
            'message': {
                'mid_price': 1.2,
                'asks': [
                    {
                        'price': 8888.87,
                        'size': 8.7
                    },
                    {
                        'price': 9999.98,
                        'size': 9.8
                    }
                ],
                'bids': [
                    {
                        'price': 6666.65,
                        'size': 6.5
                    },
                    {
                        'price': 7777.76,
                        'size': 7.6
                    }
                ]
            }
        }
    }
    market_make_bot.on_message_board_snapshot_fx_btc_jpy(None, message)
    assert market_make_bot.board_fx_btc_jpy == {
        'mid_price': 1.2,
        'asks': [
            {
                'price': 9999.98,
                'size': 9.8
            },
            {
                'price': 8888.87,
                'size': 8.7
            }
        ],
        'bids': [
            {
                'price': 7777.76,
                'size': 7.6
            },
            {
                'price': 6666.65,
                'size': 6.5
            }
        ]
    }

def test_on_message_board_fx_btc_jpy_01():
    message = {
        'params': {
            'message': {
                'mid_price': 1.1,
                'asks': [
                    {
                        'price': 8888.88,
                        'size': 8.8
                    }
                ],
                'bids': [
                    {
                        'price': 6666.66,
                        'size': 6.6
                    }
                ]
            }
        }
    }
    market_make_bot.on_message_board_fx_btc_jpy(None, message)
    assert market_make_bot.board_fx_btc_jpy == {}
    market_make_bot.board_fx_btc_jpy = {
        'mid_price': 9.9,
        'asks': [],
        'bids': []
    }
    market_make_bot.on_message_board_fx_btc_jpy(None, message)
    assert market_make_bot.board_fx_btc_jpy == {
        'mid_price': 1.1,
        'asks': [
            {
                'price': 8888.88,
                'size': 8.8
            }
        ],
        'bids': [
            {
                'price': 6666.66,
                'size': 6.6
            }
        ]
    }
    message = {
        'params': {
            'message': {
                'mid_price': 1.2,
                'asks': [
                    {
                        'price': 8888.88,
                        'size': 8.9
                    }
                ],
                'bids': [
                    {
                        'price': 6666.66,
                        'size': 6.7
                    }
                ]
            }
        }
    }
    market_make_bot.on_message_board_fx_btc_jpy(None, message)
    assert market_make_bot.board_fx_btc_jpy == {
        'mid_price': 1.2,
        'asks': [
            {
                'price': 8888.88,
                'size': 8.9
            }
        ],
        'bids': [
            {
                'price': 6666.66,
                'size': 6.7
            }
        ]
    }
    message = {
        'params': {
            'message': {
                'mid_price': 0.0,
                'asks': [
                    {
                        'price': 8888.88,
                        'size': 0.0
                    }
                ],
                'bids': [
                    {
                        'price': 6666.66,
                        'size': 0.0
                    }
                ]
            }
        }
    }
    market_make_bot.on_message_board_fx_btc_jpy(None, message)
    assert market_make_bot.board_fx_btc_jpy == {
        'mid_price': 0.0,
        'asks': [],
        'bids': []
    }
