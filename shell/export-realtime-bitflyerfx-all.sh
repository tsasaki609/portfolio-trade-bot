#!/bin/bash
cat | xargs -I{} sh -c 'python utility/export-realtime-bitflyerfx.py {} > /dev/null 2>&1 &' << EOS
lightning_board_snapshot_BTC_JPY
lightning_board_snapshot_FX_BTC_JPY
lightning_board_BTC_JPY
lightning_board_FX_BTC_JPY
lightning_executions_FX_BTC_JPY
lightning_executions_BTC_JPY
EOS
