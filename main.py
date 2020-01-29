import pathlib

import pandas as pd
import numpy as np

from table_handler import identify_table_origin




def read_data(fname: pathlib.Path):
    """Reads the data to disk.
    Input can be a CSV or .XLSX file.
    """
    if 'xls' in fname.suffix:
        return pd.read_excel(fname)

    if 'csv' in fname.suffix:
        return pd.read_csv(fname, header=0,)






if __name__ == "__main__":
    binance0 = pathlib.Path('examples/Binance Trades.xlsx')
    bit2c0 = pathlib.Path('examples/bit2c-financial-report-2016__1_ (1).xlsx')
    bit2c1 = pathlib.Path('examples/bit2c-financial-report-2018.xlsx')
    bitfinex0 = pathlib.Path('examples/Bitfinex Trades .csv')
    bittrex0 = pathlib.Path('examples/BittrexOrderHistory_2017.csv')
    cex0 = pathlib.Path('examples/CEX Trades.csv')
    exodus0 = pathlib.Path('examples/Exodus - all-txs-2019-03-13_04-58-29.csv')
    ledgers0 = pathlib.Path('examples/ledgers.csv')
    lqui0 = pathlib.Path('examples/lqui.csv')
    member0 = pathlib.Path('examples/member123_trades_FROM_Mon-Dec-16-2013_TO_Sun-Dec-15-2019_ON_2019-12-15T21-47-19.528Z_edited.csv')
    shapeshift0 = pathlib.Path('examples/Shapeshift Tx - Sheet1.csv')
    trade0 = pathlib.Path('examples/tradeHistory.csv')
    trades0 = pathlib.Path('examples/trades (1).xlsx')
    data = read_data(trades0)
