import pathlib
from collections import namedtuple

import pandas as pd

from table_handler import identify_table_origin, mandatory_columns


FilteredData = namedtuple("FilteredData", "data, illegal")


def read_data(fname: pathlib.Path):
    """Reads the data to disk.
    Input can be a CSV or .XLSX file.
    """
    if "xls" in fname.suffix:
        return pd.read_excel(fname)

    if "csv" in fname.suffix:
        return pd.read_csv(fname, header=0)


def filter_unneeded_rows(data: pd.DataFrame) -> FilteredData:
    """Removes rows which aren't needed in the DF.
    If a row contains one of the designated symbols it drops them and records them.
    Returns an Filtered object which contains information about the retained and
    removed rows.
    """
    allowed_values = ["BUY", "SELL"]
    correct_rows = data["Action"].isin(allowed_values)
    num_of_ok_rows = correct_rows.sum()
    if num_of_ok_rows == len(data):
        return FilteredData(data, {})
    legal_rows = data.loc[correct_rows, :]
    illegal_rows = data.loc[~correct_rows, :]
    unique_illegal = illegal_rows["Action"].unique()
    uniques = {}
    for unique in unique_illegal:
        values = data.loc[data["Action"] == unique, :]
        uniques[unique] = (len(values), values.index.to_numpy() + 1)
    return FilteredData(legal_rows, uniques)


def format_result(filtered: FilteredData, original_size: int) -> str:
    """Formats the result of the computation to a table
    understabable by a lay person.
    """
    if len(filtered.data) == original_size:
        return f"All {original_size} rows were converted successfully."
    formatted = f"{len(filtered.data)} rows converted successfully. Illegal rows were:\n\n"
    df = pd.DataFrame(filtered.illegal).transpose()
    df = df.rename(columns={0: "Number of rows", 1: "Row Index"})
    df.index.name = 'Action'
    formatted = formatted + repr(df)
    return formatted


def run(file) -> str:
    data = read_data(file)
    try:
        returned = identify_table_origin(data.columns)(data)
    except (NotImplementedError, KeyError):
        return "Unknown table format. Please contact the application's author."
    try:
        assert all(col in returned.columns for col in mandatory_columns)
    except AssertionError:
        return "Internal Error. Please contact the application's author."
    filtered = filter_unneeded_rows(returned)
    new_fname = file.stem + '_converted' + '.csv'
    new_fname = file.with_name(new_fname)
    try:
        filtered.data.to_csv(new_fname, index=False, float_format="%f")
    except PermissionError:
        return "Unable to save file in folder. Please make sure it exists and that you have sufficient permissions to write to that directory, and try again."
    formatted = format_result(filtered, len(returned))
    return formatted


if __name__ == "__main__":
    binance0 = pathlib.Path("examples/Binance Trades.xlsx")
    bit2c0 = pathlib.Path("examples/bit2c-financial-report-2016__1_ (1).xlsx")
    bit2c1 = pathlib.Path("examples/bit2c-financial-report-2018.xlsx")
    bitfinex0 = pathlib.Path("examples/Bitfinex Trades .csv")
    bittrex0 = pathlib.Path("examples/BittrexOrderHistory_2017.csv")
    cex0 = pathlib.Path("examples/CEX Trades.csv")
    exodus0 = pathlib.Path("examples/Exodus - all-txs-2019-03-13_04-58-29.csv")
    ledgers0 = pathlib.Path("examples/ledgers.csv")
    lqui0 = pathlib.Path("examples/lqui.csv")
    member0 = pathlib.Path(
        "examples/member123_trades_FROM_Mon-Dec-16-2013_TO_Sun-Dec-15-2019_ON_2019-12-15T21-47-19.528Z_edited.csv"
    )
    shapeshift0 = pathlib.Path("examples/Shapeshift Tx - Sheet1.csv")
    trade0 = pathlib.Path("examples/tradeHistory.csv")
    trades0 = pathlib.Path("examples/trades (1).xlsx")
    files = [
        binance0,
        bit2c0,
        bit2c1,
        bitfinex0,
        bittrex0,
        cex0,
        exodus0,
        ledgers0,
        lqui0,
        member0,
        shapeshift0,
        trade0,
        trades0,
    ]
    for file in files:
        run(file)
