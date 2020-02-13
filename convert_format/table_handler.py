"""
This module contains methods that convert each table type
to the correct format.
"""
import pandas as pd
import numpy as np


mandatory_columns = ["Date", "Action", "Symbol", "Volume", "Currency"]
all_columns = mandatory_columns + ["Account", "Total", "Price", "Fee", "FeeCurrency"]

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S %z"

binance0 = (
    "Date(UTC)",
    "Market",
    "Type",
    "Price",
    "Amount",
    "Total",
    "Fee",
    "Fee Coin",
)
bit2c0 = (
    "Date",
    "Action",
    "firstCoin",
    "Currency",
    "Volume",
    "Price",
    "Fee",
    "FeeCurrency",
    "Source",
)

bit2c1 = (
    "id",
    "created",
    "accountAction",
    "firstCoin",
    "secondCoin",
    "firstAmount",
    "secondAmount",
    "price",
    "feeAmount",
    "fee",
    "ref",
)

bitfinex0 = ("#", "PAIR", "AMOUNT", "PRICE", "FEE", "FEE CURRENCY", "DATE", "ORDER ID")
bittrex0 = (
    "Uuid",
    "Exchange",
    "TimeStamp",
    "OrderType",
    "Limit",
    "Quantity",
    "QuantityRemaining",
    "Commission",
    "Price",
    "PricePerUnit",
    "IsConditional",
    "Condition",
    "ConditionTarget",
    "ImmediateOrCancel",
    "Closed",
)
cex0 = (
    "DateUTC",
    "Amount",
    "Symbol",
    "Balance",
    "Type",
    "Pair",
    "FeeSymbol",
    "FeeAmount",
    "Comment",
)
exodus0 = (
    "DATE",
    "TYPE",
    "OUTAMOUNT",
    "OUTCURRENCY",
    "FEEAMOUNT",
    "FEECURRENCY",
    "OUTTXID",
    "OUTTXURL",
    "INAMOUNT",
    "INCURRENCY",
    "INTXID",
    "INTXURL",
    "ORDERID",
)
ledgers0 = (
    "txid",
    "refid",
    "time",
    "type",
    "aclass",
    "asset",
    "amount",
    "fee",
    "balance",
)
lqui0 = (
    "Date",
    "Market",
    "Type",
    "Price",
    "Amount",
    "Total",
    "Fee",
    "OrderId",
    "TradeId",
    "Change Base",
    " Change Quote",
)
member0 = (
    "Symbol",
    "Currency",
    "Action",
    "Volume",
    "PRICE",
    "FEE",
    "FEECURRENCY",
    "DATE",
    "Source",
)
shapeshift0 = (
    "כמות רכישה",
    "מטבע רכישה",
    "כמות מכירה",
    "מטבע מכירה",
    "עמלה (אופציונלי)",
    "מטבע עמלה (אופציונלי)",
    "זירה",
    "אסמכתא (אופציונלי)",
    "תאריך",
)
trade0 = (
    "Date",
    "Market",
    "Category",
    "Type",
    "Price",
    "Amount",
    "Total",
    "Fee",
    "Order Number",
    "Base Total Less Fee",
    "Quote Total Less Fee",
)
trades0 = (
    "Date (UTC)",
    "Instrument",
    "Trade ID",
    "Order ID",
    "Side",
    "Quantity",
    "Price",
    "Volume",
    "Fee",
    "Rebate",
    "Total",
)


def identify_table_origin(columns):
    return table_origin[tuple(columns.to_list())]


def transform_date(col: pd.Series):
    """Transforms a Series containing datetime data to the
    acceptable format.
    """
    return pd.to_datetime(col, utc=True).dt.strftime(DATETIME_FORMAT)


def convert_binance0(data):
    data["Date(UTC)"] = transform_date(data["Date(UTC)"])
    renaming = {
        "Date(UTC)": "Date",
        "Type": "Action",
        "Amount": "Volume",
        "Fee Coin": "Currency",
    }
    renamed = data.rename(columns=renaming)
    renamed["Symbol"] = renamed["Currency"]
    return renamed


def convert_bit2c0(data):
    data["Date"] = transform_date(data["Date"])
    renaming = {"firstCoin": "Symbol"}
    renamed = data.rename(columns=renaming)
    renamed["Action"] = renamed["Action"].str.upper()
    return renamed


def convert_bit2c1(data):
    """First coin-second coin"""
    data["Date"] = transform_date(data["created"])
    renaming = {
        "accountAction": "Action",
        "firstCoin": "Symbol",
        "secondCoin": "Currency",
        "firstAmount": "Volume",
        "feeAmount": "Fee",
        "price": "Price",
    }
    renamed = data.rename(columns=renaming)
    renamed["Action"] = renamed["Action"].str.upper()
    return renamed


def convert_bitfinex0(data):
    """Action? PAIR"""
    data["Date"] = transform_date(data["DATE"])
    renaming = {
        "AMOUNT": "Volume",
        "PRICE": "Price",
        "FEE": "Fee",
        "FEE CURRENCY": "FeeCurrency",
    }
    renamed = data.rename(columns=renaming)
    renamed["Symbol"] = renamed["PAIR"].str.split(pat="/", expand=True)[0]
    renamed["Currency"] = renamed["PAIR"].str.split(pat="/", expand=True)[1]
    renamed["Action"] = ""
    renamed.loc[(renamed["Volume"] > 0), "Action"] = "BUY"
    renamed.loc[(renamed["Volume"] < 0), "Action"] = "SELL"
    renamed["Volume"] = np.abs(renamed["Volume"])
    return renamed


def convert_bittrex0(data):
    """LIMIT_SELL, EXCHANGE, PricePerUnit"""
    data["Date"] = transform_date(data["TimeStamp"])
    renaming = {
        "Quantity": "Volume",
        "Commision": "Fee",
        "Price": "Total",
        "PricePerUnit": "Price",
    }
    renamed = data.rename(columns=renaming)
    renamed["Symbol"] = renamed["Exchange"].str.split("-", expand=True)[1]
    renamed["Currency"] = renamed["Exchange"].str.split("-", expand=True)[0]
    renamed["Action"] = renamed["OrderType"].str.split("_", expand=True)[1]
    return renamed


def convert_cex0(data):
    """Pair"""
    data["Date"] = transform_date(data["DateUTC"])
    renaming = {
        "Type": "Action",
        "FeeSymbol": "FeeCurrency",
        "FeeAmount": "Fee",
        "Amount": "Volume",
    }
    renamed = data.rename(columns=renaming)
    renamed["Currency"] = data["Pair"].str.split("/", expand=True)[1]
    renamed["Action"] = renamed["Action"].str.upper()
    return renamed


def convert_exodus0(data):
    """OUTAMOUNT INAMOUNT only withdrawals and deposits"""
    raise NotImplementedError
    data["Date"] = data["DATE"].str.split("(", expand=True)[0]
    data["Date"] = transform_date(data["Date"])
    renaming = {
        "TYPE": "Action",
        "OUTAMOUNT": "Volume",
        "OUTCURRENCY": "Symbol",
        "FEEAMOUNT": "Fee",
        "FEECURRENCY": "FeeCurrency",
    }
    renamed = data.rename(columns=renaming)
    renamed["Action"] = renamed["Action"].str.upper()
    renamed["Volume"] *= -1
    renamed["OUTCURRENCY"]
    return renamed


def convert_ledgers0(data):
    """What is the "amount" of the second row of each trade?"""
    parsed = []
    converted_row = pd.DataFrame(columns=all_columns)
    renaming = {"type": "Action", "asset": "Symbol", "amount": "Volume", "fee": "Fee"}
    for _, trade in data.groupby('refid'):
        if len(trade) != 2:
            continue
        converted_row["Date"] = transform_date(trade.loc[:, 'time']).iloc[0]
        renamed = trade.rename(columns=renaming)
        coin_names = renamed["Symbol"].str[1:].str.replace('XBT', 'BTC')
        converted_row["Currency"] = coin_names.iloc[0]
        converted_row["Symbol"] = coin_names.iloc[1]
        converted_row["Action"] = 'SELL' if renamed.iloc[0, 6] < 0 else 'BUY'
        converted_row["Volume"] = np.abs(converted_row["Volume"])
        parsed.append(converted_row.copy())

    return pd.concat(parsed)


def convert_lqui0(data):
    data["Date"] = transform_date(data["Date"])
    renaming = {"Type": "Action", "Amount": "Volume"}
    renamed = data.rename(columns=renaming)
    renamed["Symbol"] = renamed["Market"].str.split("/", expand=True)[0]
    renamed["Currency"] = renamed["Market"].str.split("/", expand=True)[1]
    renamed["Fee"] = renamed["Fee"].str[:-1]
    return renamed


def convert_member0(data):
    data["Date"] = transform_date(data["DATE"])
    renaming = {"PRICE": "Price", "FEE": "Fee", "FEECURRENCY": "FeeCurrency"}
    renamed = data.rename(columns=renaming)
    renamed["Action"] = renamed["Action"].str.upper()
    return renamed


def convert_shapeshift0(data):
    """Rechisha mehira?"""
    data["Date"] = transform_date(data["תאריך"])
    renaming = {
        "כמות רכישה": "Volume",
        "מטבע רכישה": "Symbol",
        "מטבע מכירה": "Currency",
        "עמלה (אופציונלי)": "Fee",
        "מטבע עמלה (אופציונלי)": "FeeCurrency",
        "כמות מכירה": "Sold",
    }
    renamed = data.rename(columns=renaming)
    renamed["Volume"] = renamed["Volume"].where(renamed["Volume"] > 0, renamed["Sold"])
    renamed["Action"] = "BUY"
    renamed.loc[renamed["Sold"] > 0, "Action"] = "SELL"
    return renamed


def convert_trade0(data):
    """Market?"""
    data["Date"] = transform_date(data["Date"])
    renaming = {"Type": "Action", "Amount": "Volume"}
    renamed = data.rename(columns=renaming)
    renamed["Action"] = renamed["Action"].str.upper()
    renamed["Fee"] = renamed["Fee"].str[:-1]
    renamed["Symbol"] = renamed["Market"].str.split("/", expand=True)[0]
    renamed["Currency"] = renamed["Market"].str.split("/", expand=True)[1]
    return renamed


def convert_trades0(data):
    """Rebate? Total? Quantity?"""
    data["Date"] = transform_date(data["Date (UTC)"])
    renaming = {"Side": "Action", "Quantity": "Volume", "Volume": "TotalBeforeFee"}
    renamed = data.rename(columns=renaming)
    renamed["Symbol"] = renamed["Instrument"].str.split("/", expand=True)[0]
    renamed["Currency"] = renamed["Instrument"].str.split("/", expand=True)[1]
    renamed["Action"] = renamed["Action"].str.upper()
    renamed["Fee"] += renamed["Rebate"]
    return renamed


table_origin = {
    binance0: convert_binance0,
    bit2c0: convert_bit2c0,
    bit2c1: convert_bit2c1,
    bitfinex0: convert_bitfinex0,
    bittrex0: convert_bittrex0,
    cex0: convert_cex0,
    exodus0: convert_exodus0,
    ledgers0: convert_ledgers0,
    lqui0: convert_lqui0,
    member0: convert_member0,
    shapeshift0: convert_shapeshift0,
    trade0: convert_trade0,
    trades0: convert_trades0,
}
