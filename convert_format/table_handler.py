"""
This module contains methods that convert each table type
to the correct format.
"""
import re
import datetime
from itertools import combinations
import json

import pandas as pd
import numpy as np
import requests


mandatory_columns = ["Date", "Action", "Symbol", "Volume", "Currency"]
all_columns = mandatory_columns + ["Account", "Total", "Price", "Fee", "FeeCurrency"]

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S %z"

epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

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

bit2c2 = (
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
    "balance1",
    "balance2",
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

trade1 = (
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
    "Fee Currency",
    "Fee Total",
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

idex0 = (
    'a',
)

def identify_table_origin(columns):
    return table_origin[tuple(columns.to_list())]


def transform_date(col: pd.Series):
    """Transforms a Series containing datetime data to the
    acceptable format.
    """
    return pd.to_datetime(col, utc=True).dt.strftime(DATETIME_FORMAT)


def convert_idex0(data):
    pass

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
    renamed["Account"] = "Bitfinex"
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
    """Converts the CEX Trades files to the supported format.
    The idea is to separate the dataframe to two main groups -
    selling actions and buying actions. Inside these two groups
    we'll again separate the tables into the row which declares
    order ("Sell Order XXX" for example) and the actual
    actions or instructions.

    Once this division was made we'll parse the two groups -
    selling and buying orders - separately, and construct a new
    dataframe containing both, with one line per transaction,
    either sell or buy.

    Finally we'll also add the fee of a transaction based on the
    fee row which is sometimes present.
    """
    data["Date"] = transform_date(data["DateUTC"])
    buy_orders = data["Comment"].str.contains("Buy Order")
    buy_orders = data.loc[buy_orders, :]
    sell_orders = data["Comment"].str.contains("Sell Order")
    sell_orders = data.loc[sell_orders, :]
    buy_actions = data["Comment"].str.contains("Bought ")
    buy_actions = data.loc[buy_actions, :]
    sell_actions = data["Comment"].str.contains("Sold ")
    sell_actions = data.loc[sell_actions, :]
    fees = data.loc[data["Type"].str.contains("costsNothing"), :]
    assert len(buy_orders) + len(sell_orders) + len(buy_actions) + len(
        sell_actions
    ) + len(fees) == len(data)
    fees = _turn_fees_to_dict(fees)
    bought_transactions = _process_cex_order_action_pair(
        buy_orders, buy_actions, "Bought", fees
    )
    sold_transactions = _process_cex_order_action_pair(
        sell_orders, sell_actions, "Sold", fees
    )
    return pd.concat([bought_transactions, sold_transactions], ignore_index=True)


def _process_cex_order_action_pair(orders, actions, name, fees):
    """Helper function for CEX processing which finds the groups
    of selling or buying actions.

    This functions iterates over orders (rows with "Buy\Sell"
    Order #) and finds their corresponding "action" rows, i.e.
    rows that describe the actual transactions that were carried
    as a result of that order.

    It does so by try to sum up all actions that - if summed -
    result in an amount similar to the one describe in the parent
    order.
    """
    dtypes = {
        "Date": "datetime64[ns]",
        "Action": object,
        "Symbol": object,
        "Volume": np.float64,
        "Currency": object,
        "Account": object,
        "Price": np.float64,
        "Fee": np.float64,
        "FeeCurrency": object,
    }
    df = pd.DataFrame(
        [],
        columns=[
            "Date",
            "Action",
            "Symbol",
            "Volume",
            "Currency",
            "Account",
            "Price",
            "Fee",
            "FeeCurrency",
        ],
    ).astype(dtypes)

    number_regex = re.compile(" #(\d+)")
    new_df = []
    row_index = 0
    actions = _create_processed_actions_df(actions.copy(), name)
    while len(orders) > 0:
        current_order = orders.head(1)
        orders = orders.drop(current_order.index)
        order_amount = np.abs(current_order["Amount"].iloc[0])
        order_number = number_regex.findall(current_order["Comment"].iloc[0])[0]
        order_volume_in_coins = (
            abs(order_amount) if current_order["Symbol"].iloc[0] != "USD" else 0
        )
        later_actions = actions.loc[actions["Date"] >= current_order["Date"].iloc[0], :]
        average_price_of_actions = 0
        identical_action_to_order = later_actions.loc[
            np.isclose(later_actions.loc[:, "expected"], [order_amount], 0.01, 0.001)
        ]
        if len(identical_action_to_order) == 1:
            actions_of_order = identical_action_to_order.index
            average_price_of_actions = identical_action_to_order.loc[
                actions_of_order[0], "currency_price"
            ]
            symbol = identical_action_to_order.loc[actions_of_order[0], "parsed_symbol"]
            currency = identical_action_to_order.loc[
                actions_of_order[0], "parsed_currency"
            ]
            order_volume_in_coins += identical_action_to_order.loc[
                actions_of_order[0], "order_volume_in_coins"
            ]
        elif len(identical_action_to_order) >= 2:
            raise AssertionError(
                f"Two or more transactions found that could be assigned to order {current_order}. Exiting."
            )
        # Case of no one-to-one correspondence between order and action
        else:
            pair_found = False
            for number_of_items_per_group in range(2, 20):
                if pair_found:
                    break
                combs = combinations(later_actions.index, number_of_items_per_group)
                for comb in combs:
                    current_pair = later_actions.loc[comb, :]
                    if np.isclose(
                        [current_pair.loc[:, "expected"].sum()],
                        [order_amount],
                        0.01,
                        0.01,
                    ).all():
                        pair_found = True
                        break
            else:
                raise AssertionError(
                    f"More than 20 lines were needed to find all transactions of order {current_order}. Exiting."
                )
            # We're found a pair (or a triplet) of transactions that together make up one
            # order. We'll now parse its parameters.

            actions_of_order = current_pair.index
            average_price_of_actions = current_pair.loc[:, "currency_price"].mean()
            order_volume_in_coins += current_pair.loc[:, "order_volume_in_coins"].sum()
            symbol = current_pair.loc[actions_of_order[0], "parsed_symbol"]
            currency = current_pair.loc[actions_of_order[0], "parsed_currency"]
        df_of_order = {}
        df_of_order["Date"] = current_order["Date"].iloc[0]
        df_of_order["Action"] = current_order["Type"].iloc[0].upper()
        df_of_order["Symbol"] = symbol
        df_of_order["Volume"] = order_volume_in_coins
        df_of_order["Currency"] = currency
        df_of_order["Account"] = "CEX"
        df_of_order["Price"] = average_price_of_actions / len(actions_of_order)
        try:
            df_of_order["Fee"] = fees[order_number][0]
        except KeyError:
            df_of_order["Fee"] = 0
            df_of_order["FeeCurrency"] = ""
        else:
            df_of_order["FeeCurrency"] = fees[order_number][1]
        df = pd.DataFrame(df_of_order, index=[row_index]).astype(dtypes)
        new_df.append(df)
        actions = actions.drop(actions_of_order, axis=0)
        row_index += 1
    return pd.concat(new_df, ignore_index=True)


def _turn_fees_to_dict(fees):
    """Turns the fees dataframe into a dictionary with the key being the
    order number and the values being a 2-tuple of (amount, currency)"""
    fees_dict = {}
    number_regex = re.compile(" #(\d+)")
    for index, row in fees.iterrows():
        order_number = number_regex.findall(row["Comment"])[0]
        amount = row["Amount"]
        currency = row["Symbol"]
        fees_dict[order_number] = (amount, currency)
    return fees_dict


def _create_processed_actions_df(actions, name):
    """Generates a more robust and useful actions DataFrame,
    containing the needed information for each action in an easy-to-use format.
    It's useful since it will speed up its iteration and parsing later on.
    """
    symbol_price_regex = re.compile(f"{name} ([\d\.]+) [A-Z]+ at")
    currency_price_regex = re.compile(" at ([\d\.]+) [A-Z]+")
    currency_name_regex = re.compile(" at [\d\.]+ ([A-Z]+)")
    symbol_name_regex = re.compile(f"{name} [\d\.]+ ([A-Z]+) at")
    added_data = []
    new_columns = [
        ("expected", np.float64(0.0)),
        ("order_volume_in_coins", np.float64(0.0)),
        ("parsed_currency", ""),
        ("parsed_symbol", ""),
        ("currency_price", 0.0),
    ]
    for col in new_columns:
        actions.loc[:, col[0]] = col[1]

    for index, action in actions.iterrows():
        added_columns = {}
        currency_price = float(currency_price_regex.findall(action["Comment"])[0])
        symbol_amount = float(symbol_price_regex.findall(action["Comment"])[0])
        if action["Symbol"] == "USD":
            expected_amount = symbol_amount
        else:
            expected_amount = symbol_amount * currency_price
        added_columns["expected"] = expected_amount
        if action["Symbol"] != "USD":
            order_volume_in_coins = action["Amount"]
        else:
            order_volume_in_coins = 0
        added_columns["order_volume_in_coins"] = order_volume_in_coins
        currency = currency_name_regex.findall(action["Comment"])[0]
        symbol = symbol_name_regex.findall(action["Comment"])[0]
        added_columns["parsed_currency"] = currency
        added_columns["parsed_symbol"] = symbol
        added_columns["currency_price"] = currency_price
        added_data.append((pd.DataFrame(added_columns.copy(), index=[index])))

    for datum in added_data:
        actions.update(datum)
    return actions


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
    converted_row = pd.DataFrame([np.arange(len(all_columns))], columns=all_columns)
    for _, trade in data.groupby("refid"):
        if len(trade) != 2:
            continue
        if trade["type"].iloc[0] != 'trade':
            continue
        converted_row["Date"] = transform_date(trade.loc[:, "time"]).iloc[0]
        coin_names = trade.loc[:, "asset"].str[1:].replace("XBT", "BTC")
        converted_row["Currency"] = coin_names.iloc[1]
        converted_row["FeeCurrency"] = coin_names.iloc[1]
        converted_row["Symbol"] = coin_names.iloc[0]
        converted_row["Action"] = "SELL" if trade.iloc[0, 6] < 0 else "BUY"
        converted_row["Volume"] = np.abs(trade.loc[:, "amount"].iloc[0])
        converted_row["Total"] = np.abs(trade.loc[:, "amount"].iloc[1])
        converted_row["Fee"] = trade.loc[:, "fee"].iloc[1]
        converted_row["Price"] = converted_row["Total"] / converted_row["Volume"]
        converted_row["Account"] = "Kraken"
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
    """Uses and external API to calculate the Price"""
    data["Date"] = transform_date(data["תאריך"])
    renaming = {
        "כמות רכישה": "Volume",
        "מטבע רכישה": "Symbol",
        "מטבע מכירה": "Currency",
        "עמלה (אופציונלי)": "Fee",
        "מטבע עמלה (אופציונלי)": "FeeCurrency",
        "כמות מכירה": "Sold",
        "זירה": "Account"
    }
    renamed = data.rename(columns=renaming)
    renamed["Volume"] = renamed["Volume"].where(renamed["Volume"] > 0, renamed["Sold"])
    renamed["Action"] = "BUY"
    renamed["Fee"] = 0.01 * renamed["Volume"]
    renamed["FeeCurrency"] = renamed["Symbol"]
    prices = []
    for index, row in renamed.iterrows():
        symbol_rate = _get_coin_conversion_rate(row["Symbol"], row["Date"])
        currency_rate = _get_coin_conversion_rate(row["Currency"], row["Date"])
        price = symbol_rate / currency_rate
        prices.append(price)
    renamed["Price"] = prices
    return renamed


def _get_coin_conversion_rate(coin: str, date: datetime.datetime) -> float:
    """Returns the conversion rate of the given coin to USD,
    as found from Bitfinex's API in the given date.
    """
    bitfinex_url = 'https://api-pub.bitfinex.com/v2/trades/t{}USD/hist'
    coin = coin.upper()
    date = int(pd.to_datetime(date).timestamp() * 1000.0)
    params = {'limit': 1, 'start': date, 'end': date + 86400000, 'sort': 1}
    try:
        r = requests.get(bitfinex_url.format(coin), params=params)
        r.raise_for_status()
    except requests.HTTPError:
        raise
    else:
        return r.json()[0][3]


def convert_trade0(data):
    data["Date"] = transform_date(data["Date"])
    renaming = {"Type": "Action", "Amount": "Volume"}
    renamed = data.rename(columns=renaming)
    renamed["Action"] = renamed["Action"].str.upper()
    renamed["Fee"] = renamed["Fee"].str[:-1]
    renamed["Symbol"] = renamed["Market"].str.split("/", expand=True)[0]
    renamed["Currency"] = renamed["Market"].str.split("/", expand=True)[1]
    return renamed


def convert_trade1(data):
    renamed = convert_trade0(data)
    renamed["Fee"] = data["Fee Total"]
    renamed["FeeCurrency"] = data["Fee Currency"]
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
    bit2c2: convert_bit2c1,
    bitfinex0: convert_bitfinex0,
    bittrex0: convert_bittrex0,
    cex0: convert_cex0,
    exodus0: convert_exodus0,
    ledgers0: convert_ledgers0,
    lqui0: convert_lqui0,
    member0: convert_member0,
    shapeshift0: convert_shapeshift0,
    trade0: convert_trade0,
    trade1: convert_trade1,
    trades0: convert_trades0,
    idex0: convert_idex0,
}
