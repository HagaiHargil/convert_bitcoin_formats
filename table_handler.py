"""
This module contains methods that convert each table type
to the correct format.
"""

columns = ["Date", "Action", "Account", "Symbol", "Volume", "Price", "Currency", "Fee"]

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


def convert_binance0(data):
    pass


def convert_bit2c0(data):
    pass


def convert_bit2c1(data):
    pass


def convert_bitfinex0(data):
    pass


def convert_bittrex0(data):
    pass


def convert_cex0(data):
    pass


def convert_exodus0(data):
    pass


def convert_ledgers0(data):
    pass


def convert_lqui0(data):
    pass


def convert_member0(data):
    pass


def convert_shapeshift0(data):
    pass


def convert_trade0(data):
    pass


def convert_trades0(data):
    pass


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
