"""Stock model for B3 (Brazilian Stock Exchange)."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class Stock:
    """B3 Stock entity.

    Represents a stock listed on B3 (Brasil, Bolsa, Balcão).
    Brazilian stocks use ticker codes like PETR4, VALE3, ITUB4.
    """

    stock_id: str
    ticker: str  # B3 ticker (e.g., PETR4, VALE3, ITUB4)
    company_name: str
    sector: str  # e.g., ENERGY, FINANCE, MINING, RETAIL
    segment: str  # e.g., NOVO_MERCADO, N1, N2, TRADICIONAL
    current_price: Decimal
    currency: str  # BRL
    isin: str  # International Securities Identification Number
    lot_size: int  # Standard lot (usually 100)
    created_at: datetime


@dataclass
class Trade:
    """Stock trade entity.

    Represents a buy or sell order executed on B3.
    """

    trade_id: str
    account_id: str  # Investment account (INVESTIMENTOS)
    stock_id: str
    ticker: str  # Denormalized for convenience
    trade_type: str  # BUY, SELL
    quantity: int  # Number of shares
    price_per_share: Decimal
    total_amount: Decimal  # quantity * price_per_share
    fees: Decimal  # Brokerage + B3 fees + taxes
    net_amount: Decimal  # total_amount +/- fees
    order_type: str  # MARKET, LIMIT, STOP
    status: str  # PENDING, EXECUTED, CANCELLED, PARTIALLY_FILLED
    executed_at: datetime
    settlement_date: datetime  # T+2 in Brazil
