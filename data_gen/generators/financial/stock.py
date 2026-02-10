"""Stock and Trade generator for B3 (Brazilian Stock Exchange)."""

import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterator

from data_gen.generators.base import BaseGenerator
from data_gen.models.financial import Stock, Trade
from data_gen.models.financial.enums import OrderType, StockSector, StockSegment, TradeStatus, TradeType


class StockGenerator(BaseGenerator):
    """Generate synthetic B3 stocks.

    Generates realistic Brazilian stocks with proper tickers,
    sectors, and price ranges based on actual B3 listings.
    """

    # Real B3 stocks with realistic data
    B3_STOCKS = [
        # Energy & Oil
        {"ticker": "PETR4", "company": "Petrobras", "sector": "ENERGY", "price_range": (25, 40)},
        {"ticker": "PETR3", "company": "Petrobras ON", "sector": "ENERGY", "price_range": (28, 42)},
        {"ticker": "CSAN3", "company": "Cosan", "sector": "ENERGY", "price_range": (15, 25)},
        {"ticker": "PRIO3", "company": "PRIO", "sector": "ENERGY", "price_range": (40, 55)},
        {"ticker": "UGPA3", "company": "Ultrapar", "sector": "ENERGY", "price_range": (20, 35)},
        # Finance
        {"ticker": "ITUB4", "company": "Itaú Unibanco", "sector": "FINANCE", "price_range": (25, 35)},
        {"ticker": "BBDC4", "company": "Bradesco", "sector": "FINANCE", "price_range": (12, 18)},
        {"ticker": "BBAS3", "company": "Banco do Brasil", "sector": "FINANCE", "price_range": (45, 60)},
        {"ticker": "SANB11", "company": "Santander Brasil", "sector": "FINANCE", "price_range": (25, 35)},
        {"ticker": "B3SA3", "company": "B3 S.A.", "sector": "FINANCE", "price_range": (10, 15)},
        {"ticker": "BBSE3", "company": "BB Seguridade", "sector": "FINANCE", "price_range": (30, 40)},
        # Mining
        {"ticker": "VALE3", "company": "Vale", "sector": "MINING", "price_range": (60, 85)},
        {"ticker": "CMIN3", "company": "CSN Mineração", "sector": "MINING", "price_range": (4, 8)},
        {"ticker": "CSNA3", "company": "CSN", "sector": "MINING", "price_range": (10, 18)},
        # Retail
        {"ticker": "MGLU3", "company": "Magazine Luiza", "sector": "RETAIL", "price_range": (1, 5)},
        {"ticker": "LREN3", "company": "Lojas Renner", "sector": "RETAIL", "price_range": (12, 20)},
        {"ticker": "AMER3", "company": "Americanas", "sector": "RETAIL", "price_range": (0.5, 2)},
        {"ticker": "VIIA3", "company": "Via", "sector": "RETAIL", "price_range": (1, 4)},
        {"ticker": "PETZ3", "company": "Petz", "sector": "RETAIL", "price_range": (3, 8)},
        # Telecom
        {"ticker": "VIVT3", "company": "Telefônica Vivo", "sector": "TELECOM", "price_range": (45, 55)},
        {"ticker": "TIMS3", "company": "TIM", "sector": "TELECOM", "price_range": (15, 22)},
        # Food & Beverage
        {"ticker": "ABEV3", "company": "Ambev", "sector": "FOOD_BEVERAGE", "price_range": (10, 16)},
        {"ticker": "JBSS3", "company": "JBS", "sector": "FOOD_BEVERAGE", "price_range": (25, 40)},
        {"ticker": "BRFS3", "company": "BRF", "sector": "FOOD_BEVERAGE", "price_range": (15, 25)},
        {"ticker": "MDIA3", "company": "M. Dias Branco", "sector": "FOOD_BEVERAGE", "price_range": (25, 40)},
        # Utilities
        {"ticker": "ELET3", "company": "Eletrobras ON", "sector": "UTILITIES", "price_range": (35, 50)},
        {"ticker": "ELET6", "company": "Eletrobras PNB", "sector": "UTILITIES", "price_range": (38, 52)},
        {"ticker": "EGIE3", "company": "Engie Brasil", "sector": "UTILITIES", "price_range": (40, 50)},
        {"ticker": "SBSP3", "company": "Sabesp", "sector": "UTILITIES", "price_range": (70, 95)},
        {"ticker": "CMIG4", "company": "Cemig", "sector": "UTILITIES", "price_range": (10, 15)},
        # Construction & Real Estate
        {"ticker": "CYRE3", "company": "Cyrela", "sector": "REAL_ESTATE", "price_range": (18, 28)},
        {"ticker": "MRVE3", "company": "MRV", "sector": "REAL_ESTATE", "price_range": (6, 12)},
        {"ticker": "EZTC3", "company": "EZTEC", "sector": "REAL_ESTATE", "price_range": (12, 20)},
        # Healthcare
        {"ticker": "RDOR3", "company": "Rede D'Or", "sector": "HEALTHCARE", "price_range": (25, 35)},
        {"ticker": "HAPV3", "company": "Hapvida", "sector": "HEALTHCARE", "price_range": (3, 6)},
        {"ticker": "FLRY3", "company": "Fleury", "sector": "HEALTHCARE", "price_range": (13, 20)},
        # Technology
        {"ticker": "TOTS3", "company": "TOTVS", "sector": "TECHNOLOGY", "price_range": (28, 38)},
        {"ticker": "LWSA3", "company": "Locaweb", "sector": "TECHNOLOGY", "price_range": (4, 8)},
        # Transportation
        {"ticker": "RAIL3", "company": "Rumo", "sector": "TRANSPORTATION", "price_range": (20, 28)},
        {"ticker": "CCRO3", "company": "CCR", "sector": "TRANSPORTATION", "price_range": (11, 16)},
        {"ticker": "AZUL4", "company": "Azul", "sector": "TRANSPORTATION", "price_range": (10, 20)},
        {"ticker": "GOLL4", "company": "Gol", "sector": "TRANSPORTATION", "price_range": (5, 12)},
    ]

    SEGMENTS = list(StockSegment)
    SEGMENT_WEIGHTS = [0.50, 0.20, 0.15, 0.15]

    def __init__(self, seed: int | None = None) -> None:
        super().__init__(seed)

    def generate(self, stock_data: dict | None = None) -> Stock:
        """Generate a single B3 stock.

        Parameters
        ----------
        stock_data : dict | None
            Optional predefined stock data from B3_STOCKS.

        Returns
        -------
        Stock
            Generated stock.
        """
        if stock_data is None:
            stock_data = random.choice(self.B3_STOCKS)

        price_min, price_max = stock_data["price_range"]
        current_price = round(random.uniform(price_min, price_max), 2)

        segment = random.choices(self.SEGMENTS, weights=self.SEGMENT_WEIGHTS, k=1)[0]

        # Generate ISIN (Brazilian format: BRXXXXXXXXXXXX)
        isin = f"BR{stock_data['ticker'][:4].upper()}ACN{random.randint(100, 999)}"

        return Stock(
            stock_id=self.fake.uuid4(),
            ticker=stock_data["ticker"],
            company_name=stock_data["company"],
            sector=StockSector(stock_data["sector"]),
            segment=segment,
            current_price=Decimal(str(current_price)),
            currency="BRL",
            isin=isin,
            lot_size=100,  # Standard B3 lot
            created_at=datetime.now(),
        )

    def generate_all(self) -> Iterator[Stock]:
        """Generate all predefined B3 stocks.

        Yields
        ------
        Stock
            Each B3 stock from the predefined list.
        """
        for stock_data in self.B3_STOCKS:
            yield self.generate(stock_data)

    def generate_batch(self, count: int) -> list[Stock]:
        """Generate a batch of random stocks.

        Parameters
        ----------
        count : int
            Number of stocks to generate.

        Returns
        -------
        list[Stock]
            List of generated stocks.
        """
        stocks = []
        selected = random.sample(self.B3_STOCKS, min(count, len(self.B3_STOCKS)))
        for stock_data in selected:
            stocks.append(self.generate(stock_data))
        return stocks


class TradeGenerator(BaseGenerator):
    """Generate synthetic stock trades.

    Generates realistic trade data including fees, settlement dates,
    and order types based on B3 market patterns.
    """

    ORDER_TYPES = list(OrderType)
    ORDER_TYPE_WEIGHTS = [0.50, 0.40, 0.10]

    TRADE_TYPES = list(TradeType)

    # Fee structure (approximate B3 fees)
    BROKERAGE_FEE_PCT = Decimal("0.0005")  # 0.05% typical discount broker
    B3_EMOLUMENTS_PCT = Decimal("0.000275")  # 0.0275%
    B3_SETTLEMENT_PCT = Decimal("0.0000275")  # 0.00275%

    def __init__(self, seed: int | None = None) -> None:
        super().__init__(seed)

    def generate(
        self,
        account_id: str,
        stock: Stock,
        trade_type: TradeType | None = None,
        executed_at: datetime | None = None,
    ) -> Trade:
        """Generate a single trade.

        Parameters
        ----------
        account_id : str
            Investment account ID.
        stock : Stock
            Stock being traded.
        trade_type : str | None
            BUY or SELL (random if not specified).
        executed_at : datetime | None
            Execution timestamp (now if not specified).

        Returns
        -------
        Trade
            Generated trade.
        """
        if trade_type is None:
            trade_type = random.choice(self.TRADE_TYPES)

        if executed_at is None:
            # Random time within market hours (10:00 - 17:00)
            days_ago = random.randint(0, 90)
            base_date = datetime.now() - timedelta(days=days_ago)
            hour = random.randint(10, 16)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            executed_at = base_date.replace(hour=hour, minute=minute, second=second)

        # Quantity: 100 (1 lot) to 10000 shares
        quantity = random.choice([100, 200, 300, 500, 1000, 2000, 5000, 10000])

        # Price with small variation from current price
        price_variation = random.uniform(-0.02, 0.02)  # ±2%
        price = float(stock.current_price) * (1 + price_variation)
        price_per_share = Decimal(str(round(price, 2)))

        total_amount = price_per_share * quantity

        # Calculate fees
        brokerage = total_amount * self.BROKERAGE_FEE_PCT
        emoluments = total_amount * self.B3_EMOLUMENTS_PCT
        settlement = total_amount * self.B3_SETTLEMENT_PCT
        fees = (brokerage + emoluments + settlement).quantize(Decimal("0.01"))

        # Net amount (buy: pay more, sell: receive less)
        if trade_type == TradeType.BUY:
            net_amount = total_amount + fees
        else:
            net_amount = total_amount - fees

        order_type = random.choices(self.ORDER_TYPES, weights=self.ORDER_TYPE_WEIGHTS, k=1)[0]

        # Settlement T+2 (skip weekends)
        settlement_date = self._calculate_settlement_date(executed_at)

        return Trade(
            trade_id=self.fake.uuid4(),
            account_id=account_id,
            stock_id=stock.stock_id,
            ticker=stock.ticker,
            trade_type=trade_type,
            quantity=quantity,
            price_per_share=price_per_share,
            total_amount=total_amount.quantize(Decimal("0.01")),
            fees=fees,
            net_amount=net_amount.quantize(Decimal("0.01")),
            order_type=order_type,
            status=TradeStatus.EXECUTED,
            executed_at=executed_at,
            settlement_date=settlement_date,
        )

    def _calculate_settlement_date(self, executed_at: datetime) -> datetime:
        """Calculate T+2 settlement date, skipping weekends.

        Parameters
        ----------
        executed_at : datetime
            Trade execution datetime.

        Returns
        -------
        datetime
            Settlement datetime.
        """
        settlement = executed_at
        business_days = 0
        while business_days < 2:
            settlement += timedelta(days=1)
            # Skip weekends (Saturday=5, Sunday=6)
            if settlement.weekday() < 5:
                business_days += 1
        return settlement

    def generate_trades_for_account(
        self,
        account_id: str,
        stocks: list[Stock],
        num_trades: int = 10,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[Trade]:
        """Generate multiple trades for an investment account.

        Parameters
        ----------
        account_id : str
            Investment account ID.
        stocks : list[Stock]
            Available stocks to trade.
        num_trades : int
            Number of trades to generate.
        start_date : datetime | None
            Start of trading period.
        end_date : datetime | None
            End of trading period.

        Returns
        -------
        list[Trade]
            List of generated trades.
        """
        if not stocks:
            return []

        if start_date is None:
            start_date = datetime.now() - timedelta(days=90)
        if end_date is None:
            end_date = datetime.now()

        trades = []
        for _ in range(num_trades):
            stock = random.choice(stocks)

            # Random datetime within range
            delta = end_date - start_date
            random_seconds = random.randint(0, int(delta.total_seconds()))
            executed_at = start_date + timedelta(seconds=random_seconds)

            # Ensure market hours
            executed_at = executed_at.replace(
                hour=random.randint(10, 16),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )

            trade = self.generate(account_id, stock, executed_at=executed_at)
            trades.append(trade)

        # Sort by execution time
        trades.sort(key=lambda t: t.executed_at)
        return trades
