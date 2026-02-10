"""Enumeration types for financial domain entities."""

from enum import Enum


class EmploymentStatus(str, Enum):
    EMPLOYED = "EMPLOYED"
    SELF_EMPLOYED = "SELF_EMPLOYED"
    RETIRED = "RETIRED"
    UNEMPLOYED = "UNEMPLOYED"


class AccountType(str, Enum):
    CONTA_CORRENTE = "CONTA_CORRENTE"
    POUPANCA = "POUPANCA"
    INVESTIMENTOS = "INVESTIMENTOS"


class AccountStatus(str, Enum):
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    CLOSED = "CLOSED"


class TransactionType(str, Enum):
    PIX = "PIX"
    TED = "TED"
    DOC = "DOC"
    WITHDRAW = "WITHDRAW"
    DEPOSIT = "DEPOSIT"
    BOLETO = "BOLETO"


class Direction(str, Enum):
    CREDIT = "CREDIT"
    DEBIT = "DEBIT"


class TransactionStatus(str, Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class CardBrand(str, Enum):
    VISA = "VISA"
    MASTERCARD = "MASTERCARD"
    ELO = "ELO"


class CardStatus(str, Enum):
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    CANCELLED = "CANCELLED"


class CardTransactionStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"


class LoanType(str, Enum):
    PERSONAL = "PERSONAL"
    HOUSING = "HOUSING"
    VEHICLE = "VEHICLE"


class AmortizationSystem(str, Enum):
    SAC = "SAC"
    PRICE = "PRICE"


class LoanStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    ACTIVE = "ACTIVE"
    PAID_OFF = "PAID_OFF"
    DEFAULT = "DEFAULT"
    DELINQUENT = "DELINQUENT"


class InstallmentStatus(str, Enum):
    PENDING = "PENDING"
    PAID = "PAID"
    LATE = "LATE"
    DEFAULT = "DEFAULT"


class PropertyType(str, Enum):
    APARTMENT = "APARTMENT"
    HOUSE = "HOUSE"
    LAND = "LAND"


class StockSector(str, Enum):
    ENERGY = "ENERGY"
    FINANCE = "FINANCE"
    MINING = "MINING"
    RETAIL = "RETAIL"
    TECHNOLOGY = "TECHNOLOGY"
    HEALTHCARE = "HEALTHCARE"
    UTILITIES = "UTILITIES"
    TELECOM = "TELECOM"
    FOOD_BEVERAGE = "FOOD_BEVERAGE"
    REAL_ESTATE = "REAL_ESTATE"
    TRANSPORTATION = "TRANSPORTATION"


class StockSegment(str, Enum):
    NOVO_MERCADO = "NOVO_MERCADO"
    N1 = "N1"
    N2 = "N2"
    TRADICIONAL = "TRADICIONAL"


class TradeType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"


class TradeStatus(str, Enum):
    PENDING = "PENDING"
    EXECUTED = "EXECUTED"
    CANCELLED = "CANCELLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"


class PixKeyType(str, Enum):
    CPF = "CPF"
    CNPJ = "CNPJ"
    EMAIL = "EMAIL"
    PHONE = "PHONE"
    EVP = "EVP"
