"""Transaction generator for financial domain."""

import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterator

from data_gen.generators.base import BaseGenerator
from data_gen.generators.pool import FakerPool
from data_gen.models.financial import Account, Transaction
from data_gen.models.financial.enums import Direction, PixKeyType, TransactionStatus, TransactionType
from data_gen.store.financial import FinancialDataStore


class TransactionGenerator(BaseGenerator):
    """Generate synthetic bank transactions."""

    TRANSACTION_TYPES = list(TransactionType)
    TRANSACTION_WEIGHTS = [0.50, 0.10, 0.05, 0.15, 0.10, 0.10]

    PIX_KEY_TYPES = list(PixKeyType)

    def __init__(self, seed: int | None = None, pool: FakerPool | None = None) -> None:
        super().__init__(seed, pool=pool)

    def generate(self, account_id: str, customer_id: str = "") -> Transaction:
        """Generate a single transaction for an account.

        Parameters
        ----------
        account_id : str
            Account ID to associate with the transaction.
        customer_id : str
            Customer ID that owns the account.

        Returns
        -------
        Transaction
            Generated transaction.
        """
        tx_type = random.choices(self.TRANSACTION_TYPES, weights=self.TRANSACTION_WEIGHTS, k=1)[0]

        # Determine direction based on type
        if tx_type in (TransactionType.WITHDRAW, TransactionType.BOLETO):
            direction = Direction.DEBIT
        elif tx_type == TransactionType.DEPOSIT:
            direction = Direction.CREDIT
        else:
            direction = random.choice(list(Direction))

        # Amount based on Pareto distribution
        amount = random.paretovariate(1.5) * 50
        amount = min(amount, 50000)
        amount = round(amount, 2)

        # Generate Pix-specific fields
        counterparty_key = None
        counterparty_name = None
        pix_e2e_id = None
        pix_key_type = None

        if tx_type == TransactionType.PIX:
            pix_key_type = random.choice(self.PIX_KEY_TYPES)
            counterparty_key = self._generate_pix_key(pix_key_type)
            counterparty_name = self.pool.name()
            pix_e2e_id = self._generate_e2e_id()
        elif tx_type in (TransactionType.TED, TransactionType.DOC):
            counterparty_key = f"{random.randint(1, 999):03d}/{random.randint(1, 9999):04d}/{random.randint(1, 999999):06d}-{random.randint(0, 9)}"
            counterparty_name = self.pool.name()

        description = self._generate_description(tx_type, counterparty_name)

        return Transaction(
            transaction_id=self.pool.uuid(),
            account_id=account_id,
            customer_id=customer_id,
            transaction_type=tx_type,
            amount=Decimal(str(amount)),
            direction=direction,
            counterparty_key=counterparty_key,
            counterparty_name=counterparty_name,
            description=description,
            timestamp=datetime.now() - timedelta(
                days=random.randint(0, 90),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            ),
            status=TransactionStatus.COMPLETED,
            pix_e2e_id=pix_e2e_id,
            pix_key_type=pix_key_type,
        )

    def generate_pix(self, account_id: str, customer_id: str = "") -> Transaction:
        """Generate a PIX transaction.

        Parameters
        ----------
        account_id : str
            Account ID to associate with the transaction.

        Returns
        -------
        Transaction
            Generated PIX transaction.
        """
        pix_key_type = random.choice(self.PIX_KEY_TYPES)
        counterparty_key = self._generate_pix_key(pix_key_type)
        counterparty_name = self.pool.name()
        pix_e2e_id = self._generate_e2e_id()

        amount = random.paretovariate(1.5) * 50
        amount = min(amount, 50000)
        amount = round(amount, 2)

        return Transaction(
            transaction_id=self.pool.uuid(),
            account_id=account_id,
            customer_id=customer_id,
            transaction_type=TransactionType.PIX,
            amount=Decimal(str(amount)),
            direction=random.choice(list(Direction)),
            counterparty_key=counterparty_key,
            counterparty_name=counterparty_name,
            description=f"Pix para {counterparty_name}",
            timestamp=datetime.now() - timedelta(
                days=random.randint(0, 90),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            ),
            status=TransactionStatus.COMPLETED,
            pix_e2e_id=pix_e2e_id,
            pix_key_type=pix_key_type,
        )

    def generate_for_account(
        self,
        account: Account,
        store: FinancialDataStore,
        start_date: datetime,
        end_date: datetime,
        avg_transactions_per_day: float = 2.0,
    ) -> Iterator[Transaction]:
        """Generate transactions for an account over a time period."""
        current_date = start_date

        while current_date < end_date:
            # Poisson-distributed number of transactions per day
            num_transactions = max(0, int(random.expovariate(1 / avg_transactions_per_day)))

            for _ in range(num_transactions):
                # Random time during business hours (weighted)
                hour = self._weighted_hour()
                minute = random.randint(0, 59)
                second = random.randint(0, 59)

                timestamp = current_date.replace(hour=hour, minute=minute, second=second)

                yield self._generate_one(account, store, timestamp)

            current_date += timedelta(days=1)

    def _generate_one(
        self,
        account: Account,
        store: FinancialDataStore,
        timestamp: datetime,
    ) -> Transaction:
        """Generate a single transaction."""
        tx_type = random.choices(self.TRANSACTION_TYPES, weights=self.TRANSACTION_WEIGHTS, k=1)[0]

        # Determine direction based on type
        if tx_type in (TransactionType.WITHDRAW, TransactionType.BOLETO):
            direction = Direction.DEBIT
        elif tx_type == TransactionType.DEPOSIT:
            direction = Direction.CREDIT
        else:
            direction = random.choice(list(Direction))

        # Amount based on Pareto distribution (many small, few large)
        amount = random.paretovariate(1.5) * 50
        amount = min(amount, 50000)  # Cap at 50k
        amount = round(amount, 2)

        # Generate counterparty
        counterparty_key = None
        counterparty_name = None
        pix_e2e_id = None
        pix_key_type = None

        if tx_type == TransactionType.PIX:
            pix_key_type = random.choice(self.PIX_KEY_TYPES)
            counterparty_key = self._generate_pix_key(pix_key_type)
            counterparty_name = self.pool.name()
            pix_e2e_id = self._generate_e2e_id()
        elif tx_type in (TransactionType.TED, TransactionType.DOC):
            # Use another account from store if available
            other_account = store.get_random_account()
            if other_account and other_account.account_id != account.account_id:
                counterparty_key = f"{other_account.bank_code}/{other_account.branch}/{other_account.account_number}"
            else:
                counterparty_key = f"{random.randint(1, 999):03d}/{random.randint(1, 9999):04d}/{random.randint(1, 999999):06d}-{random.randint(0, 9)}"
            counterparty_name = self.pool.name()

        description = self._generate_description(tx_type, counterparty_name)

        return Transaction(
            transaction_id=self.pool.uuid(),
            account_id=account.account_id,
            customer_id=account.customer_id,
            transaction_type=tx_type,
            amount=Decimal(str(amount)),
            direction=direction,
            counterparty_key=counterparty_key,
            counterparty_name=counterparty_name,
            description=description,
            timestamp=timestamp,
            status=TransactionStatus.COMPLETED,
            pix_e2e_id=pix_e2e_id,
            pix_key_type=pix_key_type,
        )

    def _weighted_hour(self) -> int:
        """Generate hour weighted toward business hours."""
        if random.random() < 0.7:
            # 70% during business hours (8-18)
            return random.randint(8, 18)
        else:
            # 30% other hours
            return random.choice(list(range(0, 8)) + list(range(19, 24)))

    def _generate_pix_key(self, key_type: PixKeyType) -> str:
        """Generate a Pix key based on type."""
        if key_type == PixKeyType.CPF:
            return self.pool.cpf_raw()
        elif key_type == PixKeyType.CNPJ:
            return self.pool.cnpj_raw()
        elif key_type == PixKeyType.EMAIL:
            return self.pool.email()
        elif key_type == PixKeyType.PHONE:
            msisdn = self.pool.msisdn()
            return "+55" + msisdn[2:]
        else:  # EVP
            return self.pool.uuid()

    def _generate_e2e_id(self) -> str:
        """Generate Pix E2E ID."""
        import string

        ispb = "".join(str(random.randint(0, 9)) for _ in range(8))
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        seq = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
        return f"E{ispb}{timestamp}{seq}"

    def _generate_description(self, tx_type: TransactionType, counterparty_name: str | None) -> str:
        """Generate transaction description."""
        if tx_type == TransactionType.PIX:
            return f"Pix para {counterparty_name}" if counterparty_name else "Pix"
        elif tx_type == TransactionType.TED:
            return f"TED para {counterparty_name}" if counterparty_name else "TED"
        elif tx_type == TransactionType.DOC:
            return f"DOC para {counterparty_name}" if counterparty_name else "DOC"
        elif tx_type == TransactionType.WITHDRAW:
            return "Saque ATM"
        elif tx_type == TransactionType.DEPOSIT:
            return "Depósito"
        elif tx_type == TransactionType.BOLETO:
            return f"Pagamento boleto - {self.pool.company()}"
        return ""
