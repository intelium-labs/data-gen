"""Transaction generator for financial domain."""

import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterator

from faker import Faker

from data_gen.models.financial import Account, Transaction
from data_gen.store.financial import FinancialDataStore


class TransactionGenerator:
    """Generate synthetic bank transactions."""

    TRANSACTION_TYPES = ["PIX", "TED", "DOC", "WITHDRAW", "DEPOSIT", "BOLETO"]
    TRANSACTION_WEIGHTS = [0.50, 0.10, 0.05, 0.15, 0.10, 0.10]

    PIX_KEY_TYPES = ["CPF", "CNPJ", "EMAIL", "PHONE", "EVP"]

    def __init__(self, seed: int | None = None) -> None:
        self.fake = Faker("pt_BR")
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

    def generate(self, account_id: str) -> Transaction:
        """Generate a single transaction for an account.

        Parameters
        ----------
        account_id : str
            Account ID to associate with the transaction.

        Returns
        -------
        Transaction
            Generated transaction.
        """
        tx_type = random.choices(self.TRANSACTION_TYPES, weights=self.TRANSACTION_WEIGHTS, k=1)[0]

        # Determine direction based on type
        if tx_type in ["WITHDRAW", "BOLETO"]:
            direction = "DEBIT"
        elif tx_type == "DEPOSIT":
            direction = "CREDIT"
        else:
            direction = random.choice(["CREDIT", "DEBIT"])

        # Amount based on Pareto distribution
        amount = random.paretovariate(1.5) * 50
        amount = min(amount, 50000)
        amount = round(amount, 2)

        # Generate Pix-specific fields
        counterparty_key = None
        counterparty_name = None
        pix_e2e_id = None
        pix_key_type = None

        if tx_type == "PIX":
            pix_key_type = random.choice(self.PIX_KEY_TYPES)
            counterparty_key = self._generate_pix_key(pix_key_type)
            counterparty_name = self.fake.name()
            pix_e2e_id = self._generate_e2e_id()
        elif tx_type in ["TED", "DOC"]:
            counterparty_key = f"{random.randint(1, 999):03d}/{random.randint(1, 9999):04d}/{random.randint(1, 999999):06d}-{random.randint(0, 9)}"
            counterparty_name = self.fake.name()

        description = self._generate_description(tx_type, counterparty_name)

        return Transaction(
            transaction_id=self.fake.uuid4(),
            account_id=account_id,
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
            status="COMPLETED",
            pix_e2e_id=pix_e2e_id,
            pix_key_type=pix_key_type,
        )

    def generate_pix(self, account_id: str) -> Transaction:
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
        counterparty_name = self.fake.name()
        pix_e2e_id = self._generate_e2e_id()

        amount = random.paretovariate(1.5) * 50
        amount = min(amount, 50000)
        amount = round(amount, 2)

        return Transaction(
            transaction_id=self.fake.uuid4(),
            account_id=account_id,
            transaction_type="PIX",
            amount=Decimal(str(amount)),
            direction=random.choice(["CREDIT", "DEBIT"]),
            counterparty_key=counterparty_key,
            counterparty_name=counterparty_name,
            description=f"Pix para {counterparty_name}",
            timestamp=datetime.now() - timedelta(
                days=random.randint(0, 90),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            ),
            status="COMPLETED",
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
        if tx_type in ["WITHDRAW", "BOLETO"]:
            direction = "DEBIT"
        elif tx_type == "DEPOSIT":
            direction = "CREDIT"
        else:
            direction = random.choice(["CREDIT", "DEBIT"])

        # Amount based on Pareto distribution (many small, few large)
        amount = random.paretovariate(1.5) * 50
        amount = min(amount, 50000)  # Cap at 50k
        amount = round(amount, 2)

        # Generate counterparty
        counterparty_key = None
        counterparty_name = None
        pix_e2e_id = None
        pix_key_type = None

        if tx_type == "PIX":
            pix_key_type = random.choice(self.PIX_KEY_TYPES)
            counterparty_key = self._generate_pix_key(pix_key_type)
            counterparty_name = self.fake.name()
            pix_e2e_id = self._generate_e2e_id()
        elif tx_type in ["TED", "DOC"]:
            # Use another account from store if available
            other_account = store.get_random_account()
            if other_account and other_account.account_id != account.account_id:
                counterparty_key = f"{other_account.bank_code}/{other_account.branch}/{other_account.account_number}"
            else:
                counterparty_key = f"{random.randint(1, 999):03d}/{random.randint(1, 9999):04d}/{random.randint(1, 999999):06d}-{random.randint(0, 9)}"
            counterparty_name = self.fake.name()

        description = self._generate_description(tx_type, counterparty_name)

        return Transaction(
            transaction_id=self.fake.uuid4(),
            account_id=account.account_id,
            transaction_type=tx_type,
            amount=Decimal(str(amount)),
            direction=direction,
            counterparty_key=counterparty_key,
            counterparty_name=counterparty_name,
            description=description,
            timestamp=timestamp,
            status="COMPLETED",
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

    def _generate_pix_key(self, key_type: str) -> str:
        """Generate a Pix key based on type."""
        if key_type == "CPF":
            return self.fake.cpf().replace(".", "").replace("-", "")
        elif key_type == "CNPJ":
            return self.fake.cnpj().replace(".", "").replace("/", "").replace("-", "")
        elif key_type == "EMAIL":
            return self.fake.email()
        elif key_type == "PHONE":
            return "+55" + self.fake.msisdn()[2:]
        else:  # EVP
            return self.fake.uuid4()

    def _generate_e2e_id(self) -> str:
        """Generate Pix E2E ID."""
        import string

        ispb = "".join(str(random.randint(0, 9)) for _ in range(8))
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        seq = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
        return f"E{ispb}{timestamp}{seq}"

    def _generate_description(self, tx_type: str, counterparty_name: str | None) -> str:
        """Generate transaction description."""
        if tx_type == "PIX":
            return f"Pix para {counterparty_name}" if counterparty_name else "Pix"
        elif tx_type == "TED":
            return f"TED para {counterparty_name}" if counterparty_name else "TED"
        elif tx_type == "DOC":
            return f"DOC para {counterparty_name}" if counterparty_name else "DOC"
        elif tx_type == "WITHDRAW":
            return "Saque ATM"
        elif tx_type == "DEPOSIT":
            return "Depósito"
        elif tx_type == "BOLETO":
            return f"Pagamento boleto - {self.fake.company()}"
        return ""
