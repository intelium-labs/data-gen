"""Account generator for financial domain."""

import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterator

from data_gen.generators.base import BaseGenerator
from data_gen.generators.pool import FakerPool
from data_gen.models.financial import Account
from data_gen.models.financial.enums import AccountStatus, AccountType


class AccountGenerator(BaseGenerator):
    """Generate synthetic bank accounts.

    Brazilian account types:
    - CONTA_CORRENTE: Checking account (most common, ~70%)
    - POUPANCA: Savings account (~20%)
    - INVESTIMENTOS: Investment account (~10%)
    """

    ACCOUNT_TYPES = list(AccountType)
    ACCOUNT_TYPE_WEIGHTS = [0.70, 0.20, 0.10]

    BANK_CODES = {
        "001": "Banco do Brasil",
        "033": "Santander",
        "104": "Caixa Econômica Federal",
        "237": "Bradesco",
        "341": "Itaú",
        "260": "Nubank",
        "077": "Inter",
        "336": "C6 Bank",
    }

    def __init__(self, seed: int | None = None, pool: FakerPool | None = None) -> None:
        super().__init__(seed, pool=pool)

    def generate(self, customer_id: str) -> Account:
        """Generate a single account for a customer.

        Parameters
        ----------
        customer_id : str
            Customer ID to associate with the account.

        Returns
        -------
        Account
            Generated account.
        """
        account_type = random.choices(
            self.ACCOUNT_TYPES, weights=self.ACCOUNT_TYPE_WEIGHTS, k=1
        )[0]
        return self._generate_one(
            customer_id=customer_id,
            customer_created_at=datetime.now() - timedelta(days=random.randint(0, 365)),
            monthly_income=Decimal(str(random.randint(2000, 20000))),
            account_type=account_type,
        )

    def generate_for_customer(
        self,
        customer_id: str,
        customer_created_at: datetime,
        monthly_income: Decimal,
    ) -> Iterator[Account]:
        """Generate accounts for a specific customer."""
        # Most customers have 1-2 accounts
        num_accounts = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1], k=1)[0]

        for i in range(num_accounts):
            account_type = random.choices(
                self.ACCOUNT_TYPES, weights=self.ACCOUNT_TYPE_WEIGHTS, k=1
            )[0]

            # Don't generate duplicate types usually
            if i > 0 and account_type == AccountType.CONTA_CORRENTE:
                account_type = random.choice([AccountType.POUPANCA, AccountType.INVESTIMENTOS])

            yield self._generate_one(
                customer_id, customer_created_at, monthly_income, account_type
            )

    def _generate_one(
        self,
        customer_id: str,
        customer_created_at: datetime,
        monthly_income: Decimal,
        account_type: AccountType,
    ) -> Account:
        """Generate a single account."""
        bank_code = random.choice(list(self.BANK_CODES.keys()))

        # Balance based on income (0.5 to 3x monthly income typically)
        balance_multiplier = Decimal(str(round(random.uniform(0.1, 3.0), 4)))
        noise = Decimal(str(round(random.gauss(0, float(monthly_income) * 0.3), 2)))
        balance = monthly_income * balance_multiplier + noise
        balance = max(Decimal("0"), balance)

        # Account created same day or after customer
        days_after = random.randint(0, 30)
        created_at = customer_created_at + timedelta(days=days_after)
        if created_at > datetime.now():
            created_at = datetime.now()

        return Account(
            account_id=self.pool.uuid(),
            customer_id=customer_id,
            account_type=account_type,
            bank_code=bank_code,
            branch=f"{random.randint(1, 9999):04d}",
            account_number=f"{random.randint(1, 999999):06d}-{random.randint(0, 9)}",
            balance=balance.quantize(Decimal("0.01")),
            status=AccountStatus.ACTIVE,
            created_at=created_at,
        )
