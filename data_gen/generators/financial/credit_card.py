"""Credit card generator for financial domain."""

import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterator

from data_gen.generators.base import BaseGenerator
from data_gen.models.financial import CardTransaction, CreditCard, Customer
from data_gen.models.financial.enums import CardBrand, CardStatus, CardTransactionStatus


class CreditCardGenerator(BaseGenerator):
    """Generate synthetic credit cards and transactions."""

    BRANDS = list(CardBrand)
    BRAND_WEIGHTS = [0.45, 0.40, 0.15]

    # MCC codes and categories
    MCC_CATEGORIES = {
        "5411": ("Supermercados", (50, 800)),
        "5541": ("Postos de Combustível", (100, 400)),
        "5812": ("Restaurantes", (30, 300)),
        "5814": ("Fast Food", (20, 100)),
        "5912": ("Farmácias", (20, 500)),
        "5311": ("Lojas de Departamento", (50, 2000)),
        "5651": ("Vestuário", (50, 1000)),
        "5732": ("Eletrônicos", (100, 5000)),
        "5942": ("Livrarias", (30, 300)),
        "7832": ("Cinema", (20, 100)),
        "4121": ("Taxi/Uber", (15, 150)),
        "5999": ("Diversos", (20, 500)),
    }

    def __init__(self, seed: int | None = None) -> None:
        super().__init__(seed)

    def generate_for_customer(
        self,
        customer: Customer,
        issue_probability: float = 0.7,
    ) -> CreditCard | None:
        """Maybe generate a credit card for a customer based on credit score."""
        # Higher credit score = higher chance of getting a card
        score_factor = (customer.credit_score - 300) / 550  # 0 to 1
        adjusted_probability = issue_probability * score_factor

        if random.random() > adjusted_probability:
            return None

        # Credit limit based on income and score
        income = float(customer.monthly_income)
        score_multiplier = 1 + (customer.credit_score - 500) / 350  # 0.4 to 2
        base_limit = income * score_multiplier * random.uniform(1, 3)
        credit_limit = round(min(base_limit, 100000), -2)  # Round to nearest 100, max 100k

        brand = random.choices(self.BRANDS, weights=self.BRAND_WEIGHTS, k=1)[0]

        # Card created after customer
        days_after = random.randint(0, 365)
        created_at = customer.created_at + timedelta(days=days_after)
        if created_at > datetime.now():
            created_at = datetime.now()

        return CreditCard(
            card_id=self.fake.uuid4(),
            customer_id=customer.customer_id,
            card_number_masked=f"****-****-****-{random.randint(1000, 9999)}",
            brand=brand,
            credit_limit=Decimal(str(credit_limit)),
            available_limit=Decimal(str(credit_limit)),  # Starts with full limit
            due_day=random.randint(1, 28),
            status=CardStatus.ACTIVE,
            created_at=created_at,
        )

    def generate_transactions(
        self,
        card: CreditCard,
        start_date: datetime,
        end_date: datetime,
        avg_transactions_per_day: float = 1.5,
    ) -> Iterator[CardTransaction]:
        """Generate credit card transactions over a time period."""
        current_date = start_date

        while current_date < end_date:
            # Poisson-distributed number of transactions
            num_transactions = max(0, int(random.expovariate(1 / avg_transactions_per_day)))

            for _ in range(num_transactions):
                hour = random.randint(8, 22)
                minute = random.randint(0, 59)
                timestamp = current_date.replace(hour=hour, minute=minute)

                yield self._generate_transaction(card, timestamp)

            current_date += timedelta(days=1)

    def _generate_transaction(
        self,
        card: CreditCard,
        timestamp: datetime,
    ) -> CardTransaction:
        """Generate a single card transaction."""
        # Pick random MCC category
        mcc_code = random.choice(list(self.MCC_CATEGORIES.keys()))
        category_name, amount_range = self.MCC_CATEGORIES[mcc_code]

        # Generate amount within category range
        amount = random.uniform(*amount_range)
        amount = round(amount, 2)

        # Installments (parcelamento)
        if amount > 200:
            installments = random.choices(
                [1, 2, 3, 6, 10, 12],
                weights=[0.4, 0.15, 0.15, 0.15, 0.1, 0.05],
                k=1,
            )[0]
        else:
            installments = 1

        # 95% approved
        status = CardTransactionStatus.APPROVED if random.random() < 0.95 else CardTransactionStatus.DECLINED

        return CardTransaction(
            transaction_id=self.fake.uuid4(),
            card_id=card.card_id,
            merchant_name=self._generate_merchant_name(mcc_code),
            merchant_category=category_name,
            mcc_code=mcc_code,
            amount=Decimal(str(amount)),
            installments=installments,
            timestamp=timestamp,
            status=status,
            location_city=self.fake.city(),
            location_country="BR",
        )

    def generate(self, customer_id: str) -> CreditCard:
        """Generate a credit card for a customer.

        Parameters
        ----------
        customer_id : str
            Customer ID to associate with the card.

        Returns
        -------
        CreditCard
            Generated credit card.
        """
        brand = random.choices(self.BRANDS, weights=self.BRAND_WEIGHTS, k=1)[0]
        credit_limit = random.randint(5, 100) * 1000  # 5k to 100k

        return CreditCard(
            card_id=self.fake.uuid4(),
            customer_id=customer_id,
            card_number_masked=f"****-****-****-{random.randint(1000, 9999)}",
            brand=brand,
            credit_limit=Decimal(str(credit_limit)),
            available_limit=Decimal(str(credit_limit * random.uniform(0.3, 1.0))),
            due_day=random.randint(1, 28),
            status=CardStatus.ACTIVE,
            created_at=datetime.now() - timedelta(days=random.randint(0, 365)),
        )

    def _generate_merchant_name(self, mcc_code: str) -> str:
        """Generate a realistic merchant name for the MCC."""
        prefixes = {
            "5411": ["Supermercado", "Mercado", "Hipermercado"],
            "5541": ["Posto", "Auto Posto", "Rede"],
            "5812": ["Restaurante", "Cantina", "Bistrô"],
            "5814": ["Lanchonete", "Fast", "Express"],
            "5912": ["Farmácia", "Drogaria"],
            "5311": ["Lojas", "Magazine"],
            "5651": ["Moda", "Vestuário", "Roupas"],
            "5732": ["Tech", "Eletrônicos", "Digital"],
        }

        prefix_list = prefixes.get(mcc_code, ["Loja"])
        prefix = random.choice(prefix_list)

        return f"{prefix} {self.fake.last_name()}"


class CardTransactionGenerator(BaseGenerator):
    """Generate credit card transactions."""

    MCC_CATEGORIES = CreditCardGenerator.MCC_CATEGORIES

    def __init__(self, seed: int | None = None) -> None:
        super().__init__(seed)

    def generate(self, card_id: str) -> CardTransaction:
        """Generate a card transaction.

        Parameters
        ----------
        card_id : str
            Card ID to associate with the transaction.

        Returns
        -------
        CardTransaction
            Generated card transaction.
        """
        mcc_code = random.choice(list(self.MCC_CATEGORIES.keys()))
        category_name, amount_range = self.MCC_CATEGORIES[mcc_code]
        amount = round(random.uniform(*amount_range), 2)

        # Installments for larger purchases
        if amount > 200:
            installments = random.choices(
                [1, 2, 3, 6, 10, 12],
                weights=[0.4, 0.15, 0.15, 0.15, 0.1, 0.05],
                k=1,
            )[0]
        else:
            installments = 1

        status = CardTransactionStatus.APPROVED if random.random() < 0.95 else CardTransactionStatus.DECLINED

        return CardTransaction(
            transaction_id=self.fake.uuid4(),
            card_id=card_id,
            merchant_name=self._generate_merchant_name(mcc_code),
            merchant_category=category_name,
            mcc_code=mcc_code,
            amount=Decimal(str(amount)),
            installments=installments,
            timestamp=datetime.now() - timedelta(
                days=random.randint(0, 90),
                hours=random.randint(8, 22),
                minutes=random.randint(0, 59),
            ),
            status=status,
            location_city=self.fake.city(),
            location_country="BR",
        )

    def _generate_merchant_name(self, mcc_code: str) -> str:
        """Generate a realistic merchant name for the MCC."""
        prefixes = {
            "5411": ["Supermercado", "Mercado", "Hipermercado"],
            "5541": ["Posto", "Auto Posto", "Rede"],
            "5812": ["Restaurante", "Cantina", "Bistrô"],
            "5814": ["Lanchonete", "Fast", "Express"],
            "5912": ["Farmácia", "Drogaria"],
            "5311": ["Lojas", "Magazine"],
            "5651": ["Moda", "Vestuário", "Roupas"],
            "5732": ["Tech", "Eletrônicos", "Digital"],
        }

        prefix_list = prefixes.get(mcc_code, ["Loja"])
        prefix = random.choice(prefix_list)

        return f"{prefix} {self.fake.last_name()}"
