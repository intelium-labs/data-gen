"""Customer generator for financial domain."""

import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterator

from faker import Faker

from data_gen.models.base import Address
from data_gen.models.financial import Customer


class CustomerGenerator:
    """Generate synthetic customer data."""

    EMPLOYMENT_STATUS = ["EMPLOYED", "SELF_EMPLOYED", "RETIRED", "UNEMPLOYED"]
    EMPLOYMENT_WEIGHTS = [0.60, 0.20, 0.12, 0.08]

    # Income ranges by employment status (monthly, BRL)
    INCOME_RANGES = {
        "EMPLOYED": (2000, 30000),
        "SELF_EMPLOYED": (3000, 50000),
        "RETIRED": (1500, 15000),
        "UNEMPLOYED": (0, 2000),
    }

    def __init__(self, seed: int | None = None) -> None:
        self.fake = Faker("pt_BR")
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

    def generate(self) -> Customer:
        """Generate a single customer.

        Returns
        -------
        Customer
            Generated customer.
        """
        return self._generate_one()

    def generate_batch(self, count: int) -> Iterator[Customer]:
        """Generate multiple customers.

        Parameters
        ----------
        count : int
            Number of customers to generate.

        Yields
        ------
        Customer
            Generated customers.
        """
        for _ in range(count):
            yield self._generate_one()

    def _generate_one(self) -> Customer:
        """Generate a single customer."""
        employment = random.choices(
            self.EMPLOYMENT_STATUS, weights=self.EMPLOYMENT_WEIGHTS, k=1
        )[0]

        income_range = self.INCOME_RANGES[employment]
        # Log-normal distribution for income (more realistic)
        income = random.lognormvariate(
            mu=9.5,  # ~13,000 median
            sigma=0.7,
        )
        income = max(income_range[0], min(income, income_range[1]))

        # Credit score based on income and employment
        base_score = 500
        if employment == "EMPLOYED":
            base_score += 100
        elif employment == "SELF_EMPLOYED":
            base_score += 50
        elif employment == "RETIRED":
            base_score += 80

        # Higher income = better score (simplified)
        income_factor = min(100, int(income / 500))
        credit_score = min(850, max(300, base_score + income_factor + random.randint(-50, 50)))

        # Created date within last 5 years
        days_ago = random.randint(0, 5 * 365)
        created_at = datetime.now() - timedelta(days=days_ago)

        return Customer(
            customer_id=self.fake.uuid4(),
            cpf=self.fake.cpf(),
            name=self.fake.name(),
            email=self.fake.email(),
            phone=self.fake.cellphone_number(),
            address=self._generate_address(),
            monthly_income=Decimal(str(round(income, 2))),
            employment_status=employment,
            credit_score=credit_score,
            created_at=created_at,
        )

    def _generate_address(self) -> Address:
        """Generate a Brazilian address."""
        return Address(
            street=self.fake.street_name(),
            number=str(random.randint(1, 9999)),
            neighborhood=self.fake.bairro(),
            city=self.fake.city(),
            state=self.fake.estado_sigla(),
            postal_code=self.fake.postcode(),
            complement=random.choice(["", "", "", f"Apto {random.randint(1, 500)}"]),
        )
