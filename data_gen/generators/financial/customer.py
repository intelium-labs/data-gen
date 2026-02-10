"""Customer generator for financial domain."""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterator

from data_gen.generators.address import AddressFactory, CountryDistribution
from data_gen.generators.base import BaseGenerator
from data_gen.models.base import Address
from data_gen.models.financial import Customer
from data_gen.models.financial.enums import EmploymentStatus


class CustomerGenerator(BaseGenerator):
    """Generate synthetic customer data."""

    EMPLOYMENT_STATUS = list(EmploymentStatus)
    EMPLOYMENT_WEIGHTS = [0.60, 0.20, 0.12, 0.08]

    # Income ranges by employment status (monthly, BRL)
    INCOME_RANGES = {
        EmploymentStatus.EMPLOYED: (2000, 30000),
        EmploymentStatus.SELF_EMPLOYED: (3000, 50000),
        EmploymentStatus.RETIRED: (1500, 15000),
        EmploymentStatus.UNEMPLOYED: (0, 2000),
    }

    def __init__(
        self,
        seed: int | None = None,
        country_distribution: CountryDistribution | None = None,
    ) -> None:
        super().__init__(seed)
        self._address_factory = AddressFactory(
            distribution=country_distribution,
            seed=seed,
        )

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
        if employment == EmploymentStatus.EMPLOYED:
            base_score += 100
        elif employment == EmploymentStatus.SELF_EMPLOYED:
            base_score += 50
        elif employment == EmploymentStatus.RETIRED:
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
        """Generate an address using the configured country distribution."""
        return self._address_factory.generate()
