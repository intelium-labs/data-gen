"""Loan generator for financial domain."""

import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Iterator

from data_gen.generators.address import AddressFactory, CountryDistribution
from data_gen.generators.base import BaseGenerator
from data_gen.models.financial import Customer, Installment, Loan, Property
from data_gen.models.financial.enums import (
    AmortizationSystem,
    InstallmentStatus,
    LoanStatus,
    LoanType,
    PropertyType,
)


class PropertyGenerator(BaseGenerator):
    """Generate synthetic properties for housing loans."""

    def __init__(self, seed: int | None = None) -> None:
        super().__init__(seed)
        self._address_factory = AddressFactory(
            distribution=CountryDistribution.brazil_only(),
            seed=seed,
        )

    def generate(self) -> Property:
        """Generate a property.

        Returns
        -------
        Property
            Generated property.
        """
        property_value = random.randint(150, 2000) * 1000

        return Property(
            property_id=self.fake.uuid4(),
            property_type=random.choice([PropertyType.APARTMENT, PropertyType.HOUSE]),
            address=self._address_factory.generate_brazilian(),
            appraised_value=Decimal(str(property_value)),
            area_sqm=random.uniform(40, 200),
            registration_number=f"{random.randint(10000, 99999)}.{random.randint(1, 999):03d}",
        )


class LoanGenerator(BaseGenerator):
    """Generate synthetic loans and installments."""

    LOAN_TYPES = list(LoanType)

    # Interest rates by loan type and credit score range (monthly)
    INTEREST_RATES = {
        LoanType.PERSONAL: {
            "excellent": (0.015, 0.025),  # 1.5-2.5%
            "good": (0.025, 0.04),  # 2.5-4%
            "fair": (0.04, 0.06),  # 4-6%
            "poor": (0.06, 0.08),  # 6-8%
        },
        LoanType.HOUSING: {
            "excellent": (0.007, 0.009),  # 0.7-0.9%
            "good": (0.009, 0.011),  # 0.9-1.1%
            "fair": (0.011, 0.012),  # 1.1-1.2%
            "poor": None,  # Usually rejected
        },
        LoanType.VEHICLE: {
            "excellent": (0.012, 0.018),
            "good": (0.018, 0.025),
            "fair": (0.025, 0.035),
            "poor": (0.035, 0.05),
        },
    }

    def __init__(self, seed: int | None = None) -> None:
        super().__init__(seed)
        self._address_factory = AddressFactory(
            distribution=CountryDistribution.brazil_only(),
            seed=seed,
        )

    def generate_with_installments(
        self,
        customer_id: str,
        loan_type: LoanType = LoanType.PERSONAL,
        property_id: str | None = None,
    ) -> tuple[Loan, list[Installment]]:
        """Generate a loan with installments.

        Parameters
        ----------
        customer_id : str
            Customer ID for the loan.
        loan_type : LoanType
            Loan type (PERSONAL, HOUSING, VEHICLE).
        property_id : str | None
            Property ID for housing loans.

        Returns
        -------
        tuple[Loan, list[Installment]]
            Generated loan and its installments.
        """
        # Generate loan parameters
        if loan_type == LoanType.PERSONAL:
            principal = Decimal(str(random.randint(5, 100) * 1000))
            term_months = random.choice([6, 12, 18, 24, 36, 48, 60])
            interest_rate = Decimal(str(round(random.uniform(0.02, 0.06), 4)))
            amortization = AmortizationSystem.PRICE
        elif loan_type == LoanType.HOUSING:
            principal = Decimal(str(random.randint(100, 1500) * 1000))
            term_months = random.choice([120, 180, 240, 300, 360])
            interest_rate = Decimal(str(round(random.uniform(0.007, 0.012), 4)))
            amortization = random.choice(list(AmortizationSystem))
        else:  # VEHICLE
            principal = Decimal(str(random.randint(20, 150) * 1000))
            term_months = random.choice([12, 24, 36, 48, 60])
            interest_rate = Decimal(str(round(random.uniform(0.015, 0.035), 4)))
            amortization = AmortizationSystem.PRICE

        disbursement_date = (datetime.now() - timedelta(days=random.randint(30, 365))).date()

        loan = Loan(
            loan_id=self.fake.uuid4(),
            customer_id=customer_id,
            loan_type=loan_type,
            principal=principal,
            interest_rate=interest_rate,
            term_months=term_months,
            amortization_system=amortization,
            status=LoanStatus.ACTIVE,
            disbursement_date=disbursement_date,
            property_id=property_id,
            created_at=datetime.now() - timedelta(days=random.randint(30, 365)),
        )

        installments = list(self._generate_installments(loan))
        return loan, installments

    def generate_for_customer(
        self,
        customer: Customer,
        loan_type: LoanType = LoanType.PERSONAL,
        approval_date: datetime | None = None,
    ) -> tuple[Loan, Property | None, list[Installment]] | None:
        """Generate a loan for a customer if approved."""
        score_tier = self._get_score_tier(customer.credit_score)

        # Check if loan type is available for this score tier
        rates = self.INTEREST_RATES[loan_type].get(score_tier)
        if rates is None:
            return None  # Rejected

        # Approval probability based on score
        approval_prob = {
            "excellent": 0.95,
            "good": 0.80,
            "fair": 0.50,
            "poor": 0.20,
        }[score_tier]

        if random.random() > approval_prob:
            return None  # Rejected

        # Generate loan parameters
        interest_rate = Decimal(str(round(random.uniform(*rates), 4)))

        if loan_type == LoanType.PERSONAL:
            principal = self._generate_personal_loan_amount(customer.monthly_income)
            term_months = random.choice([6, 12, 18, 24, 36, 48, 60, 72])
            amortization = AmortizationSystem.PRICE
            prop = None
        elif loan_type == LoanType.HOUSING:
            principal, prop = self._generate_housing_loan(customer)
            term_months = random.choice([120, 180, 240, 300, 360])
            amortization = random.choice(list(AmortizationSystem))
        else:  # VEHICLE
            principal = self._generate_vehicle_loan_amount(customer.monthly_income)
            term_months = random.choice([12, 24, 36, 48, 60])
            amortization = AmortizationSystem.PRICE
            prop = None

        # Loan dates
        if approval_date is None:
            days_after_customer = random.randint(30, 365 * 2)
            approval_date = customer.created_at + timedelta(days=days_after_customer)
            if approval_date > datetime.now():
                approval_date = datetime.now() - timedelta(days=random.randint(30, 365))

        disbursement_date = (approval_date + timedelta(days=random.randint(1, 7))).date()

        loan = Loan(
            loan_id=self.fake.uuid4(),
            customer_id=customer.customer_id,
            loan_type=loan_type,
            principal=principal,
            interest_rate=interest_rate,
            term_months=term_months,
            amortization_system=amortization,
            status=LoanStatus.ACTIVE,
            disbursement_date=disbursement_date,
            property_id=prop.property_id if prop else None,
            created_at=approval_date,
        )

        # Generate installments
        installments = list(self._generate_installments(loan))

        return loan, prop, installments

    def _get_score_tier(self, score: int) -> str:
        """Get score tier from credit score."""
        if score >= 750:
            return "excellent"
        elif score >= 650:
            return "good"
        elif score >= 500:
            return "fair"
        else:
            return "poor"

    def _generate_personal_loan_amount(self, monthly_income: Decimal) -> Decimal:
        """Generate personal loan amount based on income."""
        income = float(monthly_income)
        # Typically 3-12x monthly income
        multiplier = random.uniform(3, 12)
        amount = income * multiplier
        # Round to nearest 100
        amount = round(amount / 100) * 100
        return Decimal(str(max(1000, min(amount, 100000))))

    def _generate_vehicle_loan_amount(self, monthly_income: Decimal) -> Decimal:
        """Generate vehicle loan amount."""
        income = float(monthly_income)
        # Typically 6-24x monthly income
        multiplier = random.uniform(6, 24)
        amount = income * multiplier
        amount = round(amount / 1000) * 1000
        return Decimal(str(max(10000, min(amount, 200000))))

    def _generate_housing_loan(self, customer: Customer) -> tuple[Decimal, Property]:
        """Generate housing loan with property."""
        income = float(customer.monthly_income)

        # Property value based on income (typically 50-150x monthly income)
        multiplier = random.uniform(50, 150)
        property_value = income * multiplier
        property_value = round(property_value / 10000) * 10000

        # Down payment 20-30%
        down_payment_pct = random.uniform(0.20, 0.30)
        loan_amount = property_value * (1 - down_payment_pct)
        loan_amount = round(loan_amount / 1000) * 1000

        # Create property
        prop = Property(
            property_id=self.fake.uuid4(),
            property_type=random.choice([PropertyType.APARTMENT, PropertyType.HOUSE]),
            address=self._address_factory.generate_brazilian(),
            appraised_value=Decimal(str(property_value)),
            area_sqm=random.uniform(40, 200),
            registration_number=f"{random.randint(10000, 99999)}.{random.randint(1, 999):03d}",
        )

        return Decimal(str(max(100000, min(loan_amount, 2000000)))), prop

    def _generate_installments(self, loan: Loan) -> Iterator[Installment]:
        """Generate all installments for a loan."""
        principal = float(loan.principal)
        rate = float(loan.interest_rate)
        n = loan.term_months

        if loan.amortization_system == AmortizationSystem.PRICE:
            # Fixed payment (PMT formula)
            if rate > 0:
                pmt = principal * (rate * (1 + rate) ** n) / ((1 + rate) ** n - 1)
            else:
                pmt = principal / n

            balance = principal
            for i in range(1, n + 1):
                interest = balance * rate
                principal_payment = pmt - interest
                balance -= principal_payment

                due_date = loan.disbursement_date + timedelta(days=30 * i)

                yield Installment(
                    installment_id=self.fake.uuid4(),
                    loan_id=loan.loan_id,
                    installment_number=i,
                    due_date=due_date,
                    principal_amount=Decimal(str(round(principal_payment, 2))),
                    interest_amount=Decimal(str(round(interest, 2))),
                    total_amount=Decimal(str(round(pmt, 2))),
                    paid_date=None,
                    paid_amount=None,
                    status=InstallmentStatus.PENDING,
                )
        else:  # SAC
            # Fixed principal, decreasing payments
            principal_payment = principal / n
            balance = principal

            for i in range(1, n + 1):
                interest = balance * rate
                total = principal_payment + interest
                balance -= principal_payment

                due_date = loan.disbursement_date + timedelta(days=30 * i)

                yield Installment(
                    installment_id=self.fake.uuid4(),
                    loan_id=loan.loan_id,
                    installment_number=i,
                    due_date=due_date,
                    principal_amount=Decimal(str(round(principal_payment, 2))),
                    interest_amount=Decimal(str(round(interest, 2))),
                    total_amount=Decimal(str(round(total, 2))),
                    paid_date=None,
                    paid_amount=None,
                    status=InstallmentStatus.PENDING,
                )
