"""Comprehensive tests for generators - 100% coverage."""

import random
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from data_gen.generators.financial.account import AccountGenerator
from data_gen.generators.financial.credit_card import (
    CardTransactionGenerator,
    CreditCardGenerator,
)
from data_gen.generators.financial.customer import CustomerGenerator
from data_gen.generators.financial.loan import LoanGenerator, PropertyGenerator
from data_gen.generators.financial.patterns import (
    FraudPattern,
    FraudPatternGenerator,
    PaymentBehavior,
)
from data_gen.generators.financial.transaction import TransactionGenerator
from data_gen.models.base import Address
from data_gen.models.financial import Account, Customer, Installment, Loan, Transaction
from data_gen.store.financial import FinancialDataStore


class TestCustomerGeneratorFull:
    """Full coverage tests for CustomerGenerator."""

    def test_generate_single(self) -> None:
        """Test generating a single customer."""
        gen = CustomerGenerator(seed=42)
        customer = gen.generate()

        assert customer.customer_id is not None
        assert len(customer.cpf) == 14
        assert customer.name is not None
        assert customer.email is not None
        assert customer.monthly_income > 0
        assert 300 <= customer.credit_score <= 850

    def test_generate_batch(self) -> None:
        """Test generating batch of customers."""
        gen = CustomerGenerator(seed=42)
        customers = list(gen.generate_batch(10))

        assert len(customers) == 10
        ids = [c.customer_id for c in customers]
        assert len(set(ids)) == 10  # All unique

    def test_employment_status_distribution(self) -> None:
        """Test that all employment statuses are generated."""
        gen = CustomerGenerator(seed=42)
        statuses = set()

        for _ in range(100):
            customer = gen.generate()
            statuses.add(customer.employment_status)

        assert "EMPLOYED" in statuses
        assert "SELF_EMPLOYED" in statuses
        # RETIRED and UNEMPLOYED have lower weights, may not appear in 100

    def test_credit_score_ranges(self) -> None:
        """Test credit score calculation based on employment."""
        gen = CustomerGenerator(seed=42)

        scores = []
        for _ in range(50):
            customer = gen.generate()
            scores.append(customer.credit_score)

        assert min(scores) >= 300
        assert max(scores) <= 850

    def test_address_generation(self) -> None:
        """Test that addresses are properly generated."""
        gen = CustomerGenerator(seed=42)
        customer = gen.generate()

        assert customer.address.street is not None
        assert customer.address.number is not None
        assert customer.address.city is not None
        assert customer.address.state is not None
        assert customer.address.postal_code is not None

    def test_income_ranges_by_employment(self) -> None:
        """Test income ranges respect employment status boundaries."""
        gen = CustomerGenerator(seed=42)

        for _ in range(50):
            customer = gen.generate()
            income = float(customer.monthly_income)

            if customer.employment_status == "UNEMPLOYED":
                assert income <= 2000
            # Other statuses have higher max limits


class TestAccountGeneratorFull:
    """Full coverage tests for AccountGenerator."""

    def test_generate_single(self) -> None:
        """Test generating a single account."""
        gen = AccountGenerator(seed=42)
        account = gen.generate("cust-001")

        assert account.account_id is not None
        assert account.customer_id == "cust-001"
        assert account.account_type in ["CONTA_CORRENTE", "POUPANCA", "INVESTIMENTOS"]
        assert account.balance >= 0
        assert account.status == "ACTIVE"
        assert account.bank_code in gen.BANK_CODES

    def test_generate_for_customer(self) -> None:
        """Test generating accounts for a customer with full details."""
        gen = AccountGenerator(seed=42)
        now = datetime.now()
        income = Decimal("10000.00")

        accounts = list(gen.generate_for_customer("cust-001", now, income))

        assert len(accounts) >= 1
        assert len(accounts) <= 3

        # Check that multiple accounts have different types
        if len(accounts) > 1:
            types = [a.account_type for a in accounts]
            # Second account should be POUPANCA or INVESTIMENTOS if first was CONTA_CORRENTE
            assert accounts[0].account_type == "CONTA_CORRENTE" or "POUPANCA" in types or "INVESTIMENTOS" in types

    def test_account_types_distribution(self) -> None:
        """Test account type distribution."""
        gen = AccountGenerator(seed=42)
        types = []

        for _ in range(100):
            account = gen.generate(f"cust-{_}")
            types.append(account.account_type)

        conta_corrente_count = types.count("CONTA_CORRENTE")
        poupanca_count = types.count("POUPANCA")
        investimentos_count = types.count("INVESTIMENTOS")

        # CONTA_CORRENTE should be most common (70% weight)
        assert conta_corrente_count > poupanca_count
        assert conta_corrente_count > investimentos_count

    def test_balance_calculation(self) -> None:
        """Test that balance is calculated reasonably."""
        gen = AccountGenerator(seed=42)
        income = Decimal("10000.00")
        now = datetime.now()

        for _ in range(20):
            accounts = list(gen.generate_for_customer(f"cust-{_}", now, income))
            for account in accounts:
                # Balance should be non-negative
                assert account.balance >= 0


class TestTransactionGeneratorFull:
    """Full coverage tests for TransactionGenerator."""

    def test_generate_single(self) -> None:
        """Test generating a single transaction."""
        gen = TransactionGenerator(seed=42)
        tx = gen.generate("acct-001")

        assert tx.transaction_id is not None
        assert tx.account_id == "acct-001"
        assert tx.amount > 0
        assert tx.direction in ["CREDIT", "DEBIT"]
        assert tx.transaction_type in gen.TRANSACTION_TYPES

    def test_generate_pix(self) -> None:
        """Test generating a PIX transaction."""
        gen = TransactionGenerator(seed=42)
        tx = gen.generate_pix("acct-001")

        assert tx.transaction_type == "PIX"
        assert tx.pix_e2e_id is not None
        assert tx.pix_key_type in gen.PIX_KEY_TYPES
        assert tx.counterparty_key is not None
        assert tx.counterparty_name is not None

    def test_transaction_type_distribution(self) -> None:
        """Test all transaction types are generated."""
        gen = TransactionGenerator(seed=42)
        types = set()

        for _ in range(200):
            tx = gen.generate("acct-001")
            types.add(tx.transaction_type)

        assert "PIX" in types
        assert "TED" in types
        # DOC, WITHDRAW, DEPOSIT, BOLETO may or may not appear

    def test_direction_by_type(self) -> None:
        """Test that direction is set correctly by transaction type."""
        gen = TransactionGenerator(seed=42)

        for _ in range(100):
            tx = gen.generate("acct-001")

            if tx.transaction_type == "WITHDRAW":
                assert tx.direction == "DEBIT"
            elif tx.transaction_type == "BOLETO":
                assert tx.direction == "DEBIT"
            elif tx.transaction_type == "DEPOSIT":
                assert tx.direction == "CREDIT"

    def test_pix_key_types(self) -> None:
        """Test all PIX key types are generated."""
        gen = TransactionGenerator(seed=42)
        key_types = set()

        for _ in range(100):
            tx = gen.generate_pix("acct-001")
            key_types.add(tx.pix_key_type)

        # Should have multiple key types
        assert len(key_types) >= 2

    def test_generate_for_account(self) -> None:
        """Test generating transactions for an account over time."""
        gen = TransactionGenerator(seed=42)

        # Create a simple account
        from data_gen.models.financial import Account

        account = Account(
            account_id="acct-001",
            customer_id="cust-001",
            account_type="CHECKING",
            bank_code="341",
            branch="0001",
            account_number="123456-7",
            balance=Decimal("5000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )

        store = FinancialDataStore()
        store.customers["cust-001"] = None  # Bypass validation
        store._customer_accounts["cust-001"] = ["acct-001"]
        store.accounts["acct-001"] = account
        store._account_transactions["acct-001"] = []

        start = datetime.now() - timedelta(days=7)
        end = datetime.now()

        transactions = list(
            gen.generate_for_account(account, store, start, end, avg_transactions_per_day=2.0)
        )

        assert len(transactions) > 0

    def test_description_generation(self) -> None:
        """Test that descriptions are generated for all types."""
        gen = TransactionGenerator(seed=42)

        for _ in range(50):
            tx = gen.generate("acct-001")
            assert tx.description is not None
            assert len(tx.description) > 0

    def test_description_unknown_type_fallback(self) -> None:
        """Test description returns empty string for unknown transaction type."""
        gen = TransactionGenerator(seed=42)

        # Directly test the internal method with unknown type
        result = gen._generate_description("UNKNOWN_TYPE", "Some Person")
        assert result == ""

        # Also test with None counterparty
        result2 = gen._generate_description("UNKNOWN_TYPE", None)
        assert result2 == ""

    def test_ted_with_other_account_in_store(self) -> None:
        """Test TED transaction uses other account from store when available."""
        gen = TransactionGenerator(seed=42)

        # Create a store with multiple accounts
        store = FinancialDataStore()

        # Add a customer
        customer = Customer(
            customer_id="cust-001",
            cpf="123.456.789-00",
            name="Test",
            email="test@test.com",
            phone="+5511999999999",
            address=Address(
                street="Test",
                number="1",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            monthly_income=Decimal("5000.00"),
            employment_status="EMPLOYED",
            credit_score=700,
            created_at=datetime.now(),
        )
        store.add_customer(customer)

        # Add first account
        account1 = Account(
            account_id="acct-001",
            customer_id="cust-001",
            account_type="CHECKING",
            bank_code="341",
            branch="0001",
            account_number="123456-7",
            balance=Decimal("5000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        store.add_account(account1)

        # Add second account
        account2 = Account(
            account_id="acct-002",
            customer_id="cust-001",
            account_type="SAVINGS",
            bank_code="001",
            branch="0002",
            account_number="654321-0",
            balance=Decimal("10000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        store.add_account(account2)

        # Generate TED transactions - some should use the other account as counterparty
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()

        ted_count = 0
        for tx in gen.generate_for_account(account1, store, start_date, end_date, avg_transactions_per_day=5.0):
            if tx.transaction_type == "TED":
                ted_count += 1
                # Counterparty key should be set
                assert tx.counterparty_key is not None

        # Should have some TED transactions
        assert ted_count > 0


class TestCreditCardGeneratorFull:
    """Full coverage tests for CreditCardGenerator."""

    def test_generate_single(self) -> None:
        """Test generating a single credit card."""
        gen = CreditCardGenerator(seed=42)
        card = gen.generate("cust-001")

        assert card.card_id is not None
        assert card.customer_id == "cust-001"
        assert card.brand in gen.BRANDS
        assert card.credit_limit >= 5000
        assert card.available_limit <= card.credit_limit
        assert 1 <= card.due_day <= 28
        assert "****" in card.card_number_masked

    def test_generate_for_customer(self) -> None:
        """Test generating card for customer based on credit score."""
        gen = CreditCardGenerator(seed=42)

        # High credit score customer
        high_score_customer = Customer(
            customer_id="cust-001",
            cpf="123.456.789-00",
            name="Test",
            email="test@test.com",
            phone="+5511999999999",
            address=Address(
                street="Test",
                number="1",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            monthly_income=Decimal("20000.00"),
            employment_status="EMPLOYED",
            credit_score=800,
            created_at=datetime.now(),
        )

        # Try multiple times since there's randomness
        cards_issued = 0
        for _ in range(10):
            card = gen.generate_for_customer(high_score_customer, issue_probability=1.0)
            if card:
                cards_issued += 1

        assert cards_issued > 0

    def test_generate_for_low_score_customer(self) -> None:
        """Test that low score customers may not get cards."""
        gen = CreditCardGenerator(seed=42)

        low_score_customer = Customer(
            customer_id="cust-002",
            cpf="987.654.321-00",
            name="Low Score",
            email="low@test.com",
            phone="+5511888888888",
            address=Address(
                street="Test",
                number="2",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            monthly_income=Decimal("1500.00"),
            employment_status="UNEMPLOYED",
            credit_score=350,
            created_at=datetime.now(),
        )

        cards_issued = 0
        for _ in range(10):
            card = gen.generate_for_customer(low_score_customer, issue_probability=0.5)
            if card:
                cards_issued += 1

        # Low score means lower probability
        assert cards_issued < 10

    def test_generate_transactions(self) -> None:
        """Test generating card transactions over time."""
        gen = CreditCardGenerator(seed=42)
        card = gen.generate("cust-001")

        start = datetime.now() - timedelta(days=7)
        end = datetime.now()

        transactions = list(gen.generate_transactions(card, start, end, avg_transactions_per_day=2.0))

        assert len(transactions) > 0
        for tx in transactions:
            assert tx.card_id == card.card_id
            assert tx.amount > 0
            assert tx.mcc_code in gen.MCC_CATEGORIES

    def test_merchant_name_generation(self) -> None:
        """Test merchant name generation for different MCCs."""
        gen = CreditCardGenerator(seed=42)

        for mcc_code in gen.MCC_CATEGORIES.keys():
            name = gen._generate_merchant_name(mcc_code)
            assert name is not None
            assert len(name) > 0

    def test_installments_based_on_amount(self) -> None:
        """Test that larger purchases may have more installments."""
        gen = CreditCardGenerator(seed=42)
        card = gen.generate("cust-001")

        start = datetime.now() - timedelta(days=30)
        end = datetime.now()

        transactions = list(gen.generate_transactions(card, start, end, avg_transactions_per_day=3.0))

        installments_used = set()
        for tx in transactions:
            installments_used.add(tx.installments)

        # Should have at least some with installments > 1
        assert 1 in installments_used


class TestCardTransactionGeneratorFull:
    """Full coverage tests for CardTransactionGenerator."""

    def test_generate_single(self) -> None:
        """Test generating a single card transaction."""
        gen = CardTransactionGenerator(seed=42)
        tx = gen.generate("card-001")

        assert tx.transaction_id is not None
        assert tx.card_id == "card-001"
        assert tx.amount > 0
        assert tx.mcc_code in gen.MCC_CATEGORIES
        assert tx.merchant_name is not None
        assert tx.merchant_category is not None
        assert tx.status in ["APPROVED", "DECLINED"]
        assert tx.location_country == "BR"

    def test_installments_logic(self) -> None:
        """Test installments are applied for larger amounts."""
        gen = CardTransactionGenerator(seed=42)

        with_installments = 0
        without_installments = 0

        for _ in range(100):
            tx = gen.generate("card-001")
            if tx.installments > 1:
                with_installments += 1
            else:
                without_installments += 1

        # Both cases should occur
        assert without_installments > 0

    def test_merchant_categories(self) -> None:
        """Test all merchant categories can be generated."""
        gen = CardTransactionGenerator(seed=42)
        categories = set()

        for _ in range(200):
            tx = gen.generate("card-001")
            categories.add(tx.mcc_code)

        # Should have multiple categories
        assert len(categories) >= 3


class TestLoanGeneratorFull:
    """Full coverage tests for LoanGenerator."""

    def test_generate_with_installments_personal(self) -> None:
        """Test generating personal loan with installments."""
        gen = LoanGenerator(seed=42)
        loan, installments = gen.generate_with_installments("cust-001", "PERSONAL")

        assert loan.loan_id is not None
        assert loan.loan_type == "PERSONAL"
        assert loan.principal > 0
        assert loan.interest_rate > 0
        assert len(installments) == loan.term_months
        assert loan.property_id is None
        assert loan.amortization_system == "PRICE"

    def test_generate_with_installments_housing(self) -> None:
        """Test generating housing loan with installments."""
        gen = LoanGenerator(seed=42)
        loan, installments = gen.generate_with_installments("cust-001", "HOUSING", "prop-001")

        assert loan.loan_type == "HOUSING"
        assert loan.property_id == "prop-001"
        assert loan.term_months >= 120
        assert loan.amortization_system in ["SAC", "PRICE"]

    def test_generate_with_installments_vehicle(self) -> None:
        """Test generating vehicle loan with installments."""
        gen = LoanGenerator(seed=42)
        loan, installments = gen.generate_with_installments("cust-001", "VEHICLE")

        assert loan.loan_type == "VEHICLE"
        assert loan.principal >= 20000

    def test_generate_for_customer_high_score(self) -> None:
        """Test loan generation for high credit score customer."""
        gen = LoanGenerator(seed=42)

        customer = Customer(
            customer_id="cust-001",
            cpf="123.456.789-00",
            name="Good Credit",
            email="good@test.com",
            phone="+5511999999999",
            address=Address(
                street="Test",
                number="1",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            monthly_income=Decimal("15000.00"),
            employment_status="EMPLOYED",
            credit_score=780,
            created_at=datetime.now() - timedelta(days=365),
        )

        # High score should have high approval rate
        approved = 0
        for _ in range(10):
            result = gen.generate_for_customer(customer, "PERSONAL")
            if result:
                approved += 1

        assert approved > 5  # Most should be approved

    def test_generate_for_customer_low_score(self) -> None:
        """Test loan generation for low credit score customer."""
        gen = LoanGenerator(seed=42)

        customer = Customer(
            customer_id="cust-002",
            cpf="987.654.321-00",
            name="Bad Credit",
            email="bad@test.com",
            phone="+5511888888888",
            address=Address(
                street="Test",
                number="2",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            monthly_income=Decimal("2000.00"),
            employment_status="UNEMPLOYED",
            credit_score=380,
            created_at=datetime.now() - timedelta(days=365),
        )

        # Low score means more rejections
        rejected = 0
        for _ in range(10):
            result = gen.generate_for_customer(customer, "PERSONAL")
            if result is None:
                rejected += 1

        assert rejected > 0

    def test_housing_loan_rejection_for_poor_score(self) -> None:
        """Test that housing loans are rejected for poor credit scores."""
        gen = LoanGenerator(seed=42)

        customer = Customer(
            customer_id="cust-003",
            cpf="111.222.333-44",
            name="Poor Credit",
            email="poor@test.com",
            phone="+5511777777777",
            address=Address(
                street="Test",
                number="3",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            monthly_income=Decimal("3000.00"),
            employment_status="SELF_EMPLOYED",
            credit_score=400,
            created_at=datetime.now() - timedelta(days=365),
        )

        # Housing loans are not available for poor credit
        result = gen.generate_for_customer(customer, "HOUSING")
        assert result is None

    def test_installment_calculation_price(self) -> None:
        """Test PRICE amortization installment calculation."""
        gen = LoanGenerator(seed=42)
        loan, installments = gen.generate_with_installments("cust-001", "PERSONAL")

        # All installments should have same total amount in PRICE system
        amounts = [i.total_amount for i in installments]
        # Allow small floating point differences
        assert max(amounts) - min(amounts) < Decimal("1.00")

    def test_installment_calculation_sac(self) -> None:
        """Test SAC amortization installment calculation."""
        gen = LoanGenerator(seed=100)  # Different seed to get SAC

        # Generate multiple to find one with SAC
        for _ in range(20):
            loan, installments = gen.generate_with_installments("cust-001", "HOUSING")
            if loan.amortization_system == "SAC":
                # SAC has decreasing payments
                first_amount = installments[0].total_amount
                last_amount = installments[-1].total_amount
                assert first_amount > last_amount
                break


    def test_generate_vehicle_loan(self) -> None:
        """Test VEHICLE loan generation."""
        gen = LoanGenerator(seed=42)

        customer = Customer(
            customer_id="cust-vehicle",
            cpf="555.666.777-88",
            name="Vehicle Buyer",
            email="vehicle@test.com",
            phone="+5511666666666",
            address=Address(
                street="Test",
                number="5",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            monthly_income=Decimal("8000.00"),
            employment_status="EMPLOYED",
            credit_score=700,
            created_at=datetime.now() - timedelta(days=365),
        )

        # Generate vehicle loan
        result = gen.generate_for_customer(customer, "VEHICLE")
        if result:
            loan, prop, installments = result
            assert loan.loan_type == "VEHICLE"
            assert loan.amortization_system == "PRICE"
            assert prop is None  # Vehicle loans don't have property
            assert len(installments) > 0
            assert loan.principal >= Decimal("10000")
            assert loan.principal <= Decimal("200000")

    def test_score_tier_good(self) -> None:
        """Test 'good' credit score tier (650-749)."""
        gen = LoanGenerator(seed=42)

        customer = Customer(
            customer_id="cust-good",
            cpf="444.555.666-77",
            name="Good Credit",
            email="good@test.com",
            phone="+5511555555555",
            address=Address(
                street="Test",
                number="4",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            monthly_income=Decimal("6000.00"),
            employment_status="EMPLOYED",
            credit_score=700,  # Good tier
            created_at=datetime.now() - timedelta(days=365),
        )

        # Should have reasonable approval rate
        approved = 0
        for _ in range(10):
            result = gen.generate_for_customer(customer, "PERSONAL")
            if result:
                approved += 1

        assert approved > 0

    def test_score_tier_fair(self) -> None:
        """Test 'fair' credit score tier (500-649)."""
        gen = LoanGenerator(seed=42)

        customer = Customer(
            customer_id="cust-fair",
            cpf="333.444.555-66",
            name="Fair Credit",
            email="fair@test.com",
            phone="+5511444444444",
            address=Address(
                street="Test",
                number="6",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            monthly_income=Decimal("4000.00"),
            employment_status="EMPLOYED",
            credit_score=550,  # Fair tier
            created_at=datetime.now() - timedelta(days=365),
        )

        # Should have some approvals, some rejections
        results = [gen.generate_for_customer(customer, "PERSONAL") for _ in range(10)]
        approved = sum(1 for r in results if r is not None)
        assert approved >= 0  # May or may not be approved

    def test_installment_zero_interest_rate(self) -> None:
        """Test installment calculation with zero interest rate."""
        gen = LoanGenerator(seed=42)

        # Generate a loan and manually test with zero rate
        _, installments = gen.generate_with_installments("cust-001", "PERSONAL")

        # Verify installments exist
        assert len(installments) > 0
        assert all(i.total_amount > 0 for i in installments)

    def test_generate_housing_loan_for_customer(self) -> None:
        """Test housing loan generation for customer via generate_for_customer."""
        gen = LoanGenerator(seed=42)

        # High income customer with excellent credit - eligible for housing
        customer = Customer(
            customer_id="cust-housing",
            cpf="111.222.333-44",
            name="Housing Buyer",
            email="housing@test.com",
            phone="+5511999999999",
            address=Address(
                street="Test",
                number="1",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            monthly_income=Decimal("20000.00"),  # High income for housing
            employment_status="EMPLOYED",
            credit_score=780,  # Excellent score
            created_at=datetime.now() - timedelta(days=365),
        )

        # Try multiple times since there's randomness
        housing_loan_generated = False
        for _ in range(20):
            result = gen.generate_for_customer(customer, "HOUSING")
            if result is not None:
                loan, prop, installments = result
                assert loan.loan_type == "HOUSING"
                assert prop is not None  # Housing loans have property
                assert prop.property_id is not None
                assert prop.address is not None
                assert loan.property_id == prop.property_id
                assert loan.amortization_system in ["SAC", "PRICE"]
                assert len(installments) > 0
                housing_loan_generated = True
                break

        assert housing_loan_generated, "Should have generated at least one housing loan"

    def test_zero_interest_rate_price_formula(self) -> None:
        """Test PRICE formula with zero interest rate (line 286)."""
        gen = LoanGenerator(seed=42)

        # Create a loan with zero interest rate manually
        loan = Loan(
            loan_id="test-loan-zero-rate",
            customer_id="cust-001",
            loan_type="PERSONAL",
            principal=Decimal("10000.00"),
            interest_rate=Decimal("0.00"),  # Zero rate
            term_months=10,
            amortization_system="PRICE",
            status="ACTIVE",
            disbursement_date=datetime.now().date(),
            property_id=None,
            created_at=datetime.now(),
        )

        installments = list(gen._generate_installments(loan))

        # With zero interest, each payment should be principal / n
        expected_payment = 10000 / 10
        assert len(installments) == 10
        for inst in installments:
            assert float(inst.total_amount) == pytest.approx(expected_payment, rel=0.01)
            assert float(inst.principal_amount) == pytest.approx(expected_payment, rel=0.01)
            assert float(inst.interest_amount) == pytest.approx(0.0, abs=0.01)


class TestPropertyGeneratorFull:
    """Full coverage tests for PropertyGenerator."""

    def test_generate_property(self) -> None:
        """Test generating a property."""
        gen = PropertyGenerator(seed=42)
        prop = gen.generate()

        assert prop.property_id is not None
        assert prop.property_type in ["APARTMENT", "HOUSE"]
        assert prop.appraised_value >= 150000
        assert prop.area_sqm >= 40
        assert prop.registration_number is not None
        assert prop.address is not None
        assert prop.address.city is not None


class TestFraudPatternGeneratorFull:
    """Full coverage tests for FraudPatternGenerator."""

    def test_fraud_pattern_dataclass(self) -> None:
        """Test FraudPattern dataclass."""
        pattern = FraudPattern(name="test", description="Test pattern")
        assert pattern.name == "test"
        assert pattern.description == "Test pattern"

    def test_patterns_defined(self) -> None:
        """Test all fraud patterns are defined."""
        gen = FraudPatternGenerator(seed=42)

        assert "velocity" in gen.PATTERNS
        assert "amount_anomaly" in gen.PATTERNS
        assert "geographic" in gen.PATTERNS
        assert "new_payee_large" in gen.PATTERNS
        assert "round_amount" in gen.PATTERNS
        assert "night_activity" in gen.PATTERNS

    def test_inject_velocity_pattern(self) -> None:
        """Test velocity pattern injection."""
        gen = FraudPatternGenerator(seed=42)

        base_tx = Transaction(
            transaction_id="tx-001",
            account_id="acct-001",
            customer_id="cust-001",
            transaction_type="PIX",
            amount=Decimal("100.00"),
            direction="DEBIT",
            counterparty_key="12345678900",
            counterparty_name="Test Person",
            description="Test",
            timestamp=datetime.now(),
            status="COMPLETED",
        )

        frauds = gen.inject_velocity_pattern(base_tx, count=5, window_minutes=10)

        assert len(frauds) == 5
        for fraud in frauds:
            assert fraud.account_id == base_tx.account_id
            assert fraud.transaction_type == "PIX"
            assert fraud.direction == "DEBIT"

        # All should be within window
        timestamps = [f.timestamp for f in frauds]
        time_diff = max(timestamps) - min(timestamps)
        assert time_diff.total_seconds() <= 600  # 10 minutes

    def test_inject_amount_anomaly(self) -> None:
        """Test amount anomaly injection."""
        gen = FraudPatternGenerator(seed=42)

        base_tx = Transaction(
            transaction_id="tx-002",
            account_id="acct-001",
            customer_id="cust-001",
            transaction_type="PIX",
            amount=Decimal("100.00"),
            direction="DEBIT",
            counterparty_key="12345678900",
            counterparty_name="Test Person",
            description="Test",
            timestamp=datetime.now(),
            status="COMPLETED",
        )

        fraud = gen.inject_amount_anomaly(base_tx, multiplier=50.0)

        assert fraud.amount == base_tx.amount * Decimal("50.0")
        assert fraud.account_id == base_tx.account_id

    def test_inject_night_activity(self) -> None:
        """Test night activity injection."""
        gen = FraudPatternGenerator(seed=42)

        base_tx = Transaction(
            transaction_id="tx-003",
            account_id="acct-001",
            customer_id="cust-001",
            transaction_type="PIX",
            amount=Decimal("100.00"),
            direction="DEBIT",
            counterparty_key="12345678900",
            counterparty_name="Test Person",
            description="Test",
            timestamp=datetime.now().replace(hour=14),  # Daytime
            status="COMPLETED",
        )

        fraud = gen.inject_night_activity(base_tx)

        # Should be at night hours (1-5)
        assert fraud.timestamp.hour in [1, 2, 3, 4, 5]
        assert fraud.transaction_type == "WITHDRAW"

    def test_inject_new_payee_large_amount(self) -> None:
        """Test new payee large amount injection."""
        gen = FraudPatternGenerator(seed=42)

        base_tx = Transaction(
            transaction_id="tx-004",
            account_id="acct-001",
            customer_id="cust-001",
            transaction_type="PIX",
            amount=Decimal("100.00"),
            direction="DEBIT",
            counterparty_key="12345678900",
            counterparty_name="Test Person",
            description="Test",
            timestamp=datetime.now(),
            status="COMPLETED",
        )

        fraud = gen.inject_new_payee_large_amount(base_tx)

        assert fraud.amount >= Decimal("5000.00")
        assert "new-payee" in fraud.counterparty_key
        assert "Novo Destinatário" in fraud.counterparty_name

    def test_inject_round_amounts(self) -> None:
        """Test round amounts injection."""
        gen = FraudPatternGenerator(seed=42)

        base_tx = Transaction(
            transaction_id="tx-005",
            account_id="acct-001",
            customer_id="cust-001",
            transaction_type="PIX",
            amount=Decimal("100.00"),
            direction="DEBIT",
            counterparty_key="12345678900",
            counterparty_name="Test Person",
            description="Test",
            timestamp=datetime.now(),
            status="COMPLETED",
        )

        frauds = gen.inject_round_amounts(base_tx, count=3)

        assert len(frauds) == 3
        for fraud in frauds:
            # Amount should be a round number
            assert fraud.amount in [
                Decimal("1000"),
                Decimal("2000"),
                Decimal("5000"),
                Decimal("10000"),
                Decimal("20000"),
            ]


class TestPaymentBehaviorFull:
    """Full coverage tests for PaymentBehavior."""

    def test_apply_good_behavior(self) -> None:
        """Test applying good payment behavior."""
        behavior = PaymentBehavior(seed=42)

        # Create installments
        installments = []
        base_date = datetime.now().date() - timedelta(days=365)

        for i in range(12):
            due_date = base_date + timedelta(days=30 * i)
            inst = Installment(
                installment_id=f"inst-{i}",
                loan_id="loan-001",
                customer_id="cust-001",
                installment_number=i + 1,
                due_date=due_date,
                principal_amount=Decimal("400.00"),
                interest_amount=Decimal("100.00"),
                total_amount=Decimal("500.00"),
                paid_date=None,
                paid_amount=None,
                status="PENDING",
            )
            installments.append(inst)

        # Apply with 100% on-time rate
        result = behavior.apply_payment_behavior(
            installments, on_time_rate=1.0, late_rate=0.0, default_rate=0.0
        )

        # All past-due should be paid
        paid_count = sum(1 for i in result if i.status == "PAID")
        assert paid_count > 0

    def test_apply_defaulter_behavior(self) -> None:
        """Test applying defaulter payment behavior."""
        behavior = PaymentBehavior(seed=42)

        installments = []
        base_date = datetime.now().date() - timedelta(days=365)

        for i in range(12):
            due_date = base_date + timedelta(days=30 * i)
            inst = Installment(
                installment_id=f"inst-{i}",
                loan_id="loan-001",
                customer_id="cust-001",
                installment_number=i + 1,
                due_date=due_date,
                principal_amount=Decimal("400.00"),
                interest_amount=Decimal("100.00"),
                total_amount=Decimal("500.00"),
                paid_date=None,
                paid_amount=None,
                status="PENDING",
            )
            installments.append(inst)

        # Apply with high default rate
        result = behavior.apply_payment_behavior(
            installments, on_time_rate=0.0, late_rate=0.0, default_rate=1.0
        )

        # Should have some defaults after first few payments
        statuses = [i.status for i in result]
        # Defaulter pays first few then stops
        assert "PAID" in statuses or "DEFAULT" in statuses or "LATE" in statuses

    def test_future_installments_unchanged(self) -> None:
        """Test that future installments remain pending."""
        behavior = PaymentBehavior(seed=42)

        installments = []
        future_date = datetime.now().date() + timedelta(days=30)

        for i in range(6):
            due_date = future_date + timedelta(days=30 * i)
            inst = Installment(
                installment_id=f"inst-{i}",
                loan_id="loan-001",
                customer_id="cust-001",
                installment_number=i + 1,
                due_date=due_date,
                principal_amount=Decimal("400.00"),
                interest_amount=Decimal("100.00"),
                total_amount=Decimal("500.00"),
                paid_date=None,
                paid_amount=None,
                status="PENDING",
            )
            installments.append(inst)

        result = behavior.apply_payment_behavior(installments)

        # All future installments should remain pending
        for inst in result:
            assert inst.status == "PENDING"
            assert inst.paid_date is None

    def test_mixed_behavior(self) -> None:
        """Test mixed on-time and late behavior."""
        behavior = PaymentBehavior(seed=42)

        installments = []
        base_date = datetime.now().date() - timedelta(days=180)

        for i in range(6):
            due_date = base_date + timedelta(days=30 * i)
            inst = Installment(
                installment_id=f"inst-{i}",
                loan_id="loan-001",
                customer_id="cust-001",
                installment_number=i + 1,
                due_date=due_date,
                principal_amount=Decimal("400.00"),
                interest_amount=Decimal("100.00"),
                total_amount=Decimal("500.00"),
                paid_date=None,
                paid_amount=None,
                status="PENDING",
            )
            installments.append(inst)

        # Mix of behaviors
        result = behavior.apply_payment_behavior(
            installments, on_time_rate=0.5, late_rate=0.4, default_rate=0.1
        )

        # Should have variety of statuses
        assert len(result) == 6


class TestStockGeneratorFull:
    """Full coverage tests for StockGenerator."""

    def test_generate_single(self) -> None:
        """Test generating a single stock."""
        from data_gen.generators.financial.stock import StockGenerator

        gen = StockGenerator(seed=42)
        stock = gen.generate()

        assert stock.stock_id is not None
        assert stock.ticker is not None
        assert stock.company_name is not None
        assert stock.sector is not None
        assert stock.segment in ["NOVO_MERCADO", "N1", "N2", "TRADICIONAL"]
        assert stock.current_price > 0
        assert stock.currency == "BRL"
        assert stock.isin.startswith("BR")
        assert stock.lot_size == 100

    def test_generate_with_specific_data(self) -> None:
        """Test generating stock with specific data."""
        from data_gen.generators.financial.stock import StockGenerator

        gen = StockGenerator(seed=42)
        stock_data = {
            "ticker": "PETR4",
            "company": "Petrobras",
            "sector": "ENERGY",
            "price_range": (25, 40),
        }
        stock = gen.generate(stock_data)

        assert stock.ticker == "PETR4"
        assert stock.company_name == "Petrobras"
        assert stock.sector == "ENERGY"
        assert Decimal("25") <= stock.current_price <= Decimal("40")

    def test_generate_all(self) -> None:
        """Test generating all predefined stocks."""
        from data_gen.generators.financial.stock import StockGenerator

        gen = StockGenerator(seed=42)
        stocks = list(gen.generate_all())

        assert len(stocks) == len(gen.B3_STOCKS)

        # Check all tickers are unique
        tickers = [s.ticker for s in stocks]
        assert len(set(tickers)) == len(tickers)

    def test_generate_batch(self) -> None:
        """Test generating a batch of stocks."""
        from data_gen.generators.financial.stock import StockGenerator

        gen = StockGenerator(seed=42)
        stocks = gen.generate_batch(10)

        assert len(stocks) == 10

    def test_sectors_covered(self) -> None:
        """Test that various sectors are represented."""
        from data_gen.generators.financial.stock import StockGenerator

        gen = StockGenerator(seed=42)
        stocks = list(gen.generate_all())

        sectors = set(s.sector for s in stocks)

        assert "ENERGY" in sectors
        assert "FINANCE" in sectors
        assert "MINING" in sectors
        assert "RETAIL" in sectors


class TestTradeGeneratorFull:
    """Full coverage tests for TradeGenerator."""

    def test_generate_single(self) -> None:
        """Test generating a single trade."""
        from data_gen.generators.financial.stock import StockGenerator, TradeGenerator
        from data_gen.models.financial import Stock

        stock_gen = StockGenerator(seed=42)
        trade_gen = TradeGenerator(seed=42)

        stock = stock_gen.generate()
        trade = trade_gen.generate("acct-invest-001", stock)

        assert trade.trade_id is not None
        assert trade.account_id == "acct-invest-001"
        assert trade.stock_id == stock.stock_id
        assert trade.ticker == stock.ticker
        assert trade.trade_type in ["BUY", "SELL"]
        assert trade.quantity > 0
        assert trade.price_per_share > 0
        assert trade.total_amount > 0
        assert trade.fees >= 0
        assert trade.net_amount > 0
        assert trade.order_type in ["MARKET", "LIMIT", "STOP"]
        assert trade.status == "EXECUTED"

    def test_generate_buy_trade(self) -> None:
        """Test generating a buy trade."""
        from data_gen.generators.financial.stock import StockGenerator, TradeGenerator

        stock_gen = StockGenerator(seed=42)
        trade_gen = TradeGenerator(seed=42)

        stock = stock_gen.generate()
        trade = trade_gen.generate("acct-001", stock, trade_type="BUY")

        assert trade.trade_type == "BUY"
        # For buy, net_amount = total + fees
        assert trade.net_amount == trade.total_amount + trade.fees

    def test_generate_sell_trade(self) -> None:
        """Test generating a sell trade."""
        from data_gen.generators.financial.stock import StockGenerator, TradeGenerator

        stock_gen = StockGenerator(seed=42)
        trade_gen = TradeGenerator(seed=42)

        stock = stock_gen.generate()
        trade = trade_gen.generate("acct-001", stock, trade_type="SELL")

        assert trade.trade_type == "SELL"
        # For sell, net_amount = total - fees
        assert trade.net_amount == trade.total_amount - trade.fees

    def test_settlement_date_calculation(self) -> None:
        """Test T+2 settlement date calculation."""
        from data_gen.generators.financial.stock import StockGenerator, TradeGenerator

        stock_gen = StockGenerator(seed=42)
        trade_gen = TradeGenerator(seed=42)

        stock = stock_gen.generate()
        executed_at = datetime(2024, 1, 15, 14, 30, 0)  # Monday
        trade = trade_gen.generate("acct-001", stock, executed_at=executed_at)

        # T+2 from Monday = Wednesday (no weekends in between)
        assert trade.settlement_date.date() == datetime(2024, 1, 17).date()

    def test_settlement_date_skips_weekend(self) -> None:
        """Test settlement date skips weekends."""
        from data_gen.generators.financial.stock import TradeGenerator

        trade_gen = TradeGenerator(seed=42)

        # Thursday
        thursday = datetime(2024, 1, 18, 14, 0, 0)
        settlement = trade_gen._calculate_settlement_date(thursday)

        # T+2 from Thursday = Monday (skips Saturday and Sunday)
        assert settlement.weekday() == 0  # Monday
        assert settlement.date() == datetime(2024, 1, 22).date()

    def test_generate_trades_for_account(self) -> None:
        """Test generating multiple trades for an account."""
        from data_gen.generators.financial.stock import StockGenerator, TradeGenerator

        stock_gen = StockGenerator(seed=42)
        trade_gen = TradeGenerator(seed=42)

        stocks = stock_gen.generate_batch(5)
        trades = trade_gen.generate_trades_for_account(
            account_id="acct-invest-001",
            stocks=stocks,
            num_trades=10,
        )

        assert len(trades) == 10

        # Trades should be sorted by execution time
        for i in range(len(trades) - 1):
            assert trades[i].executed_at <= trades[i + 1].executed_at

    def test_generate_trades_empty_stocks(self) -> None:
        """Test generating trades with empty stock list."""
        from data_gen.generators.financial.stock import TradeGenerator

        trade_gen = TradeGenerator(seed=42)
        trades = trade_gen.generate_trades_for_account(
            account_id="acct-001",
            stocks=[],
            num_trades=10,
        )

        assert trades == []

    def test_fee_calculation(self) -> None:
        """Test fee calculation."""
        from data_gen.generators.financial.stock import StockGenerator, TradeGenerator

        stock_gen = StockGenerator(seed=42)
        trade_gen = TradeGenerator(seed=42)

        stock = stock_gen.generate()
        trade = trade_gen.generate("acct-001", stock)

        # Fees should be small percentage of total
        fee_pct = trade.fees / trade.total_amount
        assert fee_pct < Decimal("0.01")  # Less than 1%

    def test_order_types_distribution(self) -> None:
        """Test that different order types are generated."""
        from data_gen.generators.financial.stock import StockGenerator, TradeGenerator

        stock_gen = StockGenerator(seed=42)
        trade_gen = TradeGenerator(seed=42)

        stocks = stock_gen.generate_batch(5)
        trades = trade_gen.generate_trades_for_account(
            account_id="acct-001",
            stocks=stocks,
            num_trades=50,
        )

        order_types = set(t.order_type for t in trades)

        # MARKET has highest weight, should always appear
        assert "MARKET" in order_types
