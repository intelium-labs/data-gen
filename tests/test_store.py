"""Comprehensive tests for FinancialDataStore - 100% coverage."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from data_gen.exceptions import InvalidEntityStateError, ReferentialIntegrityError
from data_gen.models.base import Address
from data_gen.models.financial import (
    Account,
    CardTransaction,
    CreditCard,
    Customer,
    Installment,
    Loan,
    Property,
    Stock,
    Trade,
    Transaction,
)
from data_gen.store.financial import FinancialDataStore, MasterDataStore


@pytest.fixture
def store() -> FinancialDataStore:
    """Create a fresh store for each test."""
    return FinancialDataStore()


@pytest.fixture
def sample_customer() -> Customer:
    """Create a sample customer."""
    return Customer(
        customer_id="cust-001",
        cpf="123.456.789-00",
        name="Test Customer",
        email="test@test.com",
        phone="+5511999999999",
        address=Address(
            street="Test Street",
            number="100",
            neighborhood="Test",
            city="São Paulo",
            state="SP",
            postal_code="01234-567",
        ),
        monthly_income=Decimal("10000.00"),
        employment_status="EMPLOYED",
        credit_score=700,
        created_at=datetime.now(),
    )


@pytest.fixture
def sample_account(sample_customer: Customer) -> Account:
    """Create a sample account."""
    return Account(
        account_id="acct-001",
        customer_id=sample_customer.customer_id,
        account_type="CHECKING",
        bank_code="341",
        branch="0001",
        account_number="123456-7",
        balance=Decimal("5000.00"),
        status="ACTIVE",
        created_at=datetime.now(),
    )


@pytest.fixture
def sample_property() -> Property:
    """Create a sample property."""
    return Property(
        property_id="prop-001",
        property_type="APARTMENT",
        address=Address(
            street="Av Paulista",
            number="1000",
            neighborhood="Bela Vista",
            city="São Paulo",
            state="SP",
            postal_code="01310-100",
        ),
        appraised_value=Decimal("500000.00"),
        area_sqm=80.0,
        registration_number="12345.001",
    )


class TestFinancialDataStoreCustomer:
    """Tests for customer operations."""

    def test_add_customer(self, store: FinancialDataStore, sample_customer: Customer) -> None:
        """Test adding a customer."""
        store.add_customer(sample_customer)

        assert sample_customer.customer_id in store.customers
        assert store.customers[sample_customer.customer_id] == sample_customer
        assert sample_customer.customer_id in store._customer_accounts
        assert sample_customer.customer_id in store._customer_cards
        assert sample_customer.customer_id in store._customer_loans

    def test_add_multiple_customers(self, store: FinancialDataStore) -> None:
        """Test adding multiple customers."""
        for i in range(5):
            customer = Customer(
                customer_id=f"cust-{i:03d}",
                cpf=f"123.456.789-{i:02d}",
                name=f"Customer {i}",
                email=f"cust{i}@test.com",
                phone=f"+551199999999{i}",
                address=Address(
                    street="Test",
                    number=str(i),
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

        assert len(store.customers) == 5


class TestFinancialDataStoreAccount:
    """Tests for account operations."""

    def test_add_account(
        self, store: FinancialDataStore, sample_customer: Customer, sample_account: Account
    ) -> None:
        """Test adding an account."""
        store.add_customer(sample_customer)
        store.add_account(sample_account)

        assert sample_account.account_id in store.accounts
        assert sample_account.account_id in store._customer_accounts[sample_customer.customer_id]
        assert sample_account.account_id in store._account_transactions

    def test_add_account_without_customer_fails(
        self, store: FinancialDataStore, sample_account: Account
    ) -> None:
        """Test that adding account without customer raises error."""
        with pytest.raises(ReferentialIntegrityError, match="Customer .* not found"):
            store.add_account(sample_account)

    def test_get_customer_accounts(
        self, store: FinancialDataStore, sample_customer: Customer
    ) -> None:
        """Test retrieving accounts for a customer."""
        store.add_customer(sample_customer)

        # Add multiple accounts
        for i in range(3):
            account = Account(
                account_id=f"acct-{i:03d}",
                customer_id=sample_customer.customer_id,
                account_type="CHECKING" if i % 2 == 0 else "SAVINGS",
                bank_code="341",
                branch="0001",
                account_number=f"12345{i}-7",
                balance=Decimal("1000.00"),
                status="ACTIVE",
                created_at=datetime.now(),
            )
            store.add_account(account)

        accounts = store.get_customer_accounts(sample_customer.customer_id)
        assert len(accounts) == 3

    def test_get_customer_accounts_empty(self, store: FinancialDataStore) -> None:
        """Test getting accounts for non-existent customer."""
        accounts = store.get_customer_accounts("non-existent")
        assert accounts == []


class TestFinancialDataStoreCreditCard:
    """Tests for credit card operations."""

    def test_add_credit_card(self, store: FinancialDataStore, sample_customer: Customer) -> None:
        """Test adding a credit card."""
        store.add_customer(sample_customer)

        card = CreditCard(
            card_id="card-001",
            customer_id=sample_customer.customer_id,
            card_number_masked="****-****-****-1234",
            brand="VISA",
            credit_limit=Decimal("5000.00"),
            available_limit=Decimal("4500.00"),
            due_day=15,
            status="ACTIVE",
            created_at=datetime.now(),
        )
        store.add_credit_card(card)

        assert card.card_id in store.credit_cards
        assert card.card_id in store._customer_cards[sample_customer.customer_id]
        assert card.card_id in store._card_transactions

    def test_add_credit_card_without_customer_fails(self, store: FinancialDataStore) -> None:
        """Test that adding card without customer raises error."""
        card = CreditCard(
            card_id="card-001",
            customer_id="non-existent",
            card_number_masked="****-****-****-1234",
            brand="VISA",
            credit_limit=Decimal("5000.00"),
            available_limit=Decimal("4500.00"),
            due_day=15,
            status="ACTIVE",
            created_at=datetime.now(),
        )

        with pytest.raises(ReferentialIntegrityError, match="Customer .* not found"):
            store.add_credit_card(card)

    def test_get_customer_cards(self, store: FinancialDataStore, sample_customer: Customer) -> None:
        """Test retrieving cards for a customer."""
        store.add_customer(sample_customer)

        for i in range(2):
            card = CreditCard(
                card_id=f"card-{i:03d}",
                customer_id=sample_customer.customer_id,
                card_number_masked=f"****-****-****-{1000 + i}",
                brand="MASTERCARD",
                credit_limit=Decimal("10000.00"),
                available_limit=Decimal("8000.00"),
                due_day=10,
                status="ACTIVE",
                created_at=datetime.now(),
            )
            store.add_credit_card(card)

        cards = store.get_customer_cards(sample_customer.customer_id)
        assert len(cards) == 2

    def test_get_customer_cards_empty(self, store: FinancialDataStore) -> None:
        """Test getting cards for non-existent customer."""
        cards = store.get_customer_cards("non-existent")
        assert cards == []


class TestFinancialDataStoreLoan:
    """Tests for loan operations."""

    def test_add_loan_personal(
        self, store: FinancialDataStore, sample_customer: Customer
    ) -> None:
        """Test adding a personal loan."""
        store.add_customer(sample_customer)

        loan = Loan(
            loan_id="loan-001",
            customer_id=sample_customer.customer_id,
            loan_type="PERSONAL",
            principal=Decimal("10000.00"),
            interest_rate=Decimal("0.0299"),
            term_months=24,
            amortization_system="PRICE",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id=None,
            created_at=datetime.now(),
        )
        store.add_loan(loan)

        assert loan.loan_id in store.loans
        assert loan.loan_id in store._customer_loans[sample_customer.customer_id]
        assert loan.loan_id in store._loan_installments

    def test_add_loan_with_property(
        self,
        store: FinancialDataStore,
        sample_customer: Customer,
        sample_property: Property,
    ) -> None:
        """Test adding a housing loan with property."""
        store.add_customer(sample_customer)
        store.add_property(sample_property)

        loan = Loan(
            loan_id="loan-002",
            customer_id=sample_customer.customer_id,
            loan_type="HOUSING",
            principal=Decimal("400000.00"),
            interest_rate=Decimal("0.0089"),
            term_months=360,
            amortization_system="SAC",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id=sample_property.property_id,
            created_at=datetime.now(),
        )
        store.add_loan(loan)

        assert loan.loan_id in store.loans
        assert loan.property_id == sample_property.property_id

    def test_add_loan_without_customer_fails(self, store: FinancialDataStore) -> None:
        """Test that adding loan without customer raises error."""
        loan = Loan(
            loan_id="loan-001",
            customer_id="non-existent",
            loan_type="PERSONAL",
            principal=Decimal("10000.00"),
            interest_rate=Decimal("0.0299"),
            term_months=24,
            amortization_system="PRICE",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id=None,
            created_at=datetime.now(),
        )

        with pytest.raises(ReferentialIntegrityError, match="Customer .* not found"):
            store.add_loan(loan)

    def test_add_loan_with_missing_property_fails(
        self, store: FinancialDataStore, sample_customer: Customer
    ) -> None:
        """Test that adding loan with non-existent property raises error."""
        store.add_customer(sample_customer)

        loan = Loan(
            loan_id="loan-002",
            customer_id=sample_customer.customer_id,
            loan_type="HOUSING",
            principal=Decimal("400000.00"),
            interest_rate=Decimal("0.0089"),
            term_months=360,
            amortization_system="SAC",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id="non-existent",
            created_at=datetime.now(),
        )

        with pytest.raises(ReferentialIntegrityError, match="Property .* not found"):
            store.add_loan(loan)

    def test_get_customer_loans(self, store: FinancialDataStore, sample_customer: Customer) -> None:
        """Test retrieving loans for a customer."""
        store.add_customer(sample_customer)

        for i in range(2):
            loan = Loan(
                loan_id=f"loan-{i:03d}",
                customer_id=sample_customer.customer_id,
                loan_type="PERSONAL",
                principal=Decimal("5000.00"),
                interest_rate=Decimal("0.03"),
                term_months=12,
                amortization_system="PRICE",
                status="ACTIVE",
                disbursement_date=date.today(),
                property_id=None,
                created_at=datetime.now(),
            )
            store.add_loan(loan)

        loans = store.get_customer_loans(sample_customer.customer_id)
        assert len(loans) == 2

    def test_get_customer_loans_empty(self, store: FinancialDataStore) -> None:
        """Test getting loans for non-existent customer."""
        loans = store.get_customer_loans("non-existent")
        assert loans == []


class TestFinancialDataStoreProperty:
    """Tests for property operations."""

    def test_add_property(self, store: FinancialDataStore, sample_property: Property) -> None:
        """Test adding a property."""
        store.add_property(sample_property)

        assert sample_property.property_id in store.properties


class TestFinancialDataStoreTransaction:
    """Tests for transaction operations."""

    def test_add_transaction(
        self, store: FinancialDataStore, sample_customer: Customer, sample_account: Account
    ) -> None:
        """Test adding a transaction."""
        store.add_customer(sample_customer)
        store.add_account(sample_account)

        tx = Transaction(
            transaction_id="tx-001",
            account_id=sample_account.account_id,
            customer_id="cust-001",
            transaction_type="PIX",
            amount=Decimal("100.00"),
            direction="DEBIT",
            counterparty_key="12345678900",
            counterparty_name="Test Recipient",
            description="Test transaction",
            timestamp=datetime.now(),
            status="COMPLETED",
        )
        store.add_transaction(tx)

        assert len(store.transactions) == 1
        assert 0 in store._account_transactions[sample_account.account_id]

    def test_add_transaction_without_account_fails(self, store: FinancialDataStore) -> None:
        """Test that adding transaction without account raises error."""
        tx = Transaction(
            transaction_id="tx-001",
            account_id="non-existent",
            customer_id="cust-001",
            transaction_type="PIX",
            amount=Decimal("100.00"),
            direction="DEBIT",
            counterparty_key="12345678900",
            counterparty_name="Test",
            description="Test",
            timestamp=datetime.now(),
            status="COMPLETED",
        )

        with pytest.raises(ReferentialIntegrityError, match="Account .* not found"):
            store.add_transaction(tx)

    def test_get_account_transactions(
        self, store: FinancialDataStore, sample_customer: Customer, sample_account: Account
    ) -> None:
        """Test retrieving transactions for an account."""
        store.add_customer(sample_customer)
        store.add_account(sample_account)

        for i in range(5):
            tx = Transaction(
                transaction_id=f"tx-{i:03d}",
                account_id=sample_account.account_id,
                customer_id="cust-001",
                transaction_type="PIX",
                amount=Decimal(f"{100 + i}.00"),
                direction="DEBIT",
                counterparty_key=f"1234567890{i}",
                counterparty_name=f"Recipient {i}",
                description=f"Transaction {i}",
                timestamp=datetime.now(),
                status="COMPLETED",
            )
            store.add_transaction(tx)

        transactions = store.get_account_transactions(sample_account.account_id)
        assert len(transactions) == 5

    def test_get_account_transactions_empty(self, store: FinancialDataStore) -> None:
        """Test getting transactions for non-existent account."""
        transactions = store.get_account_transactions("non-existent")
        assert transactions == []


class TestFinancialDataStoreCardTransaction:
    """Tests for card transaction operations."""

    def test_add_card_transaction(
        self, store: FinancialDataStore, sample_customer: Customer
    ) -> None:
        """Test adding a card transaction."""
        store.add_customer(sample_customer)

        card = CreditCard(
            card_id="card-001",
            customer_id=sample_customer.customer_id,
            card_number_masked="****-****-****-1234",
            brand="VISA",
            credit_limit=Decimal("5000.00"),
            available_limit=Decimal("4500.00"),
            due_day=15,
            status="ACTIVE",
            created_at=datetime.now(),
        )
        store.add_credit_card(card)

        card_tx = CardTransaction(
            transaction_id="ctx-001",
            card_id=card.card_id,
            customer_id="cust-001",
            merchant_name="Test Merchant",
            merchant_category="Supermercados",
            mcc_code="5411",
            amount=Decimal("150.00"),
            installments=1,
            timestamp=datetime.now(),
            status="APPROVED",
            location_city="São Paulo",
            location_country="BR",
        )
        store.add_card_transaction(card_tx)

        assert len(store.card_transactions) == 1
        assert 0 in store._card_transactions[card.card_id]

    def test_add_card_transaction_without_card_fails(self, store: FinancialDataStore) -> None:
        """Test that adding card transaction without card raises error."""
        card_tx = CardTransaction(
            transaction_id="ctx-001",
            card_id="non-existent",
            customer_id="cust-001",
            merchant_name="Test",
            merchant_category="Test",
            mcc_code="5411",
            amount=Decimal("100.00"),
            installments=1,
            timestamp=datetime.now(),
            status="APPROVED",
            location_city="Test",
            location_country="BR",
        )

        with pytest.raises(ReferentialIntegrityError, match="Credit card .* not found"):
            store.add_card_transaction(card_tx)

    def test_get_card_transactions(
        self, store: FinancialDataStore, sample_customer: Customer
    ) -> None:
        """Test retrieving transactions for a card."""
        store.add_customer(sample_customer)

        card = CreditCard(
            card_id="card-001",
            customer_id=sample_customer.customer_id,
            card_number_masked="****-****-****-1234",
            brand="VISA",
            credit_limit=Decimal("5000.00"),
            available_limit=Decimal("4500.00"),
            due_day=15,
            status="ACTIVE",
            created_at=datetime.now(),
        )
        store.add_credit_card(card)

        for i in range(3):
            card_tx = CardTransaction(
                transaction_id=f"ctx-{i:03d}",
                card_id=card.card_id,
                customer_id="cust-001",
                merchant_name=f"Merchant {i}",
                merchant_category="Test",
                mcc_code="5411",
                amount=Decimal(f"{50 + i * 10}.00"),
                installments=1,
                timestamp=datetime.now(),
                status="APPROVED",
                location_city="Test",
                location_country="BR",
            )
            store.add_card_transaction(card_tx)

        transactions = store.get_card_transactions(card.card_id)
        assert len(transactions) == 3

    def test_get_card_transactions_empty(self, store: FinancialDataStore) -> None:
        """Test getting transactions for non-existent card."""
        transactions = store.get_card_transactions("non-existent")
        assert transactions == []


class TestFinancialDataStoreInstallment:
    """Tests for installment operations."""

    def test_add_installment(
        self, store: FinancialDataStore, sample_customer: Customer
    ) -> None:
        """Test adding an installment."""
        store.add_customer(sample_customer)

        loan = Loan(
            loan_id="loan-001",
            customer_id=sample_customer.customer_id,
            loan_type="PERSONAL",
            principal=Decimal("10000.00"),
            interest_rate=Decimal("0.03"),
            term_months=12,
            amortization_system="PRICE",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id=None,
            created_at=datetime.now(),
        )
        store.add_loan(loan)

        installment = Installment(
            installment_id="inst-001",
            loan_id=loan.loan_id,
            customer_id="cust-001",
            installment_number=1,
            due_date=date.today(),
            principal_amount=Decimal("800.00"),
            interest_amount=Decimal("200.00"),
            total_amount=Decimal("1000.00"),
            paid_date=None,
            paid_amount=None,
            status="PENDING",
        )
        store.add_installment(installment)

        assert len(store.installments) == 1
        assert 0 in store._loan_installments[loan.loan_id]

    def test_add_installment_without_loan_fails(self, store: FinancialDataStore) -> None:
        """Test that adding installment without loan raises error."""
        installment = Installment(
            installment_id="inst-001",
            loan_id="non-existent",
            customer_id="cust-001",
            installment_number=1,
            due_date=date.today(),
            principal_amount=Decimal("800.00"),
            interest_amount=Decimal("200.00"),
            total_amount=Decimal("1000.00"),
            paid_date=None,
            paid_amount=None,
            status="PENDING",
        )

        with pytest.raises(ReferentialIntegrityError, match="Loan .* not found"):
            store.add_installment(installment)

    def test_get_loan_installments(
        self, store: FinancialDataStore, sample_customer: Customer
    ) -> None:
        """Test retrieving installments for a loan."""
        store.add_customer(sample_customer)

        loan = Loan(
            loan_id="loan-001",
            customer_id=sample_customer.customer_id,
            loan_type="PERSONAL",
            principal=Decimal("6000.00"),
            interest_rate=Decimal("0.03"),
            term_months=6,
            amortization_system="PRICE",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id=None,
            created_at=datetime.now(),
        )
        store.add_loan(loan)

        for i in range(6):
            installment = Installment(
                installment_id=f"inst-{i:03d}",
                loan_id=loan.loan_id,
                customer_id="cust-001",
                installment_number=i + 1,
                due_date=date.today(),
                principal_amount=Decimal("900.00"),
                interest_amount=Decimal("100.00"),
                total_amount=Decimal("1000.00"),
                paid_date=None,
                paid_amount=None,
                status="PENDING",
            )
            store.add_installment(installment)

        installments = store.get_loan_installments(loan.loan_id)
        assert len(installments) == 6

    def test_get_loan_installments_empty(self, store: FinancialDataStore) -> None:
        """Test getting installments for non-existent loan."""
        installments = store.get_loan_installments("non-existent")
        assert installments == []


class TestFinancialDataStoreUtilities:
    """Tests for utility methods."""

    def test_get_random_account(
        self, store: FinancialDataStore, sample_customer: Customer, sample_account: Account
    ) -> None:
        """Test getting a random account."""
        store.add_customer(sample_customer)
        store.add_account(sample_account)

        account = store.get_random_account()
        assert account is not None
        assert account.account_id == sample_account.account_id

    def test_get_random_account_empty_store(self, store: FinancialDataStore) -> None:
        """Test getting random account from empty store."""
        account = store.get_random_account()
        assert account is None

    def test_summary(
        self, store: FinancialDataStore, sample_customer: Customer, sample_account: Account
    ) -> None:
        """Test summary method."""
        store.add_customer(sample_customer)
        store.add_account(sample_account)

        summary = store.summary()

        assert summary["customers"] == 1
        assert summary["accounts"] == 1
        assert summary["credit_cards"] == 0
        assert summary["loans"] == 0
        assert summary["properties"] == 0
        assert summary["transactions"] == 0
        assert summary["card_transactions"] == 0
        assert summary["installments"] == 0

    def test_summary_empty_store(self, store: FinancialDataStore) -> None:
        """Test summary on empty store."""
        summary = store.summary()

        assert summary["customers"] == 0
        assert summary["accounts"] == 0

    def test_full_store_summary(self, store: FinancialDataStore, sample_customer: Customer) -> None:
        """Test summary with all entity types populated."""
        store.add_customer(sample_customer)

        account = Account(
            account_id="acct-001",
            customer_id=sample_customer.customer_id,
            account_type="CHECKING",
            bank_code="341",
            branch="0001",
            account_number="123456-7",
            balance=Decimal("5000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        store.add_account(account)

        card = CreditCard(
            card_id="card-001",
            customer_id=sample_customer.customer_id,
            card_number_masked="****-****-****-1234",
            brand="VISA",
            credit_limit=Decimal("5000.00"),
            available_limit=Decimal("4500.00"),
            due_day=15,
            status="ACTIVE",
            created_at=datetime.now(),
        )
        store.add_credit_card(card)

        prop = Property(
            property_id="prop-001",
            property_type="APARTMENT",
            address=Address(
                street="Test",
                number="1",
                neighborhood="Test",
                city="Test",
                state="SP",
                postal_code="00000-000",
            ),
            appraised_value=Decimal("500000.00"),
            area_sqm=80.0,
            registration_number="12345.001",
        )
        store.add_property(prop)

        loan = Loan(
            loan_id="loan-001",
            customer_id=sample_customer.customer_id,
            loan_type="HOUSING",
            principal=Decimal("400000.00"),
            interest_rate=Decimal("0.0089"),
            term_months=360,
            amortization_system="SAC",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id=prop.property_id,
            created_at=datetime.now(),
        )
        store.add_loan(loan)

        tx = Transaction(
            transaction_id="tx-001",
            account_id=account.account_id,
            customer_id="cust-001",
            transaction_type="PIX",
            amount=Decimal("100.00"),
            direction="DEBIT",
            counterparty_key="12345678900",
            counterparty_name="Test Recipient",
            description="Test",
            timestamp=datetime.now(),
            status="COMPLETED",
        )
        store.add_transaction(tx)

        card_tx = CardTransaction(
            transaction_id="ctx-001",
            card_id=card.card_id,
            customer_id="cust-001",
            merchant_name="Test",
            merchant_category="Test",
            mcc_code="5411",
            amount=Decimal("50.00"),
            installments=1,
            timestamp=datetime.now(),
            status="APPROVED",
            location_city="Test",
            location_country="BR",
        )
        store.add_card_transaction(card_tx)

        installment = Installment(
            installment_id="inst-001",
            loan_id=loan.loan_id,
            customer_id="cust-001",
            installment_number=1,
            due_date=date.today(),
            principal_amount=Decimal("1000.00"),
            interest_amount=Decimal("200.00"),
            total_amount=Decimal("1200.00"),
            paid_date=None,
            paid_amount=None,
            status="PENDING",
        )
        store.add_installment(installment)

        summary = store.summary()

        assert summary["customers"] == 1
        assert summary["accounts"] == 1
        assert summary["credit_cards"] == 1
        assert summary["loans"] == 1
        assert summary["properties"] == 1
        assert summary["stocks"] == 0
        assert summary["transactions"] == 1
        assert summary["card_transactions"] == 1
        assert summary["installments"] == 1
        assert summary["trades"] == 0


class TestFinancialDataStoreStock:
    """Tests for stock operations."""

    def test_add_stock(self, store: FinancialDataStore) -> None:
        """Test adding a stock."""
        stock = Stock(
            stock_id="stock-001",
            ticker="PETR4",
            company_name="Petrobras",
            sector="ENERGY",
            segment="NOVO_MERCADO",
            current_price=Decimal("32.50"),
            currency="BRL",
            isin="BRPETRACNPR6",
            lot_size=100,
            created_at=datetime.now(),
        )
        store.add_stock(stock)

        assert stock.stock_id in store.stocks
        assert store.stocks[stock.stock_id] == stock


class TestFinancialDataStoreTrade:
    """Tests for trade operations."""

    def test_add_trade(self, store: FinancialDataStore, sample_customer: Customer) -> None:
        """Test adding a trade."""
        store.add_customer(sample_customer)

        # Create investment account
        account = Account(
            account_id="acct-invest-001",
            customer_id=sample_customer.customer_id,
            account_type="INVESTIMENTOS",
            bank_code="341",
            branch="0001",
            account_number="123456-7",
            balance=Decimal("50000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        store.add_account(account)

        # Create stock
        stock = Stock(
            stock_id="stock-001",
            ticker="VALE3",
            company_name="Vale",
            sector="MINING",
            segment="NOVO_MERCADO",
            current_price=Decimal("68.00"),
            currency="BRL",
            isin="BRVALEACNOR0",
            lot_size=100,
            created_at=datetime.now(),
        )
        store.add_stock(stock)

        # Create trade
        trade = Trade(
            trade_id="trade-001",
            account_id=account.account_id,
            customer_id="cust-001",
            stock_id=stock.stock_id,
            ticker=stock.ticker,
            trade_type="BUY",
            quantity=100,
            price_per_share=Decimal("68.50"),
            total_amount=Decimal("6850.00"),
            fees=Decimal("3.43"),
            net_amount=Decimal("6853.43"),
            order_type="MARKET",
            status="EXECUTED",
            executed_at=datetime.now(),
            settlement_date=datetime.now(),
        )
        store.add_trade(trade)

        assert len(store.trades) == 1
        assert 0 in store._account_trades[account.account_id]

    def test_add_trade_without_account_fails(self, store: FinancialDataStore) -> None:
        """Test that adding trade without account raises error."""
        stock = Stock(
            stock_id="stock-001",
            ticker="VALE3",
            company_name="Vale",
            sector="MINING",
            segment="NOVO_MERCADO",
            current_price=Decimal("68.00"),
            currency="BRL",
            isin="BRVALEACNOR0",
            lot_size=100,
            created_at=datetime.now(),
        )
        store.add_stock(stock)

        trade = Trade(
            trade_id="trade-001",
            account_id="non-existent",
            customer_id="cust-001",
            stock_id=stock.stock_id,
            ticker=stock.ticker,
            trade_type="BUY",
            quantity=100,
            price_per_share=Decimal("68.50"),
            total_amount=Decimal("6850.00"),
            fees=Decimal("3.43"),
            net_amount=Decimal("6853.43"),
            order_type="MARKET",
            status="EXECUTED",
            executed_at=datetime.now(),
            settlement_date=datetime.now(),
        )

        with pytest.raises(ReferentialIntegrityError, match="Account .* not found"):
            store.add_trade(trade)

    def test_add_trade_without_stock_fails(
        self, store: FinancialDataStore, sample_customer: Customer
    ) -> None:
        """Test that adding trade without stock raises error."""
        store.add_customer(sample_customer)

        account = Account(
            account_id="acct-invest-001",
            customer_id=sample_customer.customer_id,
            account_type="INVESTIMENTOS",
            bank_code="341",
            branch="0001",
            account_number="123456-7",
            balance=Decimal("50000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        store.add_account(account)

        trade = Trade(
            trade_id="trade-001",
            account_id=account.account_id,
            customer_id="cust-001",
            stock_id="non-existent",
            ticker="VALE3",
            trade_type="BUY",
            quantity=100,
            price_per_share=Decimal("68.50"),
            total_amount=Decimal("6850.00"),
            fees=Decimal("3.43"),
            net_amount=Decimal("6853.43"),
            order_type="MARKET",
            status="EXECUTED",
            executed_at=datetime.now(),
            settlement_date=datetime.now(),
        )

        with pytest.raises(ReferentialIntegrityError, match="Stock .* not found"):
            store.add_trade(trade)

    def test_add_trade_non_investment_account_fails(
        self, store: FinancialDataStore, sample_customer: Customer
    ) -> None:
        """Test that adding trade to non-investment account raises error."""
        store.add_customer(sample_customer)

        # Create checking account (not investment)
        account = Account(
            account_id="acct-checking-001",
            customer_id=sample_customer.customer_id,
            account_type="CONTA_CORRENTE",
            bank_code="341",
            branch="0001",
            account_number="123456-7",
            balance=Decimal("50000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        store.add_account(account)

        stock = Stock(
            stock_id="stock-001",
            ticker="VALE3",
            company_name="Vale",
            sector="MINING",
            segment="NOVO_MERCADO",
            current_price=Decimal("68.00"),
            currency="BRL",
            isin="BRVALEACNOR0",
            lot_size=100,
            created_at=datetime.now(),
        )
        store.add_stock(stock)

        trade = Trade(
            trade_id="trade-001",
            account_id=account.account_id,
            customer_id="cust-001",
            stock_id=stock.stock_id,
            ticker=stock.ticker,
            trade_type="BUY",
            quantity=100,
            price_per_share=Decimal("68.50"),
            total_amount=Decimal("6850.00"),
            fees=Decimal("3.43"),
            net_amount=Decimal("6853.43"),
            order_type="MARKET",
            status="EXECUTED",
            executed_at=datetime.now(),
            settlement_date=datetime.now(),
        )

        with pytest.raises(InvalidEntityStateError, match="not an investment account"):
            store.add_trade(trade)

    def test_get_account_trades(
        self, store: FinancialDataStore, sample_customer: Customer
    ) -> None:
        """Test retrieving trades for an account."""
        store.add_customer(sample_customer)

        account = Account(
            account_id="acct-invest-001",
            customer_id=sample_customer.customer_id,
            account_type="INVESTIMENTOS",
            bank_code="341",
            branch="0001",
            account_number="123456-7",
            balance=Decimal("100000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        store.add_account(account)

        stock = Stock(
            stock_id="stock-001",
            ticker="ITUB4",
            company_name="Itaú Unibanco",
            sector="FINANCE",
            segment="NOVO_MERCADO",
            current_price=Decimal("30.00"),
            currency="BRL",
            isin="BRITUBACNPR1",
            lot_size=100,
            created_at=datetime.now(),
        )
        store.add_stock(stock)

        for i in range(5):
            trade = Trade(
                trade_id=f"trade-{i:03d}",
                account_id=account.account_id,
                customer_id="cust-001",
                stock_id=stock.stock_id,
                ticker=stock.ticker,
                trade_type="BUY" if i % 2 == 0 else "SELL",
                quantity=100 * (i + 1),
                price_per_share=Decimal(f"{30 + i}.00"),
                total_amount=Decimal(f"{(30 + i) * 100 * (i + 1)}.00"),
                fees=Decimal("5.00"),
                net_amount=Decimal(f"{(30 + i) * 100 * (i + 1) + 5}.00"),
                order_type="MARKET",
                status="EXECUTED",
                executed_at=datetime.now(),
                settlement_date=datetime.now(),
            )
            store.add_trade(trade)

        trades = store.get_account_trades(account.account_id)
        assert len(trades) == 5

    def test_get_account_trades_empty(self, store: FinancialDataStore) -> None:
        """Test getting trades for non-existent account."""
        trades = store.get_account_trades("non-existent")
        assert trades == []


# ---------------------------------------------------------------------------
# MasterDataStore Tests
# ---------------------------------------------------------------------------


@pytest.fixture
def master_store() -> MasterDataStore:
    """Create a fresh MasterDataStore for each test."""
    return MasterDataStore()


class TestMasterDataStoreCustomer:
    """Tests for MasterDataStore customer operations."""

    def test_add_customer(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test adding a customer initializes relationship indexes."""
        master_store.add_customer(sample_customer)

        assert sample_customer.customer_id in master_store.customers
        assert master_store.customers[sample_customer.customer_id] == sample_customer
        assert sample_customer.customer_id in master_store._customer_accounts
        assert sample_customer.customer_id in master_store._customer_cards
        assert sample_customer.customer_id in master_store._customer_loans


class TestMasterDataStoreAccount:
    """Tests for MasterDataStore account operations."""

    def test_add_account(
        self, master_store: MasterDataStore, sample_customer: Customer, sample_account: Account
    ) -> None:
        """Test adding an account with FK validation."""
        master_store.add_customer(sample_customer)
        master_store.add_account(sample_account)

        assert sample_account.account_id in master_store.accounts
        assert sample_account.account_id in master_store._customer_accounts[sample_customer.customer_id]

    def test_add_account_without_customer_fails(
        self, master_store: MasterDataStore, sample_account: Account
    ) -> None:
        """Test that adding account without customer raises error."""
        with pytest.raises(ReferentialIntegrityError, match="Customer .* not found"):
            master_store.add_account(sample_account)

    def test_add_account_invalidates_cache(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test that adding an account clears the accounts cache."""
        master_store.add_customer(sample_customer)

        acct1 = Account(
            account_id="acct-001",
            customer_id=sample_customer.customer_id,
            account_type="CHECKING",
            bank_code="341",
            branch="0001",
            account_number="111111-1",
            balance=Decimal("1000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        master_store.add_account(acct1)

        # Trigger cache population
        master_store.get_random_account()
        assert len(master_store._accounts_cache) == 1

        # Add another account — cache should be cleared
        acct2 = Account(
            account_id="acct-002",
            customer_id=sample_customer.customer_id,
            account_type="SAVINGS",
            bank_code="341",
            branch="0001",
            account_number="222222-2",
            balance=Decimal("2000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        master_store.add_account(acct2)
        assert master_store._accounts_cache == []

        # Next random access should rebuild cache with 2 accounts
        result = master_store.get_random_account()
        assert result is not None
        assert len(master_store._accounts_cache) == 2


class TestMasterDataStoreCreditCard:
    """Tests for MasterDataStore credit card operations."""

    def test_add_credit_card(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test adding a credit card."""
        master_store.add_customer(sample_customer)

        card = CreditCard(
            card_id="card-001",
            customer_id=sample_customer.customer_id,
            card_number_masked="****-****-****-1234",
            brand="VISA",
            credit_limit=Decimal("5000.00"),
            available_limit=Decimal("4500.00"),
            due_day=15,
            status="ACTIVE",
            created_at=datetime.now(),
        )
        master_store.add_credit_card(card)

        assert card.card_id in master_store.credit_cards
        assert card.card_id in master_store._customer_cards[sample_customer.customer_id]

    def test_add_credit_card_without_customer_fails(
        self, master_store: MasterDataStore
    ) -> None:
        """Test that adding card without customer raises error."""
        card = CreditCard(
            card_id="card-001",
            customer_id="non-existent",
            card_number_masked="****-****-****-1234",
            brand="VISA",
            credit_limit=Decimal("5000.00"),
            available_limit=Decimal("4500.00"),
            due_day=15,
            status="ACTIVE",
            created_at=datetime.now(),
        )

        with pytest.raises(ReferentialIntegrityError, match="Customer .* not found"):
            master_store.add_credit_card(card)


class TestMasterDataStoreLoan:
    """Tests for MasterDataStore loan operations."""

    def test_add_loan(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test adding a personal loan."""
        master_store.add_customer(sample_customer)

        loan = Loan(
            loan_id="loan-001",
            customer_id=sample_customer.customer_id,
            loan_type="PERSONAL",
            principal=Decimal("10000.00"),
            interest_rate=Decimal("0.03"),
            term_months=24,
            amortization_system="PRICE",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id=None,
            created_at=datetime.now(),
        )
        master_store.add_loan(loan)

        assert loan.loan_id in master_store.loans
        assert loan.loan_id in master_store._customer_loans[sample_customer.customer_id]

    def test_add_loan_with_property(
        self,
        master_store: MasterDataStore,
        sample_customer: Customer,
        sample_property: Property,
    ) -> None:
        """Test adding a housing loan with property reference."""
        master_store.add_customer(sample_customer)
        master_store.add_property(sample_property)

        loan = Loan(
            loan_id="loan-002",
            customer_id=sample_customer.customer_id,
            loan_type="HOUSING",
            principal=Decimal("400000.00"),
            interest_rate=Decimal("0.009"),
            term_months=360,
            amortization_system="SAC",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id=sample_property.property_id,
            created_at=datetime.now(),
        )
        master_store.add_loan(loan)

        assert loan.loan_id in master_store.loans

    def test_add_loan_without_customer_fails(self, master_store: MasterDataStore) -> None:
        """Test that adding loan without customer raises error."""
        loan = Loan(
            loan_id="loan-001",
            customer_id="non-existent",
            loan_type="PERSONAL",
            principal=Decimal("10000.00"),
            interest_rate=Decimal("0.03"),
            term_months=24,
            amortization_system="PRICE",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id=None,
            created_at=datetime.now(),
        )

        with pytest.raises(ReferentialIntegrityError, match="Customer .* not found"):
            master_store.add_loan(loan)

    def test_add_loan_with_missing_property_fails(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test that adding loan with non-existent property raises error."""
        master_store.add_customer(sample_customer)

        loan = Loan(
            loan_id="loan-002",
            customer_id=sample_customer.customer_id,
            loan_type="HOUSING",
            principal=Decimal("400000.00"),
            interest_rate=Decimal("0.009"),
            term_months=360,
            amortization_system="SAC",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id="non-existent",
            created_at=datetime.now(),
        )

        with pytest.raises(ReferentialIntegrityError, match="Property .* not found"):
            master_store.add_loan(loan)


class TestMasterDataStorePropertyAndStock:
    """Tests for MasterDataStore property and stock operations."""

    def test_add_property(
        self, master_store: MasterDataStore, sample_property: Property
    ) -> None:
        """Test adding a property."""
        master_store.add_property(sample_property)
        assert sample_property.property_id in master_store.properties

    def test_add_stock(self, master_store: MasterDataStore) -> None:
        """Test adding a stock."""
        stock = Stock(
            stock_id="stock-001",
            ticker="PETR4",
            company_name="Petrobras",
            sector="ENERGY",
            segment="NOVO_MERCADO",
            current_price=Decimal("32.50"),
            currency="BRL",
            isin="BRPETRACNPR6",
            lot_size=100,
            created_at=datetime.now(),
        )
        master_store.add_stock(stock)

        assert stock.stock_id in master_store.stocks


class TestMasterDataStoreFKValidation:
    """Tests for FK validation methods (validate without storing)."""

    def test_validate_transaction_fk_success(
        self, master_store: MasterDataStore, sample_customer: Customer, sample_account: Account
    ) -> None:
        """Test transaction FK validation passes with valid account."""
        master_store.add_customer(sample_customer)
        master_store.add_account(sample_account)

        master_store.validate_transaction_fk(sample_account.account_id)  # should not raise

    def test_validate_transaction_fk_fails(self, master_store: MasterDataStore) -> None:
        """Test transaction FK validation fails with missing account."""
        with pytest.raises(ReferentialIntegrityError, match="Account .* not found"):
            master_store.validate_transaction_fk("non-existent")

    def test_validate_card_transaction_fk_success(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test card transaction FK validation passes with valid card."""
        master_store.add_customer(sample_customer)
        card = CreditCard(
            card_id="card-001",
            customer_id=sample_customer.customer_id,
            card_number_masked="****-****-****-1234",
            brand="VISA",
            credit_limit=Decimal("5000.00"),
            available_limit=Decimal("4500.00"),
            due_day=15,
            status="ACTIVE",
            created_at=datetime.now(),
        )
        master_store.add_credit_card(card)

        master_store.validate_card_transaction_fk(card.card_id)  # should not raise

    def test_validate_card_transaction_fk_fails(self, master_store: MasterDataStore) -> None:
        """Test card transaction FK validation fails with missing card."""
        with pytest.raises(ReferentialIntegrityError, match="Credit card .* not found"):
            master_store.validate_card_transaction_fk("non-existent")

    def test_validate_trade_fk_success(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test trade FK validation passes with valid account and stock."""
        master_store.add_customer(sample_customer)

        account = Account(
            account_id="acct-invest-001",
            customer_id=sample_customer.customer_id,
            account_type="INVESTIMENTOS",
            bank_code="341",
            branch="0001",
            account_number="123456-7",
            balance=Decimal("50000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        master_store.add_account(account)

        stock = Stock(
            stock_id="stock-001",
            ticker="VALE3",
            company_name="Vale",
            sector="MINING",
            segment="NOVO_MERCADO",
            current_price=Decimal("68.00"),
            currency="BRL",
            isin="BRVALEACNOR0",
            lot_size=100,
            created_at=datetime.now(),
        )
        master_store.add_stock(stock)

        master_store.validate_trade_fk(account.account_id, stock.stock_id)  # should not raise

    def test_validate_trade_fk_missing_account(self, master_store: MasterDataStore) -> None:
        """Test trade FK validation fails with missing account."""
        stock = Stock(
            stock_id="stock-001",
            ticker="VALE3",
            company_name="Vale",
            sector="MINING",
            segment="NOVO_MERCADO",
            current_price=Decimal("68.00"),
            currency="BRL",
            isin="BRVALEACNOR0",
            lot_size=100,
            created_at=datetime.now(),
        )
        master_store.add_stock(stock)

        with pytest.raises(ReferentialIntegrityError, match="Account .* not found"):
            master_store.validate_trade_fk("non-existent", stock.stock_id)

    def test_validate_trade_fk_missing_stock(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test trade FK validation fails with missing stock."""
        master_store.add_customer(sample_customer)

        account = Account(
            account_id="acct-invest-001",
            customer_id=sample_customer.customer_id,
            account_type="INVESTIMENTOS",
            bank_code="341",
            branch="0001",
            account_number="123456-7",
            balance=Decimal("50000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        master_store.add_account(account)

        with pytest.raises(ReferentialIntegrityError, match="Stock .* not found"):
            master_store.validate_trade_fk(account.account_id, "non-existent")

    def test_validate_trade_fk_non_investment_account(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test trade FK validation fails with non-investment account."""
        master_store.add_customer(sample_customer)

        account = Account(
            account_id="acct-checking-001",
            customer_id=sample_customer.customer_id,
            account_type="CONTA_CORRENTE",
            bank_code="341",
            branch="0001",
            account_number="123456-7",
            balance=Decimal("50000.00"),
            status="ACTIVE",
            created_at=datetime.now(),
        )
        master_store.add_account(account)

        stock = Stock(
            stock_id="stock-001",
            ticker="VALE3",
            company_name="Vale",
            sector="MINING",
            segment="NOVO_MERCADO",
            current_price=Decimal("68.00"),
            currency="BRL",
            isin="BRVALEACNOR0",
            lot_size=100,
            created_at=datetime.now(),
        )
        master_store.add_stock(stock)

        with pytest.raises(InvalidEntityStateError, match="not an investment account"):
            master_store.validate_trade_fk(account.account_id, stock.stock_id)

    def test_validate_installment_fk_success(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test installment FK validation passes with valid loan."""
        master_store.add_customer(sample_customer)

        loan = Loan(
            loan_id="loan-001",
            customer_id=sample_customer.customer_id,
            loan_type="PERSONAL",
            principal=Decimal("10000.00"),
            interest_rate=Decimal("0.03"),
            term_months=12,
            amortization_system="PRICE",
            status="ACTIVE",
            disbursement_date=date.today(),
            property_id=None,
            created_at=datetime.now(),
        )
        master_store.add_loan(loan)

        master_store.validate_installment_fk(loan.loan_id)  # should not raise

    def test_validate_installment_fk_fails(self, master_store: MasterDataStore) -> None:
        """Test installment FK validation fails with missing loan."""
        with pytest.raises(ReferentialIntegrityError, match="Loan .* not found"):
            master_store.validate_installment_fk("non-existent")


class TestMasterDataStoreEventCounting:
    """Tests for event counting (count_event + summary)."""

    def test_count_event_single(self, master_store: MasterDataStore) -> None:
        """Test counting a single event."""
        master_store.count_event("transactions")
        assert master_store._event_counts["transactions"] == 1

    def test_count_event_batch(self, master_store: MasterDataStore) -> None:
        """Test counting multiple events at once."""
        master_store.count_event("card_transactions", count=50)
        assert master_store._event_counts["card_transactions"] == 50

    def test_count_event_accumulates(self, master_store: MasterDataStore) -> None:
        """Test that event counts accumulate."""
        master_store.count_event("trades", count=10)
        master_store.count_event("trades", count=20)
        assert master_store._event_counts["trades"] == 30

    def test_summary_includes_event_counts(
        self, master_store: MasterDataStore, sample_customer: Customer, sample_account: Account
    ) -> None:
        """Test that summary merges master counts with event counts."""
        master_store.add_customer(sample_customer)
        master_store.add_account(sample_account)
        master_store.count_event("transactions", count=100)
        master_store.count_event("card_transactions", count=50)
        master_store.count_event("installments", count=24)
        master_store.count_event("trades", count=10)

        summary = master_store.summary()

        assert summary["customers"] == 1
        assert summary["accounts"] == 1
        assert summary["credit_cards"] == 0
        assert summary["loans"] == 0
        assert summary["properties"] == 0
        assert summary["stocks"] == 0
        assert summary["transactions"] == 100
        assert summary["card_transactions"] == 50
        assert summary["installments"] == 24
        assert summary["trades"] == 10


class TestMasterDataStoreQueryMethods:
    """Tests for MasterDataStore query methods."""

    def test_get_customer_accounts(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test retrieving accounts for a customer."""
        master_store.add_customer(sample_customer)

        for i in range(3):
            account = Account(
                account_id=f"acct-{i:03d}",
                customer_id=sample_customer.customer_id,
                account_type="CHECKING",
                bank_code="341",
                branch="0001",
                account_number=f"12345{i}-7",
                balance=Decimal("1000.00"),
                status="ACTIVE",
                created_at=datetime.now(),
            )
            master_store.add_account(account)

        accounts = master_store.get_customer_accounts(sample_customer.customer_id)
        assert len(accounts) == 3

    def test_get_customer_accounts_empty(self, master_store: MasterDataStore) -> None:
        """Test getting accounts for non-existent customer."""
        assert master_store.get_customer_accounts("non-existent") == []

    def test_get_customer_cards(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test retrieving cards for a customer."""
        master_store.add_customer(sample_customer)

        for i in range(2):
            card = CreditCard(
                card_id=f"card-{i:03d}",
                customer_id=sample_customer.customer_id,
                card_number_masked=f"****-****-****-{1000 + i}",
                brand="MASTERCARD",
                credit_limit=Decimal("10000.00"),
                available_limit=Decimal("8000.00"),
                due_day=10,
                status="ACTIVE",
                created_at=datetime.now(),
            )
            master_store.add_credit_card(card)

        cards = master_store.get_customer_cards(sample_customer.customer_id)
        assert len(cards) == 2

    def test_get_customer_cards_empty(self, master_store: MasterDataStore) -> None:
        """Test getting cards for non-existent customer."""
        assert master_store.get_customer_cards("non-existent") == []

    def test_get_customer_loans(
        self, master_store: MasterDataStore, sample_customer: Customer
    ) -> None:
        """Test retrieving loans for a customer."""
        master_store.add_customer(sample_customer)

        for i in range(2):
            loan = Loan(
                loan_id=f"loan-{i:03d}",
                customer_id=sample_customer.customer_id,
                loan_type="PERSONAL",
                principal=Decimal("5000.00"),
                interest_rate=Decimal("0.03"),
                term_months=12,
                amortization_system="PRICE",
                status="ACTIVE",
                disbursement_date=date.today(),
                property_id=None,
                created_at=datetime.now(),
            )
            master_store.add_loan(loan)

        loans = master_store.get_customer_loans(sample_customer.customer_id)
        assert len(loans) == 2

    def test_get_customer_loans_empty(self, master_store: MasterDataStore) -> None:
        """Test getting loans for non-existent customer."""
        assert master_store.get_customer_loans("non-existent") == []

    def test_get_random_account(
        self, master_store: MasterDataStore, sample_customer: Customer, sample_account: Account
    ) -> None:
        """Test getting a random account."""
        master_store.add_customer(sample_customer)
        master_store.add_account(sample_account)

        account = master_store.get_random_account()
        assert account is not None
        assert account.account_id == sample_account.account_id

    def test_get_random_account_empty(self, master_store: MasterDataStore) -> None:
        """Test getting random account from empty store."""
        assert master_store.get_random_account() is None
