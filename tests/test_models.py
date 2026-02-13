"""Tests for domain models - 100% coverage."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from data_gen.models.base import Address, Event
from data_gen.models.financial import (
    Account,
    CardTransaction,
    CreditCard,
    Customer,
    Installment,
    Loan,
    Property,
    Transaction,
)


class TestAddress:
    """Tests for Address model."""

    def test_address_creation(self) -> None:
        """Test creating an address with all fields."""
        address = Address(
            street="Rua das Flores",
            number="123",
            neighborhood="Centro",
            city="São Paulo",
            state="SP",
            postal_code="01234-567",
            complement="Apto 101",
        )

        assert address.street == "Rua das Flores"
        assert address.number == "123"
        assert address.neighborhood == "Centro"
        assert address.city == "São Paulo"
        assert address.state == "SP"
        assert address.postal_code == "01234-567"
        assert address.complement == "Apto 101"

    def test_address_without_complement(self) -> None:
        """Test creating an address without complement."""
        address = Address(
            street="Av Brasil",
            number="456",
            neighborhood="Jardins",
            city="Rio de Janeiro",
            state="RJ",
            postal_code="20000-000",
        )

        assert address.complement == ""  # default is empty string
        assert address.country == "BR"  # default country


class TestEvent:
    """Tests for Event model."""

    def test_event_creation(self) -> None:
        """Test creating an event."""
        now = datetime.now()
        event = Event(
            event_id="evt-001",
            event_type="customer.created",
            event_time=now,
            source="data-gen",
            subject="cust-001",
            data={"customer_id": "cust-001"},
        )

        assert event.event_id == "evt-001"
        assert event.event_type == "customer.created"
        assert event.event_time == now
        assert event.source == "data-gen"
        assert event.subject == "cust-001"
        assert event.data == {"customer_id": "cust-001"}
        assert event.metadata == {}  # default empty dict


class TestCustomer:
    """Tests for Customer model."""

    def test_customer_creation(self) -> None:
        """Test creating a customer with all fields."""
        address = Address(
            street="Rua Test",
            number="1",
            neighborhood="Test",
            city="Test City",
            state="TS",
            postal_code="00000-000",
        )
        now = datetime.now()

        customer = Customer(
            customer_id="cust-001",
            cpf="123.456.789-00",
            name="João Silva",
            email="joao@test.com",
            phone="+5511999999999",
            address=address,
            monthly_income=Decimal("5000.00"),
            employment_status="EMPLOYED",
            credit_score=700,
            created_at=now,
        )

        assert customer.customer_id == "cust-001"
        assert customer.cpf == "123.456.789-00"
        assert customer.name == "João Silva"
        assert customer.email == "joao@test.com"
        assert customer.phone == "+5511999999999"
        assert customer.address == address
        assert customer.monthly_income == Decimal("5000.00")
        assert customer.employment_status == "EMPLOYED"
        assert customer.credit_score == 700
        assert customer.created_at == now


class TestAccount:
    """Tests for Account model."""

    def test_account_creation(self) -> None:
        """Test creating an account."""
        now = datetime.now()

        account = Account(
            account_id="acct-001",
            customer_id="cust-001",
            account_type="CHECKING",
            bank_code="341",
            branch="0001",
            account_number="123456-7",
            balance=Decimal("1000.00"),
            status="ACTIVE",
            created_at=now,
        )

        assert account.account_id == "acct-001"
        assert account.customer_id == "cust-001"
        assert account.account_type == "CHECKING"
        assert account.bank_code == "341"
        assert account.branch == "0001"
        assert account.account_number == "123456-7"
        assert account.balance == Decimal("1000.00")
        assert account.status == "ACTIVE"


class TestTransaction:
    """Tests for Transaction model."""

    def test_transaction_creation_pix(self) -> None:
        """Test creating a PIX transaction."""
        now = datetime.now()

        transaction = Transaction(
            transaction_id="tx-001",
            account_id="acct-001",
            customer_id="cust-001",
            transaction_type="PIX",
            amount=Decimal("100.00"),
            direction="DEBIT",
            counterparty_key="12345678900",
            counterparty_name="Maria Silva",
            description="Pix para Maria",
            timestamp=now,
            status="COMPLETED",
            pix_e2e_id="E12345678901234567890123456789012",
            pix_key_type="CPF",
        )

        assert transaction.transaction_id == "tx-001"
        assert transaction.transaction_type == "PIX"
        assert transaction.pix_e2e_id is not None
        assert transaction.pix_key_type == "CPF"

    def test_transaction_creation_ted(self) -> None:
        """Test creating a TED transaction without PIX fields."""
        now = datetime.now()

        transaction = Transaction(
            transaction_id="tx-002",
            account_id="acct-001",
            customer_id="cust-001",
            transaction_type="TED",
            amount=Decimal("500.00"),
            direction="CREDIT",
            counterparty_key="341/0001/123456-7",
            counterparty_name="José Santos",
            description="TED recebido",
            timestamp=now,
            status="COMPLETED",
        )

        assert transaction.transaction_type == "TED"
        assert transaction.pix_e2e_id is None
        assert transaction.pix_key_type is None

    def test_transaction_with_location(self) -> None:
        """Test transaction with location data."""
        now = datetime.now()

        transaction = Transaction(
            transaction_id="tx-003",
            account_id="acct-001",
            customer_id="cust-001",
            transaction_type="WITHDRAW",
            amount=Decimal("200.00"),
            direction="DEBIT",
            counterparty_key=None,
            counterparty_name=None,
            description="Saque ATM",
            timestamp=now,
            status="COMPLETED",
            location_lat=-23.5505,
            location_lon=-46.6333,
        )

        assert transaction.location_lat == -23.5505
        assert transaction.location_lon == -46.6333


class TestCreditCard:
    """Tests for CreditCard model."""

    def test_credit_card_creation(self) -> None:
        """Test creating a credit card."""
        now = datetime.now()

        card = CreditCard(
            card_id="card-001",
            customer_id="cust-001",
            card_number_masked="****-****-****-1234",
            brand="VISA",
            credit_limit=Decimal("5000.00"),
            available_limit=Decimal("4500.00"),
            due_day=15,
            status="ACTIVE",
            created_at=now,
        )

        assert card.card_id == "card-001"
        assert card.brand == "VISA"
        assert card.credit_limit == Decimal("5000.00")
        assert card.available_limit == Decimal("4500.00")
        assert card.due_day == 15


class TestCardTransaction:
    """Tests for CardTransaction model."""

    def test_card_transaction_creation(self) -> None:
        """Test creating a card transaction."""
        now = datetime.now()

        card_tx = CardTransaction(
            transaction_id="ctx-001",
            card_id="card-001",
            customer_id="cust-001",
            merchant_name="Supermercado ABC",
            merchant_category="Supermercados",
            mcc_code="5411",
            amount=Decimal("150.00"),
            installments=1,
            timestamp=now,
            status="APPROVED",
            location_city="São Paulo",
            location_country="BR",
        )

        assert card_tx.transaction_id == "ctx-001"
        assert card_tx.mcc_code == "5411"
        assert card_tx.installments == 1
        assert card_tx.status == "APPROVED"

    def test_card_transaction_with_installments(self) -> None:
        """Test card transaction with multiple installments."""
        now = datetime.now()

        card_tx = CardTransaction(
            transaction_id="ctx-002",
            card_id="card-001",
            customer_id="cust-001",
            merchant_name="Loja Eletrônicos",
            merchant_category="Eletrônicos",
            mcc_code="5732",
            amount=Decimal("1200.00"),
            installments=12,
            timestamp=now,
            status="APPROVED",
            location_city="Rio de Janeiro",
            location_country="BR",
        )

        assert card_tx.installments == 12


class TestLoan:
    """Tests for Loan model."""

    def test_personal_loan_creation(self) -> None:
        """Test creating a personal loan."""
        now = datetime.now()
        today = date.today()

        loan = Loan(
            loan_id="loan-001",
            customer_id="cust-001",
            loan_type="PERSONAL",
            principal=Decimal("10000.00"),
            interest_rate=Decimal("0.0299"),
            term_months=24,
            amortization_system="PRICE",
            status="ACTIVE",
            disbursement_date=today,
            property_id=None,
            created_at=now,
        )

        assert loan.loan_id == "loan-001"
        assert loan.loan_type == "PERSONAL"
        assert loan.amortization_system == "PRICE"
        assert loan.property_id is None

    def test_housing_loan_creation(self) -> None:
        """Test creating a housing loan with property."""
        now = datetime.now()
        today = date.today()

        loan = Loan(
            loan_id="loan-002",
            customer_id="cust-001",
            loan_type="HOUSING",
            principal=Decimal("500000.00"),
            interest_rate=Decimal("0.0089"),
            term_months=360,
            amortization_system="SAC",
            status="ACTIVE",
            disbursement_date=today,
            property_id="prop-001",
            created_at=now,
        )

        assert loan.loan_type == "HOUSING"
        assert loan.amortization_system == "SAC"
        assert loan.property_id == "prop-001"


class TestInstallment:
    """Tests for Installment model."""

    def test_pending_installment(self) -> None:
        """Test creating a pending installment."""
        due = date.today()

        installment = Installment(
            installment_id="inst-001",
            loan_id="loan-001",
            customer_id="cust-001",
            installment_number=1,
            due_date=due,
            principal_amount=Decimal("400.00"),
            interest_amount=Decimal("100.00"),
            total_amount=Decimal("500.00"),
            paid_date=None,
            paid_amount=None,
            status="PENDING",
        )

        assert installment.installment_number == 1
        assert installment.status == "PENDING"
        assert installment.paid_date is None
        assert installment.paid_amount is None

    def test_paid_installment(self) -> None:
        """Test creating a paid installment."""
        due = date.today()
        paid = date.today()

        installment = Installment(
            installment_id="inst-002",
            loan_id="loan-001",
            customer_id="cust-001",
            installment_number=2,
            due_date=due,
            principal_amount=Decimal("410.00"),
            interest_amount=Decimal("90.00"),
            total_amount=Decimal("500.00"),
            paid_date=paid,
            paid_amount=Decimal("500.00"),
            status="PAID",
        )

        assert installment.status == "PAID"
        assert installment.paid_date == paid
        assert installment.paid_amount == Decimal("500.00")


class TestProperty:
    """Tests for Property model."""

    def test_property_creation(self) -> None:
        """Test creating a property."""
        address = Address(
            street="Av Paulista",
            number="1000",
            neighborhood="Bela Vista",
            city="São Paulo",
            state="SP",
            postal_code="01310-100",
            complement="Apto 1001",
        )

        prop = Property(
            property_id="prop-001",
            property_type="APARTMENT",
            address=address,
            appraised_value=Decimal("800000.00"),
            area_sqm=85.5,
            registration_number="12345.001",
        )

        assert prop.property_id == "prop-001"
        assert prop.property_type == "APARTMENT"
        assert prop.appraised_value == Decimal("800000.00")
        assert prop.area_sqm == 85.5
        assert prop.registration_number == "12345.001"

    def test_house_property(self) -> None:
        """Test creating a house property."""
        address = Address(
            street="Rua das Palmeiras",
            number="500",
            neighborhood="Alphaville",
            city="Barueri",
            state="SP",
            postal_code="06454-000",
        )

        prop = Property(
            property_id="prop-002",
            property_type="HOUSE",
            address=address,
            appraised_value=Decimal("1500000.00"),
            area_sqm=250.0,
            registration_number="54321.002",
        )

        assert prop.property_type == "HOUSE"
