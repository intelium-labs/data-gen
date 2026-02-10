"""Behavioral patterns for realistic data generation."""

import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

from data_gen.models.financial import Installment, Transaction
from data_gen.models.financial.enums import (
    Direction,
    InstallmentStatus,
    TransactionStatus,
    TransactionType,
)


@dataclass
class FraudPattern:
    """Configuration for fraud pattern generation."""

    name: str
    description: str


class PaymentBehavior:
    """Simulate realistic loan payment behavior."""

    def __init__(self, seed: int | None = None) -> None:
        if seed is not None:
            random.seed(seed)

    def apply_payment_behavior(
        self,
        installments: list[Installment],
        on_time_rate: float = 0.85,
        late_rate: float = 0.10,
        default_rate: float = 0.05,
        reference_date: date | None = None,
    ) -> list[Installment]:
        """Apply realistic payment behavior to installments.

        Parameters
        ----------
        installments : list[Installment]
            List of installments to modify.
        on_time_rate : float
            Probability of paying on time (default 85%).
        late_rate : float
            Probability of paying late (default 10%).
        default_rate : float
            Probability of defaulting (default 5%).
        reference_date : date
            Current date for determining which installments are due.

        Returns
        -------
        list[Installment]
            Modified installments with payment status.
        """
        if reference_date is None:
            reference_date = date.today()

        # Determine customer behavior type
        behavior = random.choices(
            ["good", "occasional_late", "chronic_late", "defaulter"],
            weights=[on_time_rate, late_rate * 0.7, late_rate * 0.3, default_rate],
            k=1,
        )[0]

        result = []
        consecutive_missed = 0

        for inst in installments:
            if inst.due_date > reference_date:
                # Future installment - leave as pending
                result.append(inst)
                continue

            # Past due date - determine payment
            if behavior == "good":
                # Pays on time or within 3 days
                paid_date = inst.due_date + timedelta(days=random.randint(0, 3))
                status = InstallmentStatus.PAID
                paid_amount = inst.total_amount
                consecutive_missed = 0

            elif behavior == "occasional_late":
                # 80% on time, 20% late
                if random.random() < 0.8:
                    paid_date = inst.due_date + timedelta(days=random.randint(0, 5))
                    status = InstallmentStatus.PAID
                else:
                    paid_date = inst.due_date + timedelta(days=random.randint(10, 30))
                    status = InstallmentStatus.PAID
                paid_amount = inst.total_amount
                consecutive_missed = 0

            elif behavior == "chronic_late":
                # Always pays but usually late
                days_late = random.randint(5, 45)
                paid_date = inst.due_date + timedelta(days=days_late)
                status = InstallmentStatus.PAID if days_late < 90 else InstallmentStatus.LATE
                paid_amount = inst.total_amount
                consecutive_missed = 0 if status == InstallmentStatus.PAID else consecutive_missed + 1

            else:  # defaulter
                # Pays first few, then stops
                if inst.installment_number <= random.randint(2, 6):
                    paid_date = inst.due_date + timedelta(days=random.randint(0, 15))
                    status = InstallmentStatus.PAID
                    paid_amount = inst.total_amount
                    consecutive_missed = 0
                else:
                    paid_date = None
                    paid_amount = None
                    consecutive_missed += 1
                    if consecutive_missed >= 3:
                        status = InstallmentStatus.DEFAULT
                    else:
                        status = InstallmentStatus.LATE

            result.append(
                Installment(
                    installment_id=inst.installment_id,
                    loan_id=inst.loan_id,
                    installment_number=inst.installment_number,
                    due_date=inst.due_date,
                    principal_amount=inst.principal_amount,
                    interest_amount=inst.interest_amount,
                    total_amount=inst.total_amount,
                    paid_date=paid_date,
                    paid_amount=paid_amount,
                    status=status,
                )
            )

        return result


class FraudPatternGenerator:
    """Generate transactions with fraud patterns."""

    PATTERNS = {
        "velocity": FraudPattern(
            "velocity",
            "Many transactions in short time window",
        ),
        "amount_anomaly": FraudPattern(
            "amount_anomaly",
            "Transaction amount significantly higher than usual",
        ),
        "geographic": FraudPattern(
            "geographic",
            "Impossible geographic movement between transactions",
        ),
        "new_payee_large": FraudPattern(
            "new_payee_large",
            "Large amount to previously unknown recipient",
        ),
        "round_amount": FraudPattern(
            "round_amount",
            "Suspiciously round transaction amounts",
        ),
        "night_activity": FraudPattern(
            "night_activity",
            "Unusual activity during night hours",
        ),
    }

    def __init__(self, seed: int | None = None) -> None:
        if seed is not None:
            random.seed(seed)

    def inject_velocity_pattern(
        self,
        base_transaction: Transaction,
        count: int = 5,
        window_minutes: int = 10,
    ) -> list[Transaction]:
        """Generate multiple rapid transactions (velocity attack)."""
        transactions = []
        base_time = base_transaction.timestamp

        for i in range(count):
            tx = Transaction(
                transaction_id=f"fraud-vel-{base_transaction.transaction_id}-{i}",
                account_id=base_transaction.account_id,
                transaction_type=TransactionType.PIX,
                amount=Decimal(str(round(random.uniform(500, 5000), 2))),
                direction=Direction.DEBIT,
                counterparty_key=f"fraud-key-{i}",
                counterparty_name=f"Suspeito {i}",
                description="Pix - Transferência",
                timestamp=base_time + timedelta(minutes=random.randint(0, window_minutes)),
                status=TransactionStatus.COMPLETED,
            )
            transactions.append(tx)

        return transactions

    def inject_amount_anomaly(
        self,
        base_transaction: Transaction,
        multiplier: float = 50.0,
    ) -> Transaction:
        """Generate transaction with anomalous amount."""
        return Transaction(
            transaction_id=f"fraud-amt-{base_transaction.transaction_id}",
            account_id=base_transaction.account_id,
            transaction_type=base_transaction.transaction_type,
            amount=base_transaction.amount * Decimal(str(multiplier)),
            direction=Direction.DEBIT,
            counterparty_key=base_transaction.counterparty_key,
            counterparty_name=base_transaction.counterparty_name,
            description=base_transaction.description,
            timestamp=base_transaction.timestamp,
            status=TransactionStatus.COMPLETED,
        )

    def inject_night_activity(
        self,
        base_transaction: Transaction,
    ) -> Transaction:
        """Generate transaction at unusual night hours."""
        night_hour = random.choice([1, 2, 3, 4, 5])
        night_time = base_transaction.timestamp.replace(
            hour=night_hour,
            minute=random.randint(0, 59),
        )

        return Transaction(
            transaction_id=f"fraud-night-{base_transaction.transaction_id}",
            account_id=base_transaction.account_id,
            transaction_type=TransactionType.WITHDRAW,
            amount=Decimal(str(round(random.uniform(1000, 5000), 2))),
            direction=Direction.DEBIT,
            counterparty_key=None,
            counterparty_name=None,
            description="Saque ATM - Horário Incomum",
            timestamp=night_time,
            status=TransactionStatus.COMPLETED,
        )

    def inject_new_payee_large_amount(
        self,
        base_transaction: Transaction,
    ) -> Transaction:
        """Generate large transaction to a new payee."""
        large_amount = random.uniform(5000, 50000)

        return Transaction(
            transaction_id=f"fraud-newpayee-{base_transaction.transaction_id}",
            account_id=base_transaction.account_id,
            transaction_type=TransactionType.PIX,
            amount=Decimal(str(round(large_amount, 2))),
            direction=Direction.DEBIT,
            counterparty_key=f"new-payee-{random.randint(10000, 99999)}",
            counterparty_name=f"Novo Destinatário {random.randint(1, 100)}",
            description="Pix - Primeiro Pagamento",
            timestamp=base_transaction.timestamp,
            status=TransactionStatus.COMPLETED,
        )

    def inject_round_amounts(
        self,
        base_transaction: Transaction,
        count: int = 3,
    ) -> list[Transaction]:
        """Generate transactions with suspiciously round amounts."""
        round_amounts = [1000, 2000, 5000, 10000, 20000]
        transactions = []

        for i in range(count):
            amount = random.choice(round_amounts)
            tx = Transaction(
                transaction_id=f"fraud-round-{base_transaction.transaction_id}-{i}",
                account_id=base_transaction.account_id,
                transaction_type=TransactionType.PIX,
                amount=Decimal(str(amount)),
                direction=Direction.DEBIT,
                counterparty_key=f"round-key-{i}",
                counterparty_name=f"Destinatário {i}",
                description="Pix - Valor Redondo",
                timestamp=base_transaction.timestamp + timedelta(hours=i),
                status=TransactionStatus.COMPLETED,
            )
            transactions.append(tx)

        return transactions
