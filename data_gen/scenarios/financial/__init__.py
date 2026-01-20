"""Financial scenarios for Brazilian banking data generation."""

from data_gen.scenarios.financial.customer_360 import Customer360Scenario
from data_gen.scenarios.financial.fraud_detection import FraudDetectionScenario
from data_gen.scenarios.financial.loan_portfolio import LoanPortfolioScenario

__all__ = [
    "FraudDetectionScenario",
    "LoanPortfolioScenario",
    "Customer360Scenario",
]
