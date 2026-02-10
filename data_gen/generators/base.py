"""Base generator class for all data generators."""

import random
from abc import ABC

from faker import Faker


class BaseGenerator(ABC):
    """Base class for all data generators.

    Provides common initialization: Faker instance creation and
    seed-based reproducibility for both Faker and random.
    """

    def __init__(self, seed: int | None = None, locale: str = "pt_BR") -> None:
        self.fake = Faker(locale)
        if seed is not None:
            self.fake.seed_instance(seed)
            random.seed(seed)
