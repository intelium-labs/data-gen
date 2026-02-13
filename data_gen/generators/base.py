"""Base generator class for all data generators."""

from __future__ import annotations

import random
from abc import ABC

from faker import Faker

from data_gen.generators.pool import FakerPool


class BaseGenerator(ABC):
    """Base class for all data generators.

    Provides common initialization: Faker instance creation,
    seed-based reproducibility, and optional FakerPool for
    high-throughput generation.

    Parameters
    ----------
    seed : int | None
        Random seed for reproducibility.
    locale : str
        Faker locale (default ``pt_BR``).
    pool : FakerPool | None
        Pre-generated value pool. When provided, generators use
        ``pool.name()`` etc. instead of ``fake.name()`` for 2-4x
        faster generation.
    """

    def __init__(
        self,
        seed: int | None = None,
        locale: str = "pt_BR",
        pool: FakerPool | None = None,
    ) -> None:
        self.fake = Faker(locale)
        self.pool = pool or FakerPool(locale=locale, seed=seed)
        if seed is not None:
            self.fake.seed_instance(seed)
            random.seed(seed)
