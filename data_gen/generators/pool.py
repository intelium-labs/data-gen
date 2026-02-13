"""Pre-generated value pools for fast data generation.

Replaces per-call Faker invocations with O(1) random.choice() lookups
from pre-populated pools.  Typical speedup: 2-4x for generators that
are heavy on Faker string methods (name, city, email, etc.).

Usage::

    pool = FakerPool(seed=42)
    name = pool.name()          # random.choice from 5 000 names
    uid  = pool.uuid()          # batch-generated via os.urandom
    cpf  = pool.cpf()           # pre-validated CPF from pool
"""

from __future__ import annotations

import os
import random
import uuid as _uuid
from typing import Any

from faker import Faker


class UUIDPool:
    """Batch-generated UUIDs using os.urandom for minimal syscall overhead.

    Instead of calling ``uuid.uuid4()`` per entity (≈1.8 µs each),
    this pre-generates batches of 8 192 UUIDs at once by reading
    ``os.urandom(16 * batch_size)`` and slicing into UUID hex strings.

    Parameters
    ----------
    batch_size : int
        Number of UUIDs to generate per batch (default 8192).
    """

    __slots__ = ("_batch_size", "_pool", "_index")

    def __init__(self, batch_size: int = 8192) -> None:
        self._batch_size = batch_size
        self._pool: list[str] = []
        self._index = 0
        self._refill()

    def _refill(self) -> None:
        """Generate a new batch of UUIDs."""
        raw = os.urandom(16 * self._batch_size)
        self._pool = [
            _uuid.UUID(bytes=raw[i : i + 16], version=4).hex
            for i in range(0, len(raw), 16)
        ]
        self._index = 0

    def next(self) -> str:
        """Return next UUID hex string, refilling pool when exhausted."""
        if self._index >= len(self._pool):
            self._refill()
        val = self._pool[self._index]
        self._index += 1
        return val


def _generate_cpf() -> str:
    """Generate a valid Brazilian CPF (11 digits) using pure arithmetic."""
    digits = [random.randint(0, 9) for _ in range(9)]
    # First check digit
    total = sum(d * w for d, w in zip(digits, range(10, 1, -1)))
    d1 = 11 - (total % 11)
    digits.append(0 if d1 >= 10 else d1)
    # Second check digit
    total = sum(d * w for d, w in zip(digits, range(11, 1, -1)))
    d2 = 11 - (total % 11)
    digits.append(0 if d2 >= 10 else d2)
    return "".join(str(d) for d in digits)


def _generate_cpf_formatted() -> str:
    """Generate a formatted CPF (XXX.XXX.XXX-XX)."""
    raw = _generate_cpf()
    return f"{raw[:3]}.{raw[3:6]}.{raw[6:9]}-{raw[9:]}"


def _generate_cnpj() -> str:
    """Generate a valid Brazilian CNPJ (14 digits) using pure arithmetic."""
    digits = [random.randint(0, 9) for _ in range(8)] + [0, 0, 0, 1]
    # First check digit
    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    total = sum(d * w for d, w in zip(digits, weights1))
    d1 = 11 - (total % 11)
    digits.append(0 if d1 >= 10 else d1)
    # Second check digit
    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    total = sum(d * w for d, w in zip(digits, weights2))
    d2 = 11 - (total % 11)
    digits.append(0 if d2 >= 10 else d2)
    return "".join(str(d) for d in digits)


def _generate_cnpj_formatted() -> str:
    """Generate a formatted CNPJ (XX.XXX.XXX/XXXX-XX)."""
    raw = _generate_cnpj()
    return f"{raw[:2]}.{raw[2:5]}.{raw[5:8]}/{raw[8:12]}-{raw[12:]}"


class FakerPool:
    """Pre-generated pools of Faker values for fast random selection.

    All pools are populated once at construction from a Faker instance,
    then sampled via ``random.choice()`` (≈0.2 µs) instead of calling
    Faker methods (2-7 µs each).

    Parameters
    ----------
    locale : str
        Faker locale (default ``pt_BR``).
    seed : int | None
        Random seed for reproducibility.
    pool_sizes : dict[str, int] | None
        Override default pool sizes per field.
    """

    # Default pool sizes — balance between variety and startup cost
    DEFAULT_SIZES: dict[str, int] = {
        "name": 5000,
        "last_name": 1000,
        "first_name": 1000,
        "city": 500,
        "street": 1000,
        "bairro": 300,
        "postcode": 500,
        "estado": 27,
        "company": 500,
        "email_prefix": 2000,
        "phone": 2000,
        "msisdn": 2000,
        "cpf": 10000,
        "cnpj": 5000,
    }

    EMAIL_DOMAINS = [
        "gmail.com",
        "hotmail.com",
        "outlook.com",
        "yahoo.com.br",
        "uol.com.br",
        "terra.com.br",
        "bol.com.br",
        "ig.com.br",
        "live.com",
        "icloud.com",
    ]

    def __init__(
        self,
        locale: str = "pt_BR",
        seed: int | None = None,
        pool_sizes: dict[str, int] | None = None,
    ) -> None:
        sizes = {**self.DEFAULT_SIZES, **(pool_sizes or {})}
        fake = Faker(locale)
        if seed is not None:
            fake.seed_instance(seed)
            random.seed(seed)

        # Pre-generate all pools
        self._names: list[str] = [fake.name() for _ in range(sizes["name"])]
        self._last_names: list[str] = [fake.last_name() for _ in range(sizes["last_name"])]
        self._first_names: list[str] = [fake.first_name() for _ in range(sizes["first_name"])]
        self._cities: list[str] = [fake.city() for _ in range(sizes["city"])]
        self._streets: list[str] = [fake.street_name() for _ in range(sizes["street"])]
        self._bairros: list[str] = [fake.bairro() for _ in range(sizes["bairro"])]
        self._postcodes: list[str] = [fake.postcode() for _ in range(sizes["postcode"])]

        # All 27 Brazilian states
        self._estados: list[str] = list({fake.estado_sigla() for _ in range(200)})

        self._companies: list[str] = [fake.company() for _ in range(sizes["company"])]

        # Email: pre-generate prefixes, combine with random domain at call time
        self._email_prefixes: list[str] = [
            fake.user_name() for _ in range(sizes["email_prefix"])
        ]

        # Phone: use f-string template instead of Faker
        self._phones: list[str] = [fake.cellphone_number() for _ in range(sizes["phone"])]
        self._msisdns: list[str] = [fake.msisdn() for _ in range(sizes["msisdn"])]

        # CPF/CNPJ: use fast arithmetic generators
        self._cpfs: list[str] = [_generate_cpf_formatted() for _ in range(sizes["cpf"])]
        self._cpfs_raw: list[str] = [_generate_cpf() for _ in range(sizes["cpf"])]
        self._cnpjs: list[str] = [_generate_cnpj_formatted() for _ in range(sizes["cnpj"])]
        self._cnpjs_raw: list[str] = [_generate_cnpj() for _ in range(sizes["cnpj"])]

        # UUID pool (batch os.urandom)
        self._uuid_pool = UUIDPool()

    # --- Public accessors (O(1) random.choice) ---

    def uuid(self) -> str:
        """Return a unique UUID4 hex string."""
        return self._uuid_pool.next()

    def name(self) -> str:
        """Return a random full name."""
        return random.choice(self._names)

    def last_name(self) -> str:
        """Return a random last name."""
        return random.choice(self._last_names)

    def first_name(self) -> str:
        """Return a random first name."""
        return random.choice(self._first_names)

    def city(self) -> str:
        """Return a random city name."""
        return random.choice(self._cities)

    def street(self) -> str:
        """Return a random street name."""
        return random.choice(self._streets)

    def bairro(self) -> str:
        """Return a random neighborhood (bairro)."""
        return random.choice(self._bairros)

    def postcode(self) -> str:
        """Return a random postal code."""
        return random.choice(self._postcodes)

    def estado(self) -> str:
        """Return a random Brazilian state abbreviation."""
        return random.choice(self._estados)

    def company(self) -> str:
        """Return a random company name."""
        return random.choice(self._companies)

    def email(self) -> str:
        """Return a random email address."""
        prefix = random.choice(self._email_prefixes)
        domain = random.choice(self.EMAIL_DOMAINS)
        return f"{prefix}@{domain}"

    def phone(self) -> str:
        """Return a random Brazilian cellphone number."""
        return random.choice(self._phones)

    def msisdn(self) -> str:
        """Return a random MSISDN."""
        return random.choice(self._msisdns)

    def cpf(self) -> str:
        """Return a random formatted CPF (XXX.XXX.XXX-XX)."""
        return random.choice(self._cpfs)

    def cpf_raw(self) -> str:
        """Return a random unformatted CPF (11 digits)."""
        return random.choice(self._cpfs_raw)

    def cnpj(self) -> str:
        """Return a random formatted CNPJ."""
        return random.choice(self._cnpjs)

    def cnpj_raw(self) -> str:
        """Return a random unformatted CNPJ (14 digits)."""
        return random.choice(self._cnpjs_raw)
