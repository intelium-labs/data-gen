"""Address generation factory with worldwide locale support."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

from faker import Faker

from data_gen.models.base import Address

if TYPE_CHECKING:
    from data_gen.generators.pool import FakerPool


@dataclass(frozen=True)
class CountryDistribution:
    """Weighted distribution of countries for address generation.

    Parameters
    ----------
    weights : dict[str, float]
        Mapping of ISO 3166-1 alpha-2 country code to weight.
        Weights are relative (do not need to sum to 1.0).
    """

    weights: dict[str, float] = field(default_factory=lambda: {"BR": 1.0})

    @classmethod
    def brazil_dominant(cls) -> "CountryDistribution":
        """Default: 70% Brazil, 30% spread across 9 other countries."""
        return cls(
            weights={
                "BR": 0.70,
                "US": 0.10,
                "GB": 0.04,
                "DE": 0.03,
                "FR": 0.03,
                "ES": 0.02,
                "JP": 0.02,
                "MX": 0.02,
                "AR": 0.02,
                "PT": 0.02,
            }
        )

    @classmethod
    def brazil_only(cls) -> "CountryDistribution":
        """100% Brazilian addresses."""
        return cls(weights={"BR": 1.0})


# Mapping of ISO country code -> Faker locale
LOCALE_MAP: dict[str, str] = {
    "BR": "pt_BR",
    "US": "en_US",
    "GB": "en_GB",
    "DE": "de_DE",
    "FR": "fr_FR",
    "ES": "es",
    "JP": "ja_JP",
    "MX": "es_MX",
    "AR": "es_AR",
    "PT": "pt_PT",
}


class AddressFactory:
    """Generate realistic addresses for multiple countries.

    Uses Faker with locale-specific providers. Each configured country
    gets a dedicated Faker instance to ensure realistic local addresses.

    When a ``FakerPool`` is provided, Brazilian addresses use pre-generated
    pools for 3-5x faster generation.

    Parameters
    ----------
    distribution : CountryDistribution | None
        Country weight distribution. Defaults to ``brazil_dominant()``.
    seed : int | None
        Random seed for reproducibility.
    pool : FakerPool | None
        Pre-generated value pool for fast Brazilian address generation.
    """

    def __init__(
        self,
        distribution: CountryDistribution | None = None,
        seed: int | None = None,
        pool: FakerPool | None = None,
    ) -> None:
        self._distribution = distribution or CountryDistribution.brazil_dominant()
        self._countries = list(self._distribution.weights.keys())
        self._weights = list(self._distribution.weights.values())
        self._pool = pool
        self._fakers: dict[str, Faker] = {}

        for country_code in self._countries:
            locale = LOCALE_MAP.get(country_code, "en_US")
            faker_instance = Faker(locale)
            if seed is not None:
                faker_instance.seed_instance(seed)
            self._fakers[country_code] = faker_instance

    def generate(self, country: str | None = None) -> Address:
        """Generate an address, optionally for a specific country.

        Parameters
        ----------
        country : str | None
            ISO 3166-1 alpha-2 code. If ``None``, picks based on
            the configured distribution.

        Returns
        -------
        Address
            Generated address.
        """
        if country is None:
            country = random.choices(self._countries, weights=self._weights, k=1)[0]

        # Use pool for Brazilian addresses (fast path)
        if country == "BR" and self._pool is not None:
            return _generate_br_pooled(self._pool)

        fake = self._fakers.get(country)
        if fake is None:
            # Country not in distribution — create a one-off Faker
            locale = LOCALE_MAP.get(country, "en_US")
            fake = Faker(locale)

        generator = _COUNTRY_GENERATORS.get(country, _generate_generic)
        return generator(fake, country)

    def generate_brazilian(self) -> Address:
        """Generate a Brazilian address (convenience shortcut)."""
        return self.generate(country="BR")


# ---------------------------------------------------------------------------
# Per-country address generators
# ---------------------------------------------------------------------------

def _generate_br_pooled(pool: FakerPool) -> Address:
    """Generate Brazilian address using pre-generated pools (fast path)."""
    return Address(
        street=pool.street(),
        number=str(random.randint(1, 9999)),
        neighborhood=pool.bairro(),
        city=pool.city(),
        state=pool.estado(),
        postal_code=pool.postcode(),
        complement=random.choice(["", "", "", f"Apto {random.randint(1, 500)}"]),
        country="BR",
    )


def _generate_br(fake: Faker, country: str) -> Address:
    """Generate Brazilian address using pt_BR-specific methods."""
    return Address(
        street=fake.street_name(),
        number=str(random.randint(1, 9999)),
        neighborhood=fake.bairro(),
        city=fake.city(),
        state=fake.estado_sigla(),
        postal_code=fake.postcode(),
        complement=random.choice(["", "", "", f"Apto {random.randint(1, 500)}"]),
        country="BR",
    )


def _generate_us(fake: Faker, country: str) -> Address:
    """Generate US address."""
    return Address(
        street=fake.street_name(),
        number=str(random.randint(1, 9999)),
        neighborhood=fake.city_suffix(),
        city=fake.city(),
        state=fake.state_abbr(),
        postal_code=fake.zipcode(),
        complement=random.choice(["", "", "", f"Apt {random.randint(1, 500)}"]),
        country="US",
    )


def _generate_gb(fake: Faker, country: str) -> Address:
    """Generate UK address."""
    return Address(
        street=fake.street_name(),
        number=str(random.randint(1, 999)),
        neighborhood=fake.city(),
        city=fake.city(),
        state=fake.county(),
        postal_code=fake.postcode(),
        complement=random.choice(["", "", f"Flat {random.randint(1, 50)}"]),
        country="GB",
    )


def _generate_de(fake: Faker, country: str) -> Address:
    """Generate German address."""
    return Address(
        street=fake.street_name(),
        number=str(random.randint(1, 200)),
        neighborhood=fake.city(),
        city=fake.city(),
        state=fake.state(),
        postal_code=fake.postcode(),
        country="DE",
    )


def _generate_fr(fake: Faker, country: str) -> Address:
    """Generate French address."""
    return Address(
        street=fake.street_name(),
        number=str(random.randint(1, 200)),
        neighborhood=fake.city(),
        city=fake.city(),
        state=fake.region(),
        postal_code=fake.postcode(),
        country="FR",
    )


def _generate_es(fake: Faker, country: str) -> Address:
    """Generate Spanish address."""
    return Address(
        street=fake.street_name(),
        number=str(random.randint(1, 200)),
        neighborhood=fake.city(),
        city=fake.city(),
        state=fake.state(),
        postal_code=fake.postcode(),
        country="ES",
    )


def _generate_jp(fake: Faker, country: str) -> Address:
    """Generate Japanese address."""
    return Address(
        street=fake.street_name(),
        number=str(random.randint(1, 50)),
        neighborhood=fake.city(),
        city=fake.city(),
        state=fake.prefecture(),
        postal_code=fake.postcode(),
        country="JP",
    )


def _generate_mx(fake: Faker, country: str) -> Address:
    """Generate Mexican address."""
    return Address(
        street=fake.street_name(),
        number=str(random.randint(1, 9999)),
        neighborhood=fake.city(),
        city=fake.city(),
        state=fake.state(),
        postal_code=fake.postcode(),
        complement=random.choice(["", "", "", f"Depto {random.randint(1, 300)}"]),
        country="MX",
    )


def _generate_ar(fake: Faker, country: str) -> Address:
    """Generate Argentine address."""
    return Address(
        street=fake.street_name(),
        number=str(random.randint(1, 9999)),
        neighborhood=fake.city(),
        city=fake.city(),
        state=fake.province(),
        postal_code=fake.postcode(),
        complement=random.choice(["", "", "", f"Piso {random.randint(1, 20)}"]),
        country="AR",
    )


def _generate_pt(fake: Faker, country: str) -> Address:
    """Generate Portuguese address."""
    return Address(
        street=fake.street_name(),
        number=str(random.randint(1, 999)),
        neighborhood=fake.city(),
        city=fake.city(),
        state=fake.distrito(),
        postal_code=fake.postcode(),
        country="PT",
    )


def _generate_generic(fake: Faker, country: str) -> Address:
    """Fallback generator using common Faker methods."""
    state = ""
    for method in ("state", "state_abbr", "province"):
        if hasattr(fake, method):
            state = getattr(fake, method)()
            break

    return Address(
        street=fake.street_name(),
        number=str(random.randint(1, 9999)),
        neighborhood=fake.city(),
        city=fake.city(),
        state=state,
        postal_code=fake.postcode(),
        country=country,
    )


_COUNTRY_GENERATORS: dict[str, Callable[[Faker, str], Address]] = {
    "BR": _generate_br,
    "US": _generate_us,
    "GB": _generate_gb,
    "DE": _generate_de,
    "FR": _generate_fr,
    "ES": _generate_es,
    "JP": _generate_jp,
    "MX": _generate_mx,
    "AR": _generate_ar,
    "PT": _generate_pt,
}
