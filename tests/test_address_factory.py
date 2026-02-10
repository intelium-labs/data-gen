"""Tests for AddressFactory and CountryDistribution."""

from collections import Counter

import pytest

from data_gen.generators.address import (
    LOCALE_MAP,
    AddressFactory,
    CountryDistribution,
    _COUNTRY_GENERATORS,
)
from data_gen.models.base import Address


class TestCountryDistribution:
    """Tests for CountryDistribution."""

    def test_brazil_dominant_weights(self) -> None:
        """Default distribution has 70% Brazil and sums to ~1.0."""
        dist = CountryDistribution.brazil_dominant()
        assert dist.weights["BR"] == 0.70
        assert abs(sum(dist.weights.values()) - 1.0) < 1e-9
        assert len(dist.weights) == 10

    def test_brazil_only(self) -> None:
        """brazil_only produces 100% BR."""
        dist = CountryDistribution.brazil_only()
        assert dist.weights == {"BR": 1.0}

    def test_custom_weights(self) -> None:
        """Custom weights are accepted."""
        dist = CountryDistribution(weights={"BR": 0.5, "US": 0.5})
        assert dist.weights["BR"] == 0.5
        assert dist.weights["US"] == 0.5

    def test_frozen(self) -> None:
        """CountryDistribution is immutable."""
        dist = CountryDistribution.brazil_only()
        with pytest.raises(AttributeError):
            dist.weights = {"US": 1.0}  # type: ignore[misc]


class TestAddressFactory:
    """Tests for AddressFactory."""

    def test_generate_returns_address(self) -> None:
        """generate() returns an Address instance."""
        factory = AddressFactory(seed=42)
        address = factory.generate()
        assert isinstance(address, Address)

    def test_generate_brazilian_address(self) -> None:
        """Brazilian address has correct country and populated fields."""
        factory = AddressFactory(
            distribution=CountryDistribution.brazil_only(),
            seed=42,
        )
        address = factory.generate()
        assert address.country == "BR"
        assert address.street
        assert address.city
        assert address.state
        assert address.postal_code

    def test_generate_specific_country(self) -> None:
        """country= parameter overrides distribution."""
        factory = AddressFactory(
            distribution=CountryDistribution.brazil_only(),
            seed=42,
        )
        address = factory.generate(country="US")
        assert address.country == "US"

    def test_generate_brazilian_shortcut(self) -> None:
        """generate_brazilian() always returns BR address."""
        factory = AddressFactory(
            distribution=CountryDistribution.brazil_dominant(),
            seed=42,
        )
        address = factory.generate_brazilian()
        assert address.country == "BR"

    def test_seed_reproducibility(self) -> None:
        """Same seed produces same addresses."""
        factory1 = AddressFactory(
            distribution=CountryDistribution.brazil_only(),
            seed=42,
        )
        factory2 = AddressFactory(
            distribution=CountryDistribution.brazil_only(),
            seed=42,
        )
        addr1 = factory1.generate()
        addr2 = factory2.generate()
        assert addr1.street == addr2.street
        assert addr1.city == addr2.city

    def test_all_configured_countries_produce_valid_address(self) -> None:
        """Every country in LOCALE_MAP produces a valid Address."""
        factory = AddressFactory(
            distribution=CountryDistribution(
                weights={code: 1.0 for code in LOCALE_MAP}
            ),
            seed=42,
        )
        for country_code in LOCALE_MAP:
            address = factory.generate(country=country_code)
            assert isinstance(address, Address)
            assert address.country == country_code
            assert address.street
            assert address.city

    def test_distribution_affects_country_mix(self) -> None:
        """Distribution weights affect the generated country mix."""
        factory = AddressFactory(
            distribution=CountryDistribution(weights={"BR": 0.5, "US": 0.5}),
            seed=42,
        )
        countries = Counter()
        for _ in range(200):
            address = factory.generate()
            countries[address.country] += 1

        # Both should appear (statistically certain with 200 samples)
        assert countries["BR"] > 0
        assert countries["US"] > 0

    def test_brazil_only_produces_only_br(self) -> None:
        """brazil_only distribution produces 100% BR addresses."""
        factory = AddressFactory(
            distribution=CountryDistribution.brazil_only(),
            seed=42,
        )
        for _ in range(50):
            assert factory.generate().country == "BR"

    def test_unknown_country_uses_generic(self) -> None:
        """Country not in LOCALE_MAP uses generic generator."""
        factory = AddressFactory(seed=42)
        address = factory.generate(country="ZZ")
        assert isinstance(address, Address)
        assert address.country == "ZZ"


class TestCountryGenerators:
    """Tests for individual country generators."""

    def test_us_address_has_state_abbr(self) -> None:
        """US address state is a short abbreviation."""
        factory = AddressFactory(seed=42)
        address = factory.generate(country="US")
        assert address.country == "US"
        assert len(address.state) == 2

    def test_gb_address(self) -> None:
        """UK address has correct country code."""
        factory = AddressFactory(seed=42)
        address = factory.generate(country="GB")
        assert address.country == "GB"
        assert address.state  # county

    def test_de_address(self) -> None:
        """German address has correct country code."""
        factory = AddressFactory(seed=42)
        address = factory.generate(country="DE")
        assert address.country == "DE"

    def test_fr_address(self) -> None:
        """French address has correct country code."""
        factory = AddressFactory(seed=42)
        address = factory.generate(country="FR")
        assert address.country == "FR"

    def test_jp_address(self) -> None:
        """Japanese address has correct country code."""
        factory = AddressFactory(seed=42)
        address = factory.generate(country="JP")
        assert address.country == "JP"

    def test_mx_address(self) -> None:
        """Mexican address has correct country code."""
        factory = AddressFactory(seed=42)
        address = factory.generate(country="MX")
        assert address.country == "MX"

    def test_ar_address(self) -> None:
        """Argentine address has correct country code."""
        factory = AddressFactory(seed=42)
        address = factory.generate(country="AR")
        assert address.country == "AR"

    def test_pt_address(self) -> None:
        """Portuguese address has correct country code."""
        factory = AddressFactory(seed=42)
        address = factory.generate(country="PT")
        assert address.country == "PT"

    def test_es_address(self) -> None:
        """Spanish address has correct country code."""
        factory = AddressFactory(seed=42)
        address = factory.generate(country="ES")
        assert address.country == "ES"

    def test_all_generators_registered(self) -> None:
        """All LOCALE_MAP countries have registered generators."""
        for code in LOCALE_MAP:
            assert code in _COUNTRY_GENERATORS, f"Missing generator for {code}"
