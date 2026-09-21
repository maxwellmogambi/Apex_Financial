from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42

    n_customers: int = 3_000
    n_merchants: int = 1_500
    n_transactions: int = 10_000

    output_dir: Path = Path("data/synthetic")


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

class FinancialTransactionGenerator:
    """
    Generates a synthetic financial transaction dataset based on the
    statistical and relational contract established from IEEE-CIS.

    Generation order:
        customers
        -> cards
        -> devices
        -> merchants
        -> transactions
    """

    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.rng = np.random.default_rng(config.seed)

        self.customers: pd.DataFrame | None = None
        self.cards: pd.DataFrame | None = None
        self.devices: pd.DataFrame | None = None
        self.merchants: pd.DataFrame | None = None
        self.transactions: pd.DataFrame | None = None

    # -----------------------------------------------------------------------
    # Customers
    # -----------------------------------------------------------------------

    def generate_customers(self) -> pd.DataFrame:
        n = self.config.n_customers

        customer_ids = [f"C{i:06d}" for i in range(1, n + 1)]

        customer_segment = self.rng.choice(
            ["standard", "premium", "high_value"],
            size=n,
            p=[0.70, 0.25, 0.05],
        )

        home_region = self.rng.choice(
            ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret", "Other"],
            size=n,
            p=[0.35, 0.15, 0.10, 0.10, 0.08, 0.22],
        )

        customer_risk_profile = self.rng.choice(
            ["low", "medium", "high"],
            size=n,
            p=[0.70, 0.25, 0.05],
        )

        self.customers = pd.DataFrame(
            {
                "customer_id": customer_ids,
                "customer_segment": customer_segment,
                "home_region": home_region,
                "customer_risk_profile": customer_risk_profile,
            }
        )

        return self.customers

    # -----------------------------------------------------------------------
    # Cards
    # -----------------------------------------------------------------------

    def generate_cards(self) -> pd.DataFrame:
        if self.customers is None:
            raise RuntimeError("Generate customers before cards.")

        customers = self.customers

        # Most customers have one card; some have two or three.
        cards_per_customer = self.rng.choice(
            [1, 2, 3],
            size=len(customers),
            p=[0.72, 0.23, 0.05],
        )

        customer_ids = np.repeat(
            customers["customer_id"].to_numpy(),
            cards_per_customer,
        )

        n_cards = len(customer_ids)

        card_ids = [f"CARD{i:07d}" for i in range(1, n_cards + 1)]

        card_type = self.rng.choice(
            ["visa", "mastercard", "american_express", "discover"],
            size=n_cards,
            p=[0.65, 0.32, 0.015, 0.015],
        )

        payment_type = self.rng.choice(
            ["debit", "credit"],
            size=n_cards,
            p=[0.75, 0.25],
        )

        self.cards = pd.DataFrame(
            {
                "card_id": card_ids,
                "customer_id": customer_ids,
                "card_type": card_type,
                "payment_type": payment_type,
            }
        )

        return self.cards

    # -----------------------------------------------------------------------
    # Devices
    # -----------------------------------------------------------------------

    def generate_devices(self) -> pd.DataFrame:
        if self.customers is None:
            raise RuntimeError("Generate customers before devices.")

        customers = self.customers

        devices_per_customer = self.rng.choice(
            [1, 2, 3],
            size=len(customers),
            p=[0.78, 0.18, 0.04],
        )

        customer_ids = np.repeat(
            customers["customer_id"].to_numpy(),
            devices_per_customer,
        )

        n_devices = len(customer_ids)

        device_ids = [f"D{i:07d}" for i in range(1, n_devices + 1)]

        device_type = self.rng.choice(
            ["mobile", "desktop", "tablet"],
            size=n_devices,
            p=[0.65, 0.30, 0.05],
        )

        self.devices = pd.DataFrame(
            {
                "device_id": device_ids,
                "customer_id": customer_ids,
                "device_type": device_type,
            }
        )

        return self.devices

    # -----------------------------------------------------------------------
    # Merchants
    # -----------------------------------------------------------------------

    def generate_merchants(self) -> pd.DataFrame:
        n = self.config.n_merchants

        merchant_ids = [f"M{i:06d}" for i in range(1, n + 1)]

        merchant_category = self.rng.choice(
            [
                "retail",
                "grocery",
                "electronics",
                "travel",
                "food",
                "services",
            ],
            size=n,
            p=[0.25, 0.20, 0.15, 0.10, 0.20, 0.10],
        )

        merchant_region = self.rng.choice(
            ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret", "Other"],
            size=n,
            p=[0.35, 0.15, 0.10, 0.10, 0.08, 0.22],
        )

        merchant_volume_profile = self.rng.choice(
            ["normal", "medium", "high"],
            size=n,
            p=[0.85, 0.10, 0.05],
        )

        self.merchants = pd.DataFrame(
            {
                "merchant_id": merchant_ids,
                "merchant_category": merchant_category,
                "merchant_region": merchant_region,
                "merchant_volume_profile": merchant_volume_profile,
            }
        )

        return self.merchants

    # -----------------------------------------------------------------------
    # Transactions
    # -----------------------------------------------------------------------

    def generate_transactions(self) -> pd.DataFrame:
        if any(
            entity is None
            for entity in [
                self.customers,
                self.cards,
                self.devices,
                self.merchants,
            ]
        ):
            raise RuntimeError(
                "Generate customers, cards, devices, and merchants "
                "before transactions."
            )

        n = self.config.n_transactions

        customers = self.customers
        cards = self.cards
        devices = self.devices
        merchants = self.merchants

        # -------------------------------------------------------------------
        # Customer activity
        #
        # High-value customers receive more transactions, creating natural
        # frequency skew that we can later investigate with Spark.
        # -------------------------------------------------------------------

        customer_weights = customers["customer_segment"].map(
            {
                "standard": 1.0,
                "premium": 2.5,
                "high_value": 6.0,
            }
        ).to_numpy()

        customer_weights = customer_weights / customer_weights.sum()

        customer_indices = self.rng.choice(
            len(customers),
            size=n,
            p=customer_weights,
        )

        customer_ids = customers.iloc[customer_indices]["customer_id"].to_numpy()

        # -------------------------------------------------------------------
        # Select cards and devices belonging to each customer.
        # -------------------------------------------------------------------

        card_by_customer = (
            cards.groupby("customer_id")["card_id"]
            .apply(list)
            .to_dict()
        )

        device_by_customer = (
            devices.groupby("customer_id")["device_id"]
            .apply(list)
            .to_dict()
        )

        selected_cards = np.array(
            [
                self.rng.choice(card_by_customer[customer_id])
                for customer_id in customer_ids
            ]
        )

        selected_devices = np.array(
            [
                self.rng.choice(device_by_customer[customer_id])
                for customer_id in customer_ids
            ]
        )

        # -------------------------------------------------------------------
        # Merchants
        #
        # Weight merchant selection so high-volume merchants naturally
        # receive more transactions.
        # -------------------------------------------------------------------

        merchant_weights = merchants["merchant_volume_profile"].map(
            {
                "normal": 1.0,
                "medium": 3.0,
                "high": 10.0,
            }
        ).to_numpy()

        merchant_weights = merchant_weights / merchant_weights.sum()

        merchant_indices = self.rng.choice(
            len(merchants),
            size=n,
            p=merchant_weights,
        )

        selected_merchants = merchants.iloc[merchant_indices]

        merchant_ids = selected_merchants["merchant_id"].to_numpy()
        merchant_regions = selected_merchants["merchant_region"].to_numpy()

        # -------------------------------------------------------------------
        # Card/payment attributes
        # -------------------------------------------------------------------

        selected_card_details = (
            cards.set_index("card_id")
            .loc[selected_cards]
            .reset_index()
        )

        card_types = selected_card_details["card_type"].to_numpy()
        payment_types = selected_card_details["payment_type"].to_numpy()

        # -------------------------------------------------------------------
        # Product type
        #
        # Approximate IEEE-CIS ProductCD distribution.
        # -------------------------------------------------------------------

        product_type = self.rng.choice(
            ["W", "C", "R", "H", "S"],
            size=n,
            p=[0.744, 0.116, 0.064, 0.056, 0.020],
        )

        # -------------------------------------------------------------------
        # Transaction amounts
        #
        # Log-normal distribution produces the right-skewed behaviour
        # observed in IEEE-CIS.
        # -------------------------------------------------------------------

        amount = self.rng.lognormal(
            mean=np.log(68.77),
            sigma=1.15,
            size=n,
        )

        amount = np.clip(amount, 0.25, 10_000)

        # -------------------------------------------------------------------
        # Timestamps
        #
        # Approximately six months of transaction activity.
        # -------------------------------------------------------------------

        start = pd.Timestamp("2026-01-01")

        seconds_in_period = 180 * 24 * 60 * 60

        elapsed_seconds = self.rng.integers(
            0,
            seconds_in_period,
            size=n,
        )

        timestamps = (
            start
            + pd.to_timedelta(elapsed_seconds, unit="s")
        )

        # -------------------------------------------------------------------
        # Identity availability
        #
        # Approximately 24.4%, matching IEEE-CIS.
        # -------------------------------------------------------------------

        has_identity = (
            self.rng.random(n) < 0.244
        )

        # -------------------------------------------------------------------
        # Customer information
        # -------------------------------------------------------------------

        customer_details = (
            customers.set_index("customer_id")
            .loc[customer_ids]
            .reset_index()
        )

        customer_regions = customer_details["home_region"].to_numpy()
        customer_risk = customer_details["customer_risk_profile"].to_numpy()

        # -------------------------------------------------------------------
        # Behavioural anomaly signals
        # -------------------------------------------------------------------

        geographic_mismatch = (
            customer_regions != merchant_regions
        )

        # High-risk customers are more likely to generate anomalies.
        risk_multiplier = np.select(
            [
                customer_risk == "high",
                customer_risk == "medium",
            ],
            [
                2.5,
                1.4,
            ],
            default=1.0,
        )

        # -------------------------------------------------------------------
        # Fraud probability
        #
        # This is intentionally conditional rather than random.
        # -------------------------------------------------------------------

        fraud_probability = np.full(n, 0.0105)

        # Product effect
        fraud_probability *= np.select(
            [
                product_type == "C",
                product_type == "R",
                product_type == "H",
                product_type == "S",
            ],
            [
                4.5,
                1.4,
                1.8,
                3.0,
            ],
            default=1.0,
        )

        # Payment effect
        fraud_probability *= np.where(
            payment_types == "credit",
            2.0,
            1.0,
        )

        # Identity availability
        fraud_probability *= np.where(
            has_identity,
            2.8,
            1.0,
        )

        # Customer risk
        fraud_probability *= risk_multiplier

        # Geographic mismatch
        fraud_probability *= np.where(
            geographic_mismatch,
            1.25,
            1.0,
        )

        # Large transaction effect
        fraud_probability *= np.where(
            amount > 500,
            1.20,
            1.0,
        )

        fraud_probability = np.clip(
            fraud_probability,
            0,
            0.95,
        )

        is_fraud = (
            self.rng.random(n) < fraud_probability
        ).astype(int)

        # -------------------------------------------------------------------
        # Build transaction DataFrame
        # -------------------------------------------------------------------

        self.transactions = pd.DataFrame(
            {
                "transaction_id": [
                    f"T{i:08d}" for i in range(1, n + 1)
                ],
                "customer_id": customer_ids,
                "card_id": selected_cards,
                "merchant_id": merchant_ids,
                "device_id": selected_devices,
                "timestamp": timestamps,
                "amount": np.round(amount, 2),
                "product_type": product_type,
                "card_type": card_types,
                "payment_type": payment_types,
                "customer_region": customer_regions,
                "merchant_region": merchant_regions,
                "has_identity": has_identity,
                "is_fraud": is_fraud,
            }
        )

        return self.transactions

    # -----------------------------------------------------------------------
    # Validation
    # -----------------------------------------------------------------------

    def validate(self) -> None:
        """Validate basic relational and data-quality constraints."""

        if self.transactions is None:
            raise RuntimeError("Generate transactions before validation.")

        transactions = self.transactions

        assert transactions["transaction_id"].is_unique
        assert transactions["customer_id"].isin(
            self.customers["customer_id"]
        ).all()
        assert transactions["card_id"].isin(
            self.cards["card_id"]
        ).all()
        assert transactions["device_id"].isin(
            self.devices["device_id"]
        ).all()
        assert transactions["merchant_id"].isin(
            self.merchants["merchant_id"]
        ).all()

        assert transactions["amount"].gt(0).all()
        assert transactions["is_fraud"].isin([0, 1]).all()

        print("Validation passed.")
        print(f"Customers:    {len(self.customers):,}")
        print(f"Cards:        {len(self.cards):,}")
        print(f"Devices:      {len(self.devices):,}")
        print(f"Merchants:    {len(self.merchants):,}")
        print(f"Transactions: {len(transactions):,}")
        print(
            f"Fraud rate:   "
            f"{transactions['is_fraud'].mean():.2%}"
        )

    # -----------------------------------------------------------------------
    # Save
    # -----------------------------------------------------------------------

    def save(self) -> None:
        if any(
            entity is None
            for entity in [
                self.customers,
                self.cards,
                self.devices,
                self.merchants,
                self.transactions,
            ]
        ):
            raise RuntimeError("Generate all datasets before saving.")

        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        self.customers.to_parquet(
            output_dir / "customers.parquet",
            index=False,
        )

        self.cards.to_parquet(
            output_dir / "cards.parquet",
            index=False,
        )

        self.devices.to_parquet(
            output_dir / "devices.parquet",
            index=False,
        )

        self.merchants.to_parquet(
            output_dir / "merchants.parquet",
            index=False,
        )

        self.transactions.to_parquet(
            output_dir / "transactions.parquet",
            index=False,
        )

        print(f"Saved synthetic data to: {output_dir.resolve()}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    config = GeneratorConfig(
        n_customers=3_000,
        n_merchants=1_500,
        n_transactions=10_000,
    )

    generator = FinancialTransactionGenerator(config)

    generator.generate_customers()
    generator.generate_cards()
    generator.generate_devices()
    generator.generate_merchants()
    generator.generate_transactions()

    generator.validate()
    generator.save()


if __name__ == "__main__":
    main()