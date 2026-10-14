"""
Synthetic e-commerce data generator.

Loads one day of data into the `raw` schema of the warehouse:
  raw.customers, raw.products, raw.orders, raw.order_items

Design choices that make the pipeline realistic:
  * Idempotent: re-running a date deletes and reloads that date's orders,
    so Airflow retries and backfills never create duplicates.
  * Slowly changing data: products get repriced and customers change loyalty tier,
    which the dbt snapshot captures as history (SCD Type 2).
  * Messy data on purpose: inconsistent email casing/whitespace, missing channels,
    so the dbt staging layer has real cleaning to do.

Usage:
  python generate_data.py --date 2026-10-01
"""

import argparse
import os
import random
from datetime import date, datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values

FIRST_NAMES = ["Aarav", "Olivia", "Liam", "Priya", "Noah", "Emma", "Mateo", "Sofia",
               "Ethan", "Ava", "Arjun", "Mia", "Lucas", "Zara", "Owen", "Chloe",
               "Ravi", "Isla", "Leo", "Nora"]
LAST_NAMES = ["Smith", "Patel", "Brown", "Nguyen", "Garcia", "Singh", "Wilson", "Khan",
              "Martin", "Lee", "Tremblay", "Roy", "Clark", "Lopez", "Walker", "Shah"]
CITIES = [("Toronto", "CA"), ("Windsor", "CA"), ("Vancouver", "CA"), ("Montreal", "CA"),
          ("Calgary", "CA"), ("Detroit", "US"), ("Chicago", "US"), ("New York", "US"),
          ("Seattle", "US"), ("Austin", "US")]
CATEGORIES = {
    "Electronics": (2500, 90000),
    "Home": (800, 25000),
    "Apparel": (900, 12000),
    "Books": (500, 4500),
    "Sports": (1500, 30000),
    "Beauty": (600, 8000),
}
ADJECTIVES = ["Classic", "Ultra", "Eco", "Smart", "Pro", "Compact", "Deluxe", "Everyday"]
NOUNS = {
    "Electronics": ["Headphones", "Speaker", "Monitor", "Keyboard", "Charger"],
    "Home": ["Lamp", "Blender", "Cookware Set", "Rug", "Kettle"],
    "Apparel": ["Hoodie", "Jacket", "Sneakers", "T-Shirt", "Jeans"],
    "Books": ["Cookbook", "Novel", "Guidebook", "Journal", "Workbook"],
    "Sports": ["Yoga Mat", "Dumbbells", "Bike Helmet", "Water Bottle", "Backpack"],
    "Beauty": ["Moisturizer", "Shampoo", "Sunscreen", "Face Wash", "Serum"],
}
TIERS = ["bronze", "silver", "gold", "platinum"]
CHANNELS = ["web", "mobile_app", "marketplace"]
EPOCH = date(2020, 1, 1)

DDL = """
create schema if not exists raw;

create table if not exists raw.customers (
    customer_id   bigint primary key,
    first_name    text,
    last_name     text,
    email         text,
    city          text,
    country       text,
    signup_date   date,
    loyalty_tier  text,
    updated_at    timestamp
);

create table if not exists raw.products (
    product_id        integer primary key,
    product_name      text,
    category          text,
    unit_price_cents  integer,
    updated_at        timestamp
);

create table if not exists raw.orders (
    order_id     text primary key,
    customer_id  bigint,
    order_ts     timestamp,
    status       text,
    channel      text,
    _loaded_at   timestamp default now()
);

create table if not exists raw.order_items (
    order_item_id     text primary key,
    order_id          text,
    product_id        integer,
    quantity          integer,
    unit_price_cents  integer,
    discount_pct      numeric(4, 2)
);
"""


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DBT_HOST", "localhost"),
        port=int(os.getenv("DBT_PORT", "5432")),
        user=os.getenv("DBT_USER", "postgres"),
        password=os.getenv("DBT_PASSWORD", "postgres"),
        dbname=os.getenv("DBT_DBNAME", "warehouse"),
    )


def messy_email(first, last, rng):
    """Emails arrive with inconsistent casing and stray whitespace."""
    email = f"{first}.{last}{rng.randint(1, 999)}@example.com"
    style = rng.random()
    if style < 0.15:
        return email.upper()
    if style < 0.25:
        return f"  {email} "
    return email.lower()


def make_customer(customer_id, signup_date, rng):
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    city, country = rng.choice(CITIES)
    return (customer_id, first, last, messy_email(first, last, rng), city, country,
            signup_date, "bronze", datetime.combine(signup_date, datetime.min.time()))


def seed_reference_data(cur, run_date, rng):
    """Create the starting catalogue and customer base on the very first run."""
    cur.execute("select count(*) from raw.products")
    if cur.fetchone()[0] > 0:
        return

    products = []
    for pid in range(1, 61):
        category = rng.choice(list(CATEGORIES))
        low, high = CATEGORIES[category]
        name = f"{rng.choice(ADJECTIVES)} {rng.choice(NOUNS[category])}"
        products.append((pid, name, category, rng.randint(low, high),
                         datetime.combine(run_date, datetime.min.time())))
    execute_values(cur, "insert into raw.products values %s on conflict do nothing", products)

    customers = [make_customer(cid, run_date - timedelta(days=rng.randint(30, 900)), rng)
                 for cid in range(1, 301)]
    execute_values(cur, "insert into raw.customers values %s on conflict do nothing", customers)


def apply_daily_changes(cur, run_date, rng):
    """New sign-ups, price changes and tier upgrades: the slowly changing parts."""
    ts = datetime.combine(run_date, datetime.min.time()) + timedelta(hours=12)

    base_id = 100_000 + (run_date - EPOCH).days * 100
    new_customers = [make_customer(base_id + i, run_date, rng) for i in range(rng.randint(3, 10))]
    execute_values(cur, "insert into raw.customers values %s on conflict do nothing", new_customers)

    cur.execute("select product_id, unit_price_cents from raw.products")
    products = cur.fetchall()
    for product_id, price in rng.sample(products, k=rng.randint(2, 5)):
        new_price = max(299, int(price * rng.uniform(0.85, 1.15)))
        cur.execute(
            "update raw.products set unit_price_cents = %s, updated_at = %s "
            "where product_id = %s and updated_at < %s",
            (new_price, ts, product_id, ts),
        )

    cur.execute("select customer_id, loyalty_tier from raw.customers")
    customers = cur.fetchall()
    for customer_id, tier in rng.sample(customers, k=min(len(customers), rng.randint(2, 6))):
        if tier != "platinum":
            cur.execute(
                "update raw.customers set loyalty_tier = %s, updated_at = %s "
                "where customer_id = %s and updated_at < %s",
                (TIERS[TIERS.index(tier) + 1], ts, customer_id, ts),
            )


def generate_orders(cur, run_date, rng):
    """Replace this date's orders so reruns and backfills are safe."""
    day_start = datetime.combine(run_date, datetime.min.time())
    cur.execute(
        "delete from raw.order_items where order_id in "
        "(select order_id from raw.orders where order_ts >= %s and order_ts < %s)",
        (day_start, day_start + timedelta(days=1)),
    )
    cur.execute("delete from raw.orders where order_ts >= %s and order_ts < %s",
                (day_start, day_start + timedelta(days=1)))

    cur.execute("select customer_id from raw.customers where signup_date <= %s", (run_date,))
    customer_ids = [r[0] for r in cur.fetchall()]
    cur.execute("select product_id, unit_price_cents from raw.products")
    prices = dict(cur.fetchall())
    product_ids = list(prices)

    # Weekends are busier.
    n_orders = rng.randint(80, 140) + (40 if run_date.weekday() >= 5 else 0)
    orders, items = [], []
    for n in range(n_orders):
        order_id = f"ORD-{run_date:%Y%m%d}-{n:04d}"
        status = rng.choices(["completed", "returned", "cancelled"], weights=[85, 8, 7])[0]
        channel = None if rng.random() < 0.03 else rng.choice(CHANNELS)
        order_ts = day_start + timedelta(seconds=rng.randint(0, 86_399))
        orders.append((order_id, rng.choice(customer_ids), order_ts, status.upper()
                       if rng.random() < 0.1 else status, channel))

        for line, product_id in enumerate(rng.sample(product_ids, k=rng.randint(1, 4))):
            discount = rng.choice([0, 0, 0, 0.05, 0.10, 0.20])
            items.append((f"{order_id}-{line}", order_id, product_id, rng.randint(1, 3),
                          prices[product_id], discount))

    execute_values(cur, "insert into raw.orders (order_id, customer_id, order_ts, status, channel) "
                        "values %s", orders)
    execute_values(cur, "insert into raw.order_items values %s", items)
    return n_orders, len(items)


def run(run_date):
    rng = random.Random(run_date.toordinal())
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        seed_reference_data(cur, run_date, rng)
        apply_daily_changes(cur, run_date, rng)
        n_orders, n_items = generate_orders(cur, run_date, rng)
    print(f"[{run_date}] loaded {n_orders} orders and {n_items} order items")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="Date to generate, YYYY-MM-DD")
    args = parser.parse_args()
    run(date.fromisoformat(args.date))
