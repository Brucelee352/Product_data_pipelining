"""Kroger product data pipeline — fetches from the Kroger Public API and loads to DuckDB.

This pipeline replaces the legacy synthetic-data pipeline (main_data_pipeline.py).
It performs the following steps:
    1. Authenticate against the Kroger OAuth2 token endpoint (client credentials).
    2. Discover store locations near a set of US zip codes.
    3. Fetch products (across several category search terms) for each location.
    4. Load raw locations, products, and prices into a DuckDB ``raw`` schema.
    5. Run dbt transformations to build the staging + marts models.

Configuration is sourced entirely from environment variables (loaded with
python-dotenv). See ``.env.example`` for the required variables. No real
credentials are stored in source control.

Required environment variables:
    KROGER_CLIENT_ID       OAuth2 client id from https://developer.kroger.com
    KROGER_CLIENT_SECRET   OAuth2 client secret
    KROGER_BASE_URL        API base URL (default: https://api.kroger.com)
    DB_PATH                DuckDB file path, relative to project root
    LOG_LEVEL              Python logging level (default: INFO)
"""

# Standard library imports
import os
import sys
import json
import time
import logging
from base64 import b64encode
from datetime import datetime as dt
from pathlib import Path
from typing import Optional

# Third-party imports
import requests
import duckdb
from dotenv import load_dotenv
from dbt.cli.main import dbtRunner

# Load environment variables from a local .env file (if present).
load_dotenv()

# ----------------------------------------------------------------------------#
# Paths and configuration
# ----------------------------------------------------------------------------#
PROJECT_ROOT = Path(__file__).parents[2]
DBT_ROOT = PROJECT_ROOT / "dbt_pipeline_demo"
DB_PATH = PROJECT_ROOT / os.environ.get(
    "DB_PATH", "dbt_pipeline_demo/databases/kroger_pipeline.duckdb"
)

BASE_URL = os.environ.get("KROGER_BASE_URL", "https://api.kroger.com")
CLIENT_ID = os.environ.get("KROGER_CLIENT_ID")
CLIENT_SECRET = os.environ.get("KROGER_CLIENT_SECRET")

# Point dbt at the project's profiles directory.
os.environ.setdefault("DBT_PROFILES_DIR", str(PROJECT_ROOT / ".dbt"))

# Category search terms run per location, and zip codes used for discovery.
CATEGORY_SEARCHES = [
    "produce", "dairy", "bakery", "meat",
    "frozen", "snacks", "beverages", "household",
]
LOCATION_ZIPS = [
    "10001", "60601", "77001", "90001", "30301",
    "85001", "98101", "78201", "33101", "02101",
]

# Courtesy delay between API calls (seconds).
REQUEST_DELAY = 0.1
# Per-request HTTP timeout (seconds).
REQUEST_TIMEOUT = 30

# ----------------------------------------------------------------------------#
# Logging — write to both stdout and logs/pipeline.log
# ----------------------------------------------------------------------------#
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG = logging.getLogger(__name__)
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)


class PipelineState:
    """Manage the pipeline state across a run."""

    def __init__(self):
        """Initialize the pipeline state."""
        self.cached_data = None

    def reset_state(self):
        """Reset the pipeline state."""
        self.cached_data = None


# Global instance.
pipeline_state = PipelineState()


def ellipsis(process_name: str = "Loading", num_dots: int = 3,
             interval: float = 0.5) -> None:
    """Print a static loading message with trailing periods.

    Purely cosmetic; mirrors the loading indicator from the legacy pipeline.

    Args:
        process_name: The name of the process to display.
        num_dots: The number of trailing dots to print.
        interval: The delay between dots, in seconds.
    """
    try:
        sys.stdout.write(process_name)
        sys.stdout.flush()
        for _ in range(num_dots):
            sys.stdout.write(".")
            sys.stdout.flush()
            time.sleep(interval)
        sys.stdout.write("\n")
    except Exception as e:  # noqa: BLE001 - cosmetic helper, never fatal
        LOG.error("Error in ellipsis function: %s", str(e))
        raise


# ----------------------------------------------------------------------------#
# Kroger API helpers
# ----------------------------------------------------------------------------#
def get_access_token() -> str:
    """Fetch an OAuth2 client-credentials token from Kroger.

    Returns:
        The bearer access token string. Tokens expire after ~30 minutes; this
        pipeline fetches a single token at startup, which is sufficient for the
        ~165 calls it makes.

    Raises:
        RuntimeError: If credentials are missing or the token request fails.
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError(
            "KROGER_CLIENT_ID and KROGER_CLIENT_SECRET must be set "
            "(see .env.example)."
        )

    basic = b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    resp = requests.post(
        f"{BASE_URL}/v1/connect/oauth2/token",
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials", "scope": "product.compact"},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError("Token endpoint returned no access_token.")
    LOG.info("Obtained Kroger access token (expires in %ss).",
             payload.get("expires_in", "unknown"))
    return token


def fetch_locations(token: str, zip_codes: list[str]) -> list[dict]:
    """Fetch store locations near the given zip codes.

    Args:
        token: A valid bearer access token.
        zip_codes: Zip codes to search around (one API call each).

    Returns:
        A de-duplicated list of normalized location dicts ready for loading.
    """
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "application/json"}
    seen: set[str] = set()
    locations: list[dict] = []

    for zip_code in zip_codes:
        try:
            resp = requests.get(
                f"{BASE_URL}/v1/locations",
                headers=headers,
                params={
                    "filter.zipCode.near": zip_code,
                    "filter.radiusInMiles": 50,
                    "filter.limit": 10,
                },
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            LOG.warning("Location fetch failed for zip %s: %s", zip_code, e)
            time.sleep(REQUEST_DELAY)
            continue

        for loc in resp.json().get("data", []):
            loc_id = loc.get("locationId")
            if not loc_id or loc_id in seen:
                continue
            seen.add(loc_id)
            address = loc.get("address", {}) or {}
            geo = loc.get("geolocation", {}) or {}
            locations.append({
                "location_id": loc_id,
                "name": loc.get("name"),
                "chain": loc.get("chain"),
                "address_line1": address.get("addressLine1"),
                "city": address.get("city"),
                "state": address.get("state"),
                "zip_code": address.get("zipCode"),
                "latitude": geo.get("latitude"),
                "longitude": geo.get("longitude"),
            })

        LOG.info("Found %s total unique locations after zip %s.",
                 len(locations), zip_code)
        time.sleep(REQUEST_DELAY)

    return locations


def fetch_products(token: str, location_id: str,
                   categories: list[str]) -> list[dict]:
    """Fetch products for a location across multiple category search terms.

    Args:
        token: A valid bearer access token.
        location_id: The 8-digit Kroger location id.
        categories: Search terms to query for at this location.

    Returns:
        A list of raw Kroger product model dicts (as returned by the API).
    """
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "application/json"}
    products: list[dict] = []

    for term in categories:
        try:
            resp = requests.get(
                f"{BASE_URL}/v1/products",
                headers=headers,
                params={
                    "filter.term": term,
                    "filter.locationId": location_id,
                    "filter.limit": 50,
                },
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            LOG.warning("Product fetch failed (loc=%s, term=%s): %s",
                        location_id, term, e)
            time.sleep(REQUEST_DELAY)
            continue

        data = resp.json().get("data", [])
        products.extend(data)
        LOG.info("Fetched %s products for location %s, term '%s'.",
                 len(data), location_id, term)
        time.sleep(REQUEST_DELAY)

    return products


# ----------------------------------------------------------------------------#
# DuckDB loading
# ----------------------------------------------------------------------------#
def init_db(con: duckdb.DuckDBPyConnection) -> None:
    """Create the raw schema and tables if they do not already exist."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.locations (
            location_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            chain VARCHAR,
            address_line1 VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip_code VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            fetched_at TIMESTAMP DEFAULT now()
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.products (
            product_id VARCHAR PRIMARY KEY,
            description VARCHAR,
            brand VARCHAR,
            categories VARCHAR,
            fetched_at TIMESTAMP DEFAULT now()
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.product_prices (
            product_id VARCHAR,
            location_id VARCHAR,
            item_id VARCHAR,
            size VARCHAR,
            regular_price DECIMAL(10,2),
            promo_price DECIMAL(10,2),
            effective_date DATE,
            expiration_date DATE,
            fulfillment_instore BOOLEAN,
            fulfillment_delivery BOOLEAN,
            fulfillment_curbside BOOLEAN,
            fulfillment_shiptohome BOOLEAN,
            stock_level VARCHAR,
            fetched_at TIMESTAMP DEFAULT now(),
            PRIMARY KEY (product_id, location_id, item_id)
        );
    """)
    LOG.info("Initialized raw schema and tables at %s", DB_PATH)


def load_locations(con: duckdb.DuckDBPyConnection,
                   locations: list[dict]) -> None:
    """Upsert locations into raw.locations.

    Args:
        con: An open DuckDB connection.
        locations: Normalized location dicts from :func:`fetch_locations`.
    """
    if not locations:
        LOG.warning("No locations to load.")
        return

    rows = [
        (
            loc["location_id"], loc.get("name"), loc.get("chain"),
            loc.get("address_line1"), loc.get("city"), loc.get("state"),
            loc.get("zip_code"), loc.get("latitude"), loc.get("longitude"),
        )
        for loc in locations
    ]
    con.executemany("""
        INSERT OR REPLACE INTO raw.locations
            (location_id, name, chain, address_line1, city, state,
             zip_code, latitude, longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, rows)
    LOG.info("Loaded %s locations into raw.locations.", len(rows))


def _parse_date(date_obj: Optional[dict]) -> Optional[str]:
    """Extract the ISO date value from a Kroger dateValueModel object."""
    if not date_obj:
        return None
    return date_obj.get("value")


def load_products_and_prices(con: duckdb.DuckDBPyConnection,
                             products: list[dict],
                             location_id: str) -> None:
    """Upsert products and their per-location prices into the raw tables.

    Args:
        con: An open DuckDB connection.
        products: Raw Kroger product model dicts.
        location_id: The location these prices were fetched for.
    """
    if not products:
        return

    product_rows: list[tuple] = []
    price_rows: list[tuple] = []

    for prod in products:
        product_id = prod.get("productId")
        if not product_id:
            continue

        product_rows.append((
            product_id,
            prod.get("description"),
            prod.get("brand"),
            json.dumps(prod.get("categories", [])),
        ))

        items = prod.get("items") or []
        if not items:
            continue
        item = items[0]
        price = item.get("price") or {}
        fulfillment = item.get("fulfillment") or {}
        inventory = item.get("inventory") or {}

        regular_price = price.get("regular")
        # Only record a price row when a regular price is present (requires a
        # locationId in the request, which we always supply).
        if regular_price is None:
            continue

        price_rows.append((
            product_id,
            location_id,
            item.get("itemId"),
            item.get("size"),
            regular_price,
            price.get("promo"),
            _parse_date(price.get("effectiveDate")),
            _parse_date(price.get("expirationDate")),
            fulfillment.get("instore"),
            fulfillment.get("delivery"),
            fulfillment.get("curbside"),
            fulfillment.get("shiptohome"),
            inventory.get("stockLevel"),
        ))

    if product_rows:
        con.executemany("""
            INSERT OR REPLACE INTO raw.products
                (product_id, description, brand, categories)
            VALUES (?, ?, ?, ?);
        """, product_rows)

    if price_rows:
        con.executemany("""
            INSERT OR REPLACE INTO raw.product_prices
                (product_id, location_id, item_id, size, regular_price,
                 promo_price, effective_date, expiration_date,
                 fulfillment_instore, fulfillment_delivery,
                 fulfillment_curbside, fulfillment_shiptohome, stock_level)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, price_rows)

    LOG.info(
        "Loaded %s products and %s prices for location %s.",
        len(product_rows), len(price_rows), location_id,
    )


# ----------------------------------------------------------------------------#
# dbt orchestration
# ----------------------------------------------------------------------------#
def run_dbt_ops() -> None:
    """Run ``dbt deps`` then ``dbt run`` in the dbt project directory.

    The dbt profile uses a relative DuckDB path, so we ``os.chdir`` into the
    dbt project before invoking dbt and restore the original directory after.
    """
    original_dir = os.getcwd()
    try:
        os.chdir(DBT_ROOT)
        LOG.info("Changed working directory to %s", DBT_ROOT)

        dbt = dbtRunner()

        deps_result = dbt.invoke(["deps"])
        if not deps_result.success:
            raise RuntimeError("Failed to run dbt deps")

        run_result = dbt.invoke([
            "run",
            "--target", "dev",
            "--full-refresh",
        ])
        if not run_result.success:
            raise RuntimeError("Failed to run dbt models")

        LOG.info("Successfully ran dbt models.")
    except Exception as e:
        LOG.error("Error running dbt models: %s", str(e))
        raise
    finally:
        os.chdir(original_dir)
        LOG.info("Changed back to original directory: %s", original_dir)


# ----------------------------------------------------------------------------#
# Orchestration
# ----------------------------------------------------------------------------#
def main() -> None:
    """Orchestrate the full Kroger data pipeline."""
    con: Optional[duckdb.DuckDBPyConnection] = None
    try:
        pipeline_state.reset_state()

        # 1. Verify a virtual environment is active.
        is_venv = hasattr(sys, "real_prefix")
        is_venv_modern = (hasattr(sys, "base_prefix")
                          and sys.base_prefix != sys.prefix)
        if not (is_venv or is_venv_modern):
            LOG.error(
                "Virtual environment is not active. "
                "Please activate it before running the script."
            )
            LOG.info("Activation commands:")
            LOG.info("  Windows: .venv\\Scripts\\activate")
            LOG.info("  macOS/Linux: source .venv/bin/activate")
            sys.exit(1)

        LOG.info("Pipeline initialized at %s",
                 dt.now().strftime("%Y-%m-%d %H:%M:%S"))

        # 2. Authenticate.
        ellipsis("Authenticating with Kroger API")
        token = get_access_token()

        # 3. Initialize the database.
        ellipsis("Initializing database")
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(DB_PATH))
        init_db(con)

        # 4. Discover locations.
        ellipsis("Discovering store locations")
        locations = fetch_locations(token, LOCATION_ZIPS)
        load_locations(con, locations)

        # 5. Fetch products + prices for each location.
        ellipsis("Fetching products and prices")
        for loc in locations:
            location_id = loc["location_id"]
            products = fetch_products(token, location_id, CATEGORY_SEARCHES)
            load_products_and_prices(con, products, location_id)

        con.close()
        con = None

        # 6. Run dbt transformations.
        print("Running dbt transformations!")
        run_dbt_ops()

        timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Pipeline completed successfully at {timestamp}")
        LOG.info("Pipeline completed successfully at %s", timestamp)

    except (RuntimeError, IOError, ValueError, duckdb.Error,
            requests.RequestException) as e:
        LOG.error("Pipeline failed: %s", str(e))
        sys.exit(1)
    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    main()
