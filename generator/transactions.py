"""Utilities to model money transactions."""

from random import choices, choice, randint, random
from string import ascii_letters, digits
from typing import List, Tuple
import numpy as np
from psycopg2 import connect
from datetime import datetime

account_chars: str = digits + ascii_letters

def _random_customer(cust_dtls) -> tuple:
    """Return a random account number made of 12 characters."""
    return choices(cust_dtls)[0]

def _get_cust_dtls() -> List[Tuple[str, str]]:
    table_name = "user_accounts_dim"
    conn = connect(
            dbname="transactions_db",
            user="postgres",
            host="postgres",
            password="postgres"
            )
    cursor = conn.cursor()
    cust_dtls_sql = f'SELECT account_number, spend_type FROM {table_name}'
    cursor.execute(cust_dtls_sql)
    cust_dtls = cursor.fetchall()
    print(f"Number of customer accts: {len(cust_dtls)}")
    cursor.close()
    conn.close()
    return cust_dtls

def _random_amount(spend_type: int) -> float:
    # Get a bucket the user will choose to spend the amount from:
    bucket_probs = {0: [0.75, 0.1, 0.05, 0.1], 1: [0.45, 0.35, 0.1, 0.1], 2: [0.2, 0.4, 0.35, 0.05]}
    bucket_num = np.random.choice(4, 1, p=bucket_probs[spend_type])
    
    print(f"User Bucker Num: {bucket_num}")
    if bucket_num == 0:
        """Return a random amount between 1.00 and 100.00"""
        return randint(100, 1000) / 100
    elif bucket_num == 1:
        """Return a random amount between 1.00 and 500.00"""
        return randint(100, 5000) / 100
    elif bucket_num == 2:
        """Return a random amount between 1.00 and 1000.00"""
        return randint(100, 10000) / 100
    elif bucket_num == 3:
        """Return a random amount between 1000.00 and 5000"""
        return randint(10000, 50000) / 100


def create_random_transaction() -> dict:
    """Create a fake, randomised transaction."""
    cust_dtls = _get_cust_dtls()
    source_cust = _random_customer(cust_dtls)
    print(f"Source Customer: {source_cust}")
    print(f"Source Customer Type: {type(source_cust)}")
    rand_amt = _random_amount(source_cust[1])
    print(f"Random Amt: {rand_amt}")
    target_cust = _random_customer(cust_dtls)
    print(f"Target Customer: {target_cust}")

    return {
        'activity_timestamp_utc': datetime.utcnow().timestamp(),
        'event': 'transfer',
        'source': source_cust[0],
        'target': target_cust[0],
        'amount': rand_amt,
        'spend_type': source_cust[1],
        'currency': 'USD',
    }
