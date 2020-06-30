"""Utilities to model money transactions."""

from random import choices, choice, randint
from string import ascii_letters, digits
from typing import List, Tuple
import numpy as np

account_chars: str = digits + ascii_letters


def _random_customer(cust_dtls) -> tuple:
    """Return a random account number made of 12 characters."""
    return random.choice(cust_dtls)

def _get_cust_dtls() -> List[Tuple[str, str]]:
    table_name = "user_accounts_dim"
    conn = connect(
            dbname="transactions_db",
            user="postgres",
            host="db",
            password="postgres"
            )
    cursor = conn.cursor()
    cust_dtls_sql = f'SELECT account_number, spend_type FROM {table_name}'
    cursor.execute(cust_dtls_sql)
    cust_dtls = cursor.fetchall()
    return cust_dtls

def _random_amount(spend_type) -> float:
    # Get a bucket the user will choose to spend the amount from:
    bucket_probs = {0: [0.8, 0.1, 0.1], 1: [0.3, 0.5, 0.2], 2: [0.2, 0.4, 0.4]}
    bucket_num = np.random.choice(3, 1, p=bucket_probs[spend_type])
    
    if bucket_num == 0:
        """Return a random amount between 1.00 and 100.00"""
        return randint(100, 1000) / 100
    elif bucket_num == 1:
        """Return a random amount between 1.00 and 500.00"""
        return randint(100, 5000) / 100
    elif bucket_num == 2:
        """Return a random amount between 1.00 and 1000.00"""
        return randint(100, 10000) / 100


def create_random_transaction() -> dict:
    """Create a fake, randomised transaction."""
    cust_dtls = _get_cust_dtls()
    source_cust = _random_customer(cust_dtls)
    rand_amt = _random_amount(rand_cust[1])
    target_cust = _random_customer(cust_dtls)

    return {
        'source': source_cust[0],
        'target': target_cust[0],
        'amount': rand_amt,
        'currency': 'USD',
    }
