"""Utilities to model money transactions."""

from random import choices, choice, randint
from string import ascii_letters, digits
from typing import List, Tuple
import numpy as np

account_chars: str = digits + ascii_letters


def _random_account_nbr(acct_nbrs) -> str:
    """Return a random account number made of 12 characters."""
    return random.choice(acct_nbrs)

def _get_account_nbrs() -> List[Tuple[str, str]]:
    table_name = "user_accounts_dim"
    conn = connect(
            dbname="transactions_db",
            user="postgres",
            host="db",
            password="postgres"
            )
    cursor = conn.cursor()
    acct_nbrs_sql = f'SELECT account_number, spend_type FROM {table_name}'
    cursor.execute(acct_nbrs_sql)
    acct_nbrs = cursor.fetchall()
    return acct_nbrs

def _random_amount(spend_type) -> float:
    # Get a bucket the user will choose to spend the amount from:
    bucket_probs = {0: [0.8, 0.1, 0.1], 1: [0.3,0.5, 0.2], 2: [0.2,0.4,0.4]}
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
    acct_nbrs = _get_account_nbrs()
    return {
        'source': _random_account_nbr(acct_nbrs),
        'target': _random_account_nbr(acct_nbrs),
        'amount': _random_amount(),
        # Keep it simple: it's all euros
        'currency': 'EUR',
    }
