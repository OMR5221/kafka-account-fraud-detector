"""Utilities to model user profiles"""

from faker import Faker
from random import choices, randint, randrange
from string import ascii_letters, digits
from datetime import date, timedelta
from psycopg2 import connect


account_chars: str = digits + ascii_letters

def _random_account_id() -> str:
    """Return a random account number made of 12 characters."""
    return ''.join(choices(account_chars, k=12))

def _gen_user_accounts(num_accounts: int) -> dict:
    fake = Faker()
    """Return a random user_profile."""
    sex_cd = ["M", "F"]
    accounts = []
    for i in range(num_profiles):
        user = {}
        user['account_number'] = _random_account_id()
        # Sex:
        sex_index = randint(0, 1)
        user['sex'] = sex_cd[sex_index]
        if user['sex'] == 'M':
            # First Name:
            user['first_name'] = fake.first_name_male()
            # Last Name:
            user['last_name'] = fake.last_name_male()
        elif user['sex'] == 'F':
            # First Name:
            user['first_name'] = fake.first_name_female()
            # Last Name:
            user['last_name'] = fake.last_name_female()
        # DOB:
        user['birth_date'] = fake.date_of_birth()
        # Signup Date:
        current_date = date.today()
        start_date = current_date - timedelta(days=1825)
        rand_day_num = random.randrange(1825)
        user['signup_date'] = start_date + timedelta(days=rand_day_num)
        # Username:
        user['username'] = f"{lower(profile['first_name'][0])}{lower(profile[last_name])}"
        # Email:
        user['email'] = "{profile['username']}@gmail.com"
        # Address:
        user['address'] = fake.address()
        # Job:
        user['job'] = fake.job()
        # Company:
        user['company'] = fake.company()
        # Spend Type 
        spend_type = {
                0: (0,100),
                1: (0,500),
                2: (0,1000)
        }
        # Pick random spend_type
        spend_index = randint(0, 2)
        user['spend_type'] = spend_index
        
        accounts.append(user)

    return accounts

# Write the users created to a postgres db for use in the CS process:
def _write_to_db(profiles: dict):
    table_name = "user_accounts_dim"
    conn = connect(
            dbname="transactions_db",
            user="postgres",
            host="db",
            password="postgres"
            )
    cursor = conn.cursor()
    for user in profiles:
        cursor.execute(f"INSERT INTO {table_name} VALUES (
                DEFAULT,
                {user['account_number']},
                {user['first_name']},
                {user['last_name']}, 
                {user['birth_date']}, 
                {user['sex']},
                {user['signup_date']},
                {user['user_id']},
                {user['username']},
                {user['email']},
                {user['address']},
                {user['spend_type']}
        );")

    cursor.close()
    conn.close()
    
def create_user_accounts() -> dict:
    """Create a random, fake use profile"""
    _write_to_db(_gen_user_accounts(100))
