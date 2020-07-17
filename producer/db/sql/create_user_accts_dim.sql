CREATE TABLE user_accounts_dim ( 
	user_id SERIAL PRIMARY KEY,
	account_number TEXT NOT NULL,
	first_name TEXT NOT NULL,
	last_name TEXT NOT NULL,
	birth_date DATE,
	sex TEXT,
	signup_date DATE,
	username TEXT,
	email TEXT,
	address TEXT,
	spend_type INTEGER
);
