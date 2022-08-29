CREATE TABLE if not exists STG_terminals(
	effective_from date,
	terminal_id VARCHAR(128),
	terminal_type VARCHAR(128),
	terminal_city VARCHAR(128),
	terminal_address VARCHAR(128)
);

CREATE TABLE if not exists STG_transactions(
	trans_id VARCHAR(128),
	transe_date date,
	card_num VARCHAR(128),
	opertype VARCHAR(128),
	amt DECIMAL,
	oper_result VARCHAR(128),
	terminal VARCHAR(128)
);

CREATE TABLE if not exists STG_new_terminals(
	effective_from date,
	terminal_id VARCHAR(128),
	terminal_type VARCHAR(128),
	terminal_city VARCHAR(128),
	terminal_address VARCHAR(128)
);

CREATE TABLE if not exists STG_deleted_terminals(
	terminal_id VARCHAR(128),
	terminal_type VARCHAR(128),
	terminal_city VARCHAR(128),
	terminal_address VARCHAR(128)
);

CREATE TABLE if not exists STG_updated_terminals(
	effective_from date,
	terminal_id VARCHAR(128),
	terminal_type VARCHAR(128),
	terminal_city VARCHAR(128),
	terminal_address VARCHAR(128)
);

CREATE TABLE if not exists STG_passports_blacklist(
	passport_num VARCHAR(128),
	entry_dt date
);

CREATE TABLE if not exists STG_new_passports(
	passport_num VARCHAR(128),
	entry_dt date
);
