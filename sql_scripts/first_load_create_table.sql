CREATE TABLE if not exists DWH_FACT_transactions(
	trans_id VARCHAR(128),
	transe_date date,
	card_num VARCHAR(128),
	opertype VARCHAR(128),
	amt DECIMAL,
	oper_result VARCHAR(128),
	terminal VARCHAR(128)
);

CREATE TABLE if not exists DWH_DIM_terminals_HIST(
	terminal_id VARCHAR(128),
	terminal_type VARCHAR(128),
	terminal_city VARCHAR(128),
	terminal_address VARCHAR(128),
	effective_from datetime,
	effective_to datetime DEFAULT (datetime('2999-12-31 23:59:59')),
	deleted_flg integer DEFAULT 0
);

CREATE TABLE if not exists DWH_DIM_passport_blacklist(
	passport_num VARCHAR(128),
	entry_dt date
);

CREATE TABLE if not exists rep_fraud(
	event_dt date,
	passport VARCHAR(128),
	fio VARCHAR(128),
	phone VARCHAR(128),
	event_type VARCHAR(128),
	report_dt date
);

CREATE TABLE if not exists META_count_rows(
	upload_dt date,
	table_name VARCHAR(128),
	new_rows integer,
	updated_rows integer,
	deleted_rows integer
);


CREATE TABLE if not exists DWH_DIM_cards as 
	SELECT *
	FROM cards 
;

CREATE table if not exists DWH_DIM_accounts as
	SELECT *
	FROM accounts
;

CREATE TABLE if not exists DWH_DIM_clients as 
	SELECT *
	FROM clients
;
