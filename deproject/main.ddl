
------------------------------------------------

-- Создание структуры хранилища (Формат SCD2) --

------------------------------------------------

-- Создание измерения "Терминалы"

create table demipt3.mknn_dwh_dim_terminals_hist(
	terminal_id varchar,
	terminal_type varchar,
	terminal_city varchar,
	terminal_address varchar,
	effective_from timestamp(0),
	effective_to timestamp(0),
	delete_flg char(1));


-- Создание стейджинга "Терминалы"

create table demipt3.mknn_dwh_stg_terminals(
	terminal_id varchar,
	terminal_type varchar,
	terminal_city varchar,
	terminal_address varchar);


-- Создание удалений "Терминалы"

create table demipt3.mknn_dwh_stg_terminals_del(
	terminal_id varchar)

------------------------------------------------------------------------------------

-- Создание измерения "Карты"

create table demipt3.mknn_dwh_dim_cards_hist(
	card_num varchar,
	account_num varchar,
	effective_from date,
	effective_to date,
	delete_flg char(1));


-- Создание стейджинга "Карты"

create table demipt3.mknn_dwh_stg_cards(
	card_num varchar,
	account varchar,
	update_dt timestamp(0));


-- Создание удалений "Карты"

create table demipt3.mknn_dwh_stg_cards_del(
	card_num varchar)


-- Создание метаданных "Карты"

create table demipt3.mknn_meta_cards(
	max_update_dt timestamp(0));

insert into demipt3.mknn_meta_cards(max_update_dt)
values (to_timestamp('1900-01-01', 'YYYY-MM-DD'));

------------------------------------------------------------------------------------

-- Создание измерения "Аккаунты"

create table demipt3.mknn_dwh_dim_accounts_hist(
	account_num varchar,
	valid_to date,
	client varchar,
	effective_from date,
	effective_to date,
	delete_flg char(1));


-- Создание стейджинга "Аккаунты"

create table demipt3.mknn_dwh_stg_accounts(
	account varchar,
	valid_to date,
	client varchar,
	update_dt timestamp(0));


-- Создание удалений "Аккаунты"

create table demipt3.mknn_dwh_stg_accounts_del(
	account varchar)


-- Создание метаданных "Аккаунты"

create table demipt3.mknn_meta_accounts(
	max_update_dt timestamp(0));

insert into demipt3.mknn_meta_accounts(max_update_dt)
values (to_timestamp('1899-12-31', 'YYYY-MM-DD'));

------------------------------------------------------------------------------------

-- Создание измерения "Клиенты"

create table demipt3.mknn_dwh_dim_clients_hist(
	client_id varchar,
	last_name varchar,
	first_name varchar,
	patronymic varchar,
	date_of_birth date,
	passport_num varchar,
	passport_valid_to date,
	phone varchar,
	effective_from date,
	effective_to date,
	delete_flg char(1));


-- Создание стейджинга "Клиенты"

create table demipt3.mknn_dwh_stg_clients(
	client_id	varchar,
	last_name	varchar,
	first_name	varchar,
	patronymic	varchar,
	date_of_birth	date,
	passport_num	varchar,
	passport_valid_to	date,
	phone	char(16),
	update_dt	timestamp(0));


-- Создание удалений "Клиенты"

create table demipt3.mknn_dwh_stg_clients_del(
	client_id varchar)


-- Создание метаданных "Клиенты"

create table demipt3.mknn_meta_clients(
	max_update_dt timestamp(0));

insert into demipt3.mknn_meta_clients(max_update_dt)
values (to_timestamp('1899-12-31', 'YYYY-MM-DD'));

------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

-- Создание факта "Транзакции"

create table demipt3.mknn_dwh_fact_transactions(
	trans_id varchar,
	trans_date timestamp(0),
	card_num varchar,
	oper_type varchar,
	amt decimal,
	oper_result varchar,
	terminal varchar);

------------------------------------------------------------------------------------

-- Создание факта "Блэклист"

create table demipt3.mknn_dwh_fact_passport_blacklist(
	passport_num varchar,
	entry_dt date);

-- Создание стейджинга "Блэклист"

create table demipt3.mknn_dwh_stg_blacklist(
	passport_num varchar,
	entry_dt date);
	
------------------------------------------------------------------------------------

-- Создание отчета
--drop table demipt3.mknn_rep_fraud;

create table demipt3.mknn_rep_fraud(
	event_dt timestamp(0),
	passport varchar,
	fio varchar,
	phone varchar,
	event_type varchar,
	report_dt date)
	
-- Создание стейджинга отчета
--drop table demipt3.mknn_dwh_stg_rep_fraud;
	
create table demipt3.mknn_dwh_stg_rep_fraud(
	event_dt timestamp(0),
	passport varchar,
	fio varchar,
	phone varchar,
	event_type varchar,
	report_dt date)
	
--Создание мета-данных для отчета
--drop table demipt3.mknn_meta_rep;

create table demipt3.mknn_meta_rep(
	rep_date date);
	
insert into demipt3.mknn_meta_rep(rep_date)
values (to_date('2021-03-01', 'YYYY-MM-DD'));
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

	
--delete from demipt3.mknn_dwh_dim_accounts_hist;
--delete from demipt3.mknn_dwh_dim_cards_hist;
--delete from demipt3.mknn_dwh_dim_clients_hist;
--delete from demipt3.mknn_dwh_dim_terminals_hist;
--delete from demipt3.mknn_dwh_fact_passport_blacklist;
--delete from demipt3.mknn_dwh_fact_transactions;
--delete from demipt3.mknn_dwh_stg_accounts;
--delete from demipt3.mknn_dwh_stg_accounts_del;
--delete from demipt3.mknn_dwh_stg_blacklist;
--delete from demipt3.mknn_dwh_stg_cards;
--delete from demipt3.mknn_dwh_stg_cards_del;
--delete from demipt3.mknn_dwh_stg_clients;
--delete from demipt3.mknn_dwh_stg_clients_del;
--delete from demipt3.mknn_dwh_stg_rep_fraud;
--delete from demipt3.mknn_dwh_stg_terminals;
--delete from demipt3.mknn_dwh_stg_terminals_del;
--delete from demipt3.mknn_meta_accounts;
--delete from demipt3.mknn_meta_cards;
--delete from demipt3.mknn_meta_clients;
--delete from demipt3.mknn_meta_rep;
--delete from demipt3.mknn_rep_fraud;
--
--insert into demipt3.mknn_meta_cards(max_update_dt)
--values (to_timestamp('1900-01-01', 'YYYY-MM-DD'));
--insert into demipt3.mknn_meta_accounts(max_update_dt)
--values (to_timestamp('1899-12-31', 'YYYY-MM-DD'));
--insert into demipt3.mknn_meta_clients(max_update_dt)
--values (to_timestamp('1899-12-31', 'YYYY-MM-DD'));
--insert into demipt3.mknn_meta_rep(rep_date)
--values (to_date('2021-03-01', 'YYYY-MM-DD'));
--
--select * from demipt3.mknn_dwh_dim_accounts_hist;
--select * from demipt3.mknn_dwh_dim_cards_hist;
--select * from demipt3.mknn_dwh_dim_clients_hist;
--select * from demipt3.mknn_dwh_dim_terminals_hist;
--select * from demipt3.mknn_dwh_fact_passport_blacklist;
--select * from demipt3.mknn_dwh_fact_transactions;
--select * from demipt3.mknn_dwh_stg_accounts;
--select * from demipt3.mknn_dwh_stg_accounts_del;
--select * from demipt3.mknn_dwh_stg_blacklist;
--select * from demipt3.mknn_dwh_stg_cards;
--select * from demipt3.mknn_dwh_stg_cards_del;
--select * from demipt3.mknn_dwh_stg_clients;
--select * from demipt3.mknn_dwh_stg_clients_del;
--select * from demipt3.mknn_dwh_stg_rep_fraud;
--select * from demipt3.mknn_dwh_stg_terminals;
--select * from demipt3.mknn_dwh_stg_terminals_del;
--select * from demipt3.mknn_meta_accounts;
--select * from demipt3.mknn_meta_cards;
--select * from demipt3.mknn_meta_clients;
--select * from demipt3.mknn_meta_rep;
--select * from demipt3.mknn_rep_fraud;
--
--drop table demipt3.mknn_dwh_dim_accounts_hist;
--drop table demipt3.mknn_dwh_dim_cards_hist;
--drop table demipt3.mknn_dwh_dim_clients_hist;
--drop table demipt3.mknn_dwh_dim_terminals_hist;
--drop table demipt3.mknn_dwh_fact_passport_blacklist;
--drop table demipt3.mknn_dwh_fact_transactions;
--drop table demipt3.mknn_dwh_stg_accounts;
--drop table demipt3.mknn_dwh_stg_accounts_del;
--drop table demipt3.mknn_dwh_stg_blacklist;
--drop table demipt3.mknn_dwh_stg_cards;
--drop table demipt3.mknn_dwh_stg_cards_del;
--drop table demipt3.mknn_dwh_stg_clients;
--drop table demipt3.mknn_dwh_stg_clients_del;
--drop table demipt3.mknn_dwh_stg_rep_fraud;
--drop table demipt3.mknn_dwh_stg_terminals;
--drop table demipt3.mknn_dwh_stg_terminals_del;
--drop table demipt3.mknn_meta_accounts;
--drop table demipt3.mknn_meta_cards;
--drop table demipt3.mknn_meta_clients;
--drop table demipt3.mknn_meta_rep;
--drop table demipt3.mknn_rep_fraud;