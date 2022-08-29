import sqlite3
import os
import pandas as pd
from datetime import timedelta, datetime, date

conn = sqlite3.connect('project.db')
cursor = conn.cursor()

# Если программа запускается впервые, то исполняем файл ddl_dml.sql;
# Если этот файл уже исполнялся, то просто пропускаем этот блок кода
with open('ddl_dml.sql') as f:
	try:
		cursor.executescript(f.read())
		conn.commit()
	except sqlite3.OperationalError:
		pass

# Создаем все остальные постоянные таблицы, если их не существует
with open('sql_scripts/first_load_create_table.sql') as f:
	cursor.executescript(f.read())
	conn.commit()

# Создаем временные таблицы
with open('sql_scripts/create_STG_tables.sql') as f:
	cursor.executescript(f.read())
	conn.commit()

# Создаем список с названиями всех файлов в папке data
file_names = sorted(os.listdir('data'))

# Цикл проходит по названиям всех файлов в папке data
for file_name in file_names:

	# Пропускаем системный файл
	if file_name == '.DS_Store':
		continue
	
	# Создаем переменную с датой, файл за которую обрабатывается на текущей итерации
	file_date = datetime.strptime(file_name[file_name.rfind('_') + 1:file_name.rfind('.')], '%d%m%Y').date()

	# Если название файла начинается с "transactions", то работаем с ним, как с txt файлом
	if file_name.startswith('transactions'):
		with open(f'data/{file_name}') as f:
			# Создаем счетчик строк, чтобы исключать строки с заголовками
			count = 0
			for line in f:
				if count != 0:
					lst_line = line.strip().split(';')
					# Приводим поля в файле к виду, аналогичному нашей таблице
					lst_line[2], lst_line[3], lst_line[4] = lst_line[3], lst_line[4], lst_line[2]

					# Добавляем транзакции в постоянную таблицу
					cursor.execute('''
						INSERT INTO DWH_FACT_transactions(trans_id, transe_date, card_num, opertype, amt, oper_result, terminal)
						VALUES (?, ?, ?, ?, ?, ?, ?)
						''', lst_line)

					# Добавляем транзакции во временную таблицу
					cursor.execute('''
						INSERT INTO STG_transactions(trans_id, transe_date, card_num, opertype, amt, oper_result, terminal)
						VALUES (?, ?, ?, ?, ?, ?, ?)
						''', lst_line)
				count += 1

			# Сохраняем количество новых транзакций для дальнейшей вставки в META таблицу
			cursor.execute('SELECT COUNT(*) FROM STG_transactions')
			new_transactions = cursor.fetchone()[0]

			# Добавляем в rep_fraud информацию о совершении операций при просроченном или заблокированном паспорте
			# P.S. Файл с паспортами обрабатывается далее по коду, но, благодаря сортировке файлов в строке 30, файл >>>
			# с транзакциями будет обрабатываться позднее всех остальных, что позволяет добавить выявление мошеннических >>>
			# операций уже на этом этапе
			cursor.execute('''
				INSERT INTO rep_fraud(
					event_dt,
					passport,
					fio,
					phone,
					event_type,
					report_dt)
				SELECT 
					?,
					t1.passport_num,
					t1.last_name || ' ' || t1.first_name || ' '|| t1.patronymic as fio,
					t1.phone,
					1 as event_type,
					?
				FROM DWH_DIM_clients t1
				INNER JOIN DWH_DIM_accounts t2
				ON t1.client_id = t2.client
				INNER JOIN DWH_DIM_cards t3
				ON t2.account = t3.account
				INNER JOIN STG_transactions t4
				ON t3.card_num = t4.card_num
				WHERE t4.transe_date > t1.passport_valid_to
				OR t1.passport_num IN(
					SELECT passport_num
					FROM DWH_DIM_passport_blacklist
					WHERE entry_dt <= ?
					-- Пояснение про проверку entry_dt: Так как мы обрабатываем несколько файлов, на этом этапе могут возникнуть >>> 
					-- кейсы, при которых паспорт добавлен в базу заблокированных, например 3.03.2022, а >>>
					-- транзакции обрабатываются за 2.03.2022. При данной проверке мы исключаем подобные ситуации
					)
				''', [file_date, date.today(), file_date])

			# Добавляем в rep_fraud информацию о совершении операций при недействующем договоре
			cursor.execute('''
				INSERT INTO rep_fraud(
					event_dt,
					passport,
					fio,
					phone,
					event_type,
					report_dt)
				SELECT 
					?,
					t1.passport_num,
					t1.last_name || ' ' || t1.first_name || ' '|| t1.patronymic as fio,
					t1.phone,
					2 as event_type,
					?
				FROM DWH_DIM_clients t1
				INNER JOIN DWH_DIM_accounts t2
				ON t1.client_id = t2.client
				INNER JOIN DWH_DIM_cards t3
				ON t2.account = t3.account
				INNER JOIN STG_transactions t4
				ON t3.card_num = t4.card_num
				WHERE t4.transe_date > t2.valid_to
				''', [file_date, date.today()])

			# Очищаем временную таблицу с транзакциями
			cursor.execute('''
				DELETE FROM STG_transactions
				''')

			# Добавляем во временную таблицу транзакции совершенные за дату файла + последний час предыдущего дня 
			# Условие выявления по ТЗ. Могут быть транзакции, которые можно будет определить, как мошеннические, только>>>
			# по итогам мониторинга текущего дня + конца предыдущего
			cursor.execute('''
				INSERT INTO STG_transactions(
					trans_id, 
					transe_date, 
					card_num, 
					opertype, 
					amt, 
					oper_result, 
					terminal)
				SELECT *
				FROM DWH_FACT_transactions
				WHERE transe_date > (
					SELECT ? || ' ' || '23:00:01'
					FROM DWH_FACT_transactions)
				AND opertype = 'WITHDRAW'
				''', [file_date - timedelta(days=1)])

			# Добавляем в rep_fraud информацию о клиентах, совершивших операции в разных городах в течение одного часа
			cursor.execute('''
				INSERT INTO rep_fraud(
					event_dt,
					passport,
					fio,
					phone,
					event_type,
					report_dt)
				SELECT
					date(t1.transe_date),
					t4.passport_num,
					t4.last_name || ' ' || t4.first_name || ' '|| t4.patronymic as fio,
					t4.phone,
					3,
					?
				FROM (
					SELECT
						sum_cnt,
						card_num,
						transe_date,
						terminal_city,
						diff_dates
					FROM(
						SELECT
							sum_cnt,
							card_num,
							transe_date,
							terminal_city,
							(JulianDay(transe_date) - JulianDay(prev_transe_date)) * 24 as diff_dates
						FROM (
							SELECT 
								SUM(cnt) OVER(PARTITION BY card_num) as sum_cnt,
								card_num,
								transe_date,
								LAG(transe_date) OVER(PARTITION BY card_num ORDER BY transe_date) as prev_transe_date,
								terminal_city
							FROM (
								SELECT 
									COUNT(*) OVER(PARTITION BY t1.card_num ORDER BY t1.transe_date) as cnt,
									t1.card_num as card_num,
									t1.transe_date as transe_date,
									t2.terminal_city as terminal_city
								FROM STG_transactions t1
								INNER JOIN DWH_DIM_terminals_HIST t2
								ON t1.terminal = t2.terminal_id
								GROUP BY t1.card_num, t2.terminal_city) t1
							GROUP BY card_num, terminal_city) t1
						WHERE sum_cnt > 2) t1
					WHERE diff_dates <= 1) t1
				INNER JOIN DWH_DIM_cards t2
				ON t1.card_num = t2.card_num
				INNER JOIN DWH_DIM_accounts t3
				ON t2.account = t3.account
				INNER JOIN DWH_DIM_clients t4
				ON t3.client = t4.client_id
			''', [date.today()])

			# Очищаем временную таблицу с транзакциями
			cursor.execute('''
				DELETE FROM STG_transactions
				''')

			# Добавляем во временную таблицу транзакции совершенные за дату файла + последние 20 минут предыдущего дня
			# (Условие выявления по ТЗ. Могут быть транзакции, которые можно будет определить, как мошеннические, только>>>
			# по итогам мониторинга текущего дня + конца предыдущего)
			cursor.execute('''
				INSERT INTO STG_transactions(
					trans_id, 
					transe_date, 
					card_num, 
					opertype, 
					amt, 
					oper_result, 
					terminal)
				SELECT *
				FROM DWH_FACT_transactions
				WHERE transe_date > (
					SELECT ? || ' ' || '23:40:01'
					FROM DWH_FACT_transactions)
				AND opertype = 'WITHDRAW'
				''', [file_date - timedelta(days=1)])

			# Добавляем в rep_fraud информацию о клиентах с попыткой подбора суммы
			cursor.execute('''
				INSERT INTO rep_fraud(
					event_dt,
					passport,
					fio,
					phone,
					event_type,
					report_dt)
				SELECT
					date(t1.transe_date),
					t4.passport_num,
					t4.last_name || ' ' || t4.first_name || ' '|| t4.patronymic as fio,
					t4.phone,
					4,
					?
				FROM (
					SELECT
						transe_date, 
						card_num,
						amt, 
						oper_result,
						prev_result,
						prev_prev_result,
						prev_prev_prev_result,
						prev_prev_prev_dt,
						prev_amt,
						prev_prev_amt,
						prev_prev_prev_amt
					FROM(
						SELECT
							transe_date, 
							card_num,
							amt, 
							oper_result, 
							LAG(oper_result) OVER(PARTITION BY card_num) as prev_result,
							LAG(oper_result, 2) OVER(PARTITION BY card_num) as prev_prev_result,
							LAG(oper_result, 3) OVER(PARTITION BY card_num) as prev_prev_prev_result,
							LAG(transe_date, 3) OVER(PARTITION BY card_num) as prev_prev_prev_dt,
							LAG(amt) OVER(PARTITION BY card_num) as prev_amt,
							LAG(amt, 2) OVER(PARTITION BY card_num) as prev_prev_amt,
							LAG(amt, 3) OVER(PARTITION BY card_num) as prev_prev_prev_amt
						FROM STG_transactions) t1
					WHERE oper_result = 'SUCCESS'
					AND prev_result = 'REJECT'
					AND prev_prev_result = 'REJECT'
					AND prev_prev_prev_result = 'REJECT'
					AND ((JulianDay(transe_date) - JulianDay(prev_prev_prev_dt)) * 24 * 60) < 20
					AND amt < prev_amt < prev_prev_amt < prev_prev_prev_amt) t1
				INNER JOIN DWH_DIM_cards t2
				ON t1.card_num = t2.card_num
				INNER JOIN DWH_DIM_accounts t3
				ON t2.account = t3.account
				INNER JOIN DWH_DIM_clients t4
				ON t3.client = t4.client_id
			''', [date.today()])

			# Добавляем в таблицу с метаданными количество новых добавленных транзакций
			cursor.execute('''
				INSERT INTO META_count_rows(
					upload_dt,
					table_name,
					new_rows,
					updated_rows,
					deleted_rows
				)
				VALUES(
					?,
					'DWH_FACT_transactions',
					?,
					'not exists',
					'not exists'
					)
				''', [file_date, new_transactions])

			# Очищаем временную таблицу перед использованием следующего файла (при наличии такого в цикле)
			cursor.execute('''
				DELETE FROM STG_transactions
				''')
			
			conn.commit()

	# С файлом с паспортами работаем как с .xlsx файлами
	elif file_name.startswith('passport'):
		passport = pd.read_excel(f'data/{file_name}')
		passport_df = pd.DataFrame(passport).values
		for line in passport_df:
			clear_line = [line[1], str(line[0])[:str(line[0]).find(' ')]]

			# Добавляем во временную таблицу записи из файла
			cursor.execute('''
				INSERT INTO STG_passports_blacklist(
					passport_num,
					entry_dt)
				VALUES(?, ?)
				''', clear_line)

		# Добавляем во временную таблицу паспорта, которые отсутствуют в постоянной таблице 
		cursor.execute('''
			INSERT INTO STG_new_passports(
				passport_num,
				entry_dt
			)
			SELECT 
				passport_num,
				entry_dt
			FROM STG_passports_blacklist
			WHERE passport_num not IN (
				SELECT passport_num
				FROM DWH_DIM_passport_blacklist
			)
		''')

		# Добавляем в постоянную таблицу новые паспорта
		cursor.execute('''
			INSERT INTO DWH_DIM_passport_blacklist(
				passport_num,
				entry_dt
			)
			SELECT
				passport_num,
				?
			FROM STG_new_passports
			''', [file_date])

		# Находим количество новых строк с паспортами и присваиваем значение переменной
		cursor.execute('SELECT COUNT(*) FROM STG_new_passports')
		new_passports_rows = cursor.fetchone()[0]

		# Добавляем в таблицу с метаданными количество строк с паспортами
		cursor.execute('''
			INSERT INTO META_count_rows(
				upload_dt,
				table_name,
				new_rows,
				updated_rows,
				deleted_rows
			)
			VALUES(
			?, 
			'DWH_DIM_passport_blacklist', 
			?, 
			'not exists', 
			'not_exists')
			''', [file_date, new_passports_rows])

		# Очищаем временные таблицы перед использованием следующего файла (при наличии такого в цикле)
		cursor.executescript('''
			DELETE FROM STG_passports_blacklist;
			DELETE FROM STG_new_passports;
			''')
		conn.commit()

	# С файлом с терминалами работаем как с .xlsx файлами
	elif file_name.startswith('terminals'):
		terminals = pd.read_excel(f'data/{file_name}')
		terminals_df = pd.DataFrame(terminals).values
		for line in terminals_df:
			new_line = list(line)

			# Вставляем терминалы из файла во временную таблицу
			cursor.execute('''
				INSERT INTO STG_terminals(
					terminal_id,
					terminal_type,
					terminal_city,
					terminal_address)
				VALUES(?, ?, ?, ?)
				''', new_line)

		# Вставляем в STG_new_terminals терминалы, которые отсутствуют или имеют deleted_flg = 1 в постоянной таблице
		cursor.execute('''
			INSERT INTO STG_new_terminals(
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			)
			SELECT 
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			FROM STG_terminals
			WHERE terminal_id not IN (
				SELECT terminal_id
				FROM DWH_DIM_terminals_HIST
			)
			OR terminal_id IN(
				SELECT terminal_id
				FROM DWH_DIM_terminals_HIST
				WHERE deleted_flg = 1
				AND effective_to = '2999-12-31 23:59:59'
			)
		''')

		# Изменяем поле effective_to у терминалов с deleted_flg = 1, которые вновь стали актуальными (при наличии)
		cursor.execute('''
			UPDATE DWH_DIM_terminals_HIST
			SET effective_to = ?
			WHERE deleted_flg = 1
			AND effective_to = '2999-12-31 23:59:59'
			AND terminal_id IN (
				SELECT terminal_id
				FROM STG_new_terminals
			)
		''', [file_date - timedelta(days=1)])

		# Добавляем в постоянную таблицу новые терминалы
		cursor.execute('''
			INSERT INTO DWH_DIM_terminals_HIST(
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				effective_from
			)
			SELECT
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				?
			FROM STG_new_terminals
			''', [file_date])

		# Добавляем в STG_deleted_terminals неактуальные терминалы
		cursor.execute('''
			INSERT INTO STG_deleted_terminals(
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			)
			SELECT 
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			FROM DWH_DIM_terminals_HIST
			WHERE terminal_id NOT IN (
				SELECT terminal_id
				FROM STG_terminals
				)
			AND deleted_flg = 0
			AND effective_to = '2999-12-31 23:59:59'
		''')

		# Изменяем поле effective_to в постоянной таблице у записей, которые более неактуальны
		cursor.execute('''
			UPDATE DWH_DIM_terminals_HIST
			SET effective_to = ?
			WHERE deleted_flg = 0
			AND effective_to = '2999-12-31 23:59:59'
			AND terminal_id IN (
				SELECT terminal_id
				FROM STG_deleted_terminals)
			''', [file_date - timedelta(days=1)])

		# Вставляем в постоянную таблицу записи о неактуальных терминалах
		cursor.execute('''
			INSERT INTO DWH_DIM_terminals_HIST(
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				effective_from,
				deleted_flg)
			SELECT
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				?,
				1
			FROM STG_deleted_terminals
			''', [file_date])

		# Находим и вставляем в STG_updated_terminals терминалы, у которых изменился тип, город или адрес
		cursor.execute('''
			INSERT INTO STG_updated_terminals(
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			)
			SELECT 
				t1.terminal_id,
				t1.terminal_type,
				t1.terminal_city,
				t1.terminal_address
			FROM STG_terminals t1
			INNER JOIN DWH_DIM_terminals_HIST t2
			ON t1.terminal_id = t2.terminal_id
			WHERE t2.deleted_flg = 0
			AND t2.effective_to = '2999-12-31 23:59:59'
			AND (
			t1.terminal_type <> t2.terminal_type
			OR t1.terminal_city <> t2.terminal_city
			OR t1.terminal_address <> t2.terminal_address
			)
		''')

		# Изменяем поле effective_to у записей с измененным типом терминала, городом или адресом
		cursor.execute('''
			UPDATE DWH_DIM_terminals_HIST
			SET effective_to = ?
			WHERE deleted_flg = 0
			AND effective_to = '2999-12-31 23:59:59'
			AND terminal_id IN (
				SELECT terminal_id
				FROM STG_updated_terminals)
			''', [file_date - timedelta(days=1)])

		# Вставляем в постоянную таблицу записи с обновленными данными
		cursor.execute('''
			INSERT INTO DWH_DIM_terminals_HIST(
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				effective_from)
			SELECT
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address,
				?
			FROM STG_updated_terminals
			''', [file_date])

		# Находим количество новых строк с паспортами и присваиваем значение переменной
		cursor.execute('SELECT COUNT(*) FROM STG_new_terminals')
		new_terminals_rows = cursor.fetchone()[0]

		# Находим количество строк со ставшими неактуальными паспортами и присваиваем значение переменной
		cursor.execute('SELECT COUNT(*) FROM STG_deleted_terminals')
		deleted_terminals_rows = cursor.fetchone()[0]

		cursor.execute('SELECT COUNT(*) FROM STG_updated_terminals')
		updated_terminals_rows = cursor.fetchone()[0]

		# Добавляем в таблицу с метаданными количество строк с паспортами
		cursor.execute('''
			INSERT INTO META_count_rows(
				upload_dt,
				table_name,
				new_rows,
				updated_rows,
				deleted_rows
			)
			VALUES(?, 'DWH_DIM_terminals_HIST', ?, ?, ?)
			''', [file_date, new_terminals_rows, updated_terminals_rows, deleted_terminals_rows])

		# Очищаем временные таблицы перед использованием следующего файла (при наличии такого в цикле)
		cursor.executescript('''
			DELETE FROM STG_terminals;
			DELETE FROM STG_new_terminals;
			DELETE FROM STG_deleted_terminals;
			DELETE FROM STG_updated_terminals;
			''')

	os.rename(f'data/{file_name}', f'archieve/{file_name}.backup')

conn.commit()

# # Удаляем дубликаты из rep_fraud
# cursor.execute('''
# 	DELETE FROM rep_fraud
# 	WHERE rowid not IN(
# 		SELECT max(rowid)
# 		FROM rep_fraud
# 		GROUP BY event_dt, passport, fio, phone,event_type, report_dt
# 		)
# ''')

# Удаляем все временные таблицы
cursor.executescript('''
	DROP TABLE STG_terminals;
	DROP TABLE STG_new_terminals;
	DROP TABLE STG_deleted_terminals;
	DROP TABLE STG_updated_terminals;
	DROP TABLE STG_transactions;
	DROP TABLE STG_passports_blacklist;
	DROP TABLE STG_new_passports;
''')
conn.commit()

# Выгружаем отчет с мошенническими операциями
cursor.execute('''
	SELECT *
	FROM rep_fraud
''')
rep = pd.DataFrame(cursor.fetchall(), columns=['event_dt', 'passport', 'fio', 'phone', 'event_type', 'report_dt'])
rep.to_excel('rep_fraud.xlsx', columns=['event_dt', 'passport', 'fio', 'phone', 'event_type', 'report_dt'], index=False)
