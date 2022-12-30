#!/usr/bin/python3

from project_cnfg import source_db, source_host, source_user, source_pass, dwh_db, dwh_host, dwh_user, dwh_pass
import pandas as pd
import psycopg2
import os
# from progress.bar import IncrementalBar #Прогресс-бар в транзакциях

# Подключение к источнику
connection_src = psycopg2.connect(
    database = source_db,
    host = source_host,
    port = 5432,
    user = source_user,
    password = source_pass
)

# Подключение к хранилищу
connection_dwh = psycopg2.connect(
    database = dwh_db,
    host = dwh_host,
    port = 5432,
    user = dwh_user,
    password = dwh_pass
)

# Отключение автокоммита в хранилище
connection_dwh.autocommit = False

# Создание курсоров
cursor_src = connection_src.cursor()
cursor_dwh = connection_dwh.cursor()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # Работа с измерением cards # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Очистка стейдженговых таблиц
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_cards;')
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_cards_del;')
print("Стейджинги Cards очищены")


# Формирование переменной с метаданными
cursor_dwh.execute('select max_update_dt from demipt3.mknn_meta_cards')
cards_max_update_dt = cursor_dwh.fetchone()
print('Метаданные Cards захвачены')


# Захват данных из источника (изменненых с момента последней загрузки)
cursor_src.execute(f""" select
                            card_num,
                            account,
                            case
                                when update_dt is null then create_dt else update_dt
                            end as mod_dt
                        from info.cards
                        where update_dt is null and create_dt > to_timestamp('{cards_max_update_dt[0]}', 'YYYY-MM-DD HH24:MI:SS')
                           or update_dt is not null and update_dt > to_timestamp('{cards_max_update_dt[0]}', 'YYYY-MM-DD HH24:MI:SS');""")
print("Данные из источника захвачены")


# Формирование датафрейма с использованием пандас
records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )


# Загрузка данных из датафрейма в стейджинг
cursor_dwh.executemany("""  insert into demipt3.mknn_dwh_stg_cards(
                                card_num,
                                account,
                                update_dt)
                            values (%s, %s, %s);""", df.values.tolist())
print("Данные загружены в стейджинг Cards")


# Захват ключей для обработки удалений
cursor_src.execute(""" select card_num from info.cards;""")

records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

cursor_dwh.executemany("""  insert into demipt3.mknn_dwh_stg_cards_del(card_num)
                            values(%s);""", df.values.tolist())
print("Ключи для обработки удалений Cards захвачены")


# Загрукзка данных в таргет (формат СКД2)
cursor_dwh.execute("""  insert into demipt3.mknn_dwh_dim_cards_hist(
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            delete_flg)
                        select
                            stg.card_num,
                            stg.account,
                            stg.update_dt,
                            to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
                            'N' as delete_flg
                        from demipt3.mknn_dwh_stg_cards stg
                        left join demipt3.mknn_dwh_dim_cards_hist tgt
                        on stg.card_num = tgt.card_num
                        where tgt.card_num is null""")
print("Новые данные загружены в стейджинг Cards")


# Обработка обновлений (формат СКД2)
# Закрытие "старой" версии
cursor_dwh.execute("""  update demipt3.mknn_dwh_dim_cards_hist tgt
                        set effective_to = tmp.effective_to
                        from (
                            select
                                stg.card_num,
                                stg.update_dt  - interval '1 second' as effective_to
                            from demipt3.mknn_dwh_stg_cards stg
                            inner join demipt3.mknn_dwh_dim_cards_hist tgt
                                on stg.card_num = tgt.card_num
                                and tgt.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                        and tgt.delete_flg = 'N'
                            where stg.account <> tgt.account_num
                                or (stg.account is null and tgt.account_num is not null)
                                or (stg.account is not null and tgt.account_num is null)
                            ) tmp
                        where tgt.card_num = tmp.card_num
                            and tgt.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                    and tgt.delete_flg = 'N';""")
print("Старые версии обновленных данных Cards закрыты")


# Открытие "новой" версии
cursor_dwh.execute("""  insert into demipt3.mknn_dwh_dim_cards_hist(
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            delete_flg)
                        select
                            stg.card_num,
                            stg.account,
                            stg.update_dt as effective_from,
                            to_timestamp('9999-12-31', 'YYYY-MM-DD') as effective_to,
                            'N' as delete_flg
                        from demipt3.mknn_dwh_stg_cards stg
                        inner join demipt3.mknn_dwh_dim_cards_hist tgt
                            on stg.card_num = tgt.card_num
                            and tgt.effective_to = stg.update_dt - interval '1 second'
	                        and tgt.delete_flg = 'N'
                        where stg.account <> tgt.account_num
                            or (stg.account is null and tgt.account_num is not null)
                            or (stg.account is not null and tgt.account_num is null);""")
print("Новые версии обновленных данных Cards открыты")


# Обработка удалений (формат СКД2)
# Закрытие "старой версии"
cursor_dwh.execute("""  update demipt3.mknn_dwh_dim_cards_hist tgt
                        set effective_to = tmp.effective_to
                        from (
                            select
                                tgt.card_num,
                                now() - interval '1 second' as effective_to
                            from demipt3.mknn_dwh_dim_cards_hist tgt
                            left join demipt3.mknn_dwh_stg_cards_del std
                                on tgt.card_num = std.card_num
                            where std.card_num is null
                                ) tmp
                        where tgt.card_num = tmp.card_num
                          and tgt.card_num not in (select card_num
                                                     from demipt3.mknn_dwh_dim_cards_hist
                                                    where delete_flg = 'Y');""")
print("Старые версии удаленных данных Cards закрыты")


# Открытие "новой" версии
cursor_dwh.execute("""  insert into demipt3.mknn_dwh_dim_cards_hist(
                            card_num,
                            account_num,
                            effective_from,
                            effective_to,
                            delete_flg)
                        select
                            tgt.card_num,
                            tgt.account_num,
                            now() as effective_from,
                            to_timestamp('9999-12-31', 'YYYY-MM-DD') as effective_to,
                            'Y' as delete_flg
                        from demipt3.mknn_dwh_dim_cards_hist tgt
                        left join demipt3.mknn_dwh_stg_cards_del std
                            on tgt.card_num = std.card_num
                        where std.card_num is null
                        and tgt.card_num not in (select card_num
                                                     from demipt3.mknn_dwh_dim_cards_hist
                                                    where delete_flg = 'Y');""")
print("Новые версии удаленных данных Cards открыты")


# Обновление метаданных
cursor_dwh.execute("""  update demipt3.mknn_meta_cards
                        set max_update_dt = coalesce((select max(update_dt) from demipt3.mknn_dwh_stg_cards), (select max_update_dt from demipt3.mknn_meta_cards));""")
print("Метаданные Cards обновлены")


connection_dwh.commit()
print("Транзакция зафиксирована")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # Работа с измерением accounts # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Очистка стейдженговых таблиц
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_accounts;')
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_accounts_del;')
print("Стейджинги Accounts очищены")


# Формирование переменной с метаданными
cursor_dwh.execute('select max_update_dt from demipt3.mknn_meta_accounts')
accounts_max_update_dt = cursor_dwh.fetchone()
print('Метаданные Accounts захвачены')


# Захват данных из источника (изменненых с момента последней загрузки)
cursor_src.execute(f"""select
                            account,
                            valid_to,
                            client,
                            case
                                when update_dt is null then create_dt else update_dt
                            end as mod_dt
                        from info.accounts
                        where update_dt is null and create_dt > to_timestamp('{accounts_max_update_dt[0]}', 'YYYY-MM-DD HH24:MI:SS')
                           or update_dt is not null and update_dt > to_timestamp('{accounts_max_update_dt[0]}', 'YYYY-MM-DD HH24:MI:SS');""")
print("Данные из источника захвачены")


# Формирование датафрейма с использованием пандас
records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )


# Загрузка данных из датафрейма в стейджинг
cursor_dwh.executemany("""  insert into demipt3.mknn_dwh_stg_accounts(
                                account,
                                valid_to,
                                client,
                                update_dt)
                            values (%s, %s, %s, %s);""", df.values.tolist())
print("Данные загружены в стейджинг Accounts")


# Захват ключей для обработки удалений
cursor_src.execute(""" select account from info.accounts;""")

records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

cursor_dwh.executemany("""  insert into demipt3.mknn_dwh_stg_accounts_del(account)
                            values(%s);""", df.values.tolist())
print("Ключи для обработки удалений Accounts захвачены")


# Загрукзка данных в таргет (формат СКД2)
cursor_dwh.execute("""  insert into demipt3.mknn_dwh_dim_accounts_hist(
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            delete_flg)
                        select
                            stg.account,
                            stg.valid_to,
                            stg.client,
                            stg.update_dt,
                            to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
                            'N' as delete_flg
                        from demipt3.mknn_dwh_stg_accounts stg
                        left join demipt3.mknn_dwh_dim_accounts_hist tgt
                        on stg.account = tgt.account_num
                        where tgt.account_num is null""")
print("Новые данные загружены в стейджинг Accounts")


# Обработка обновлений (формат СКД2)
# Закрытие "старой" версии
cursor_dwh.execute("""  update demipt3.mknn_dwh_dim_accounts_hist tgt
                        set effective_to = tmp.effective_to
                        from (
                            select
                                stg.account,
                                stg.update_dt  - interval '1 second' as effective_to
                            from demipt3.mknn_dwh_stg_accounts stg
                            inner join demipt3.mknn_dwh_dim_accounts_hist tgt
                                on stg.account = tgt.account_num
                                and tgt.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                        and tgt.delete_flg = 'N'
                            where 1 = 0
                            or  (stg.valid_to <> tgt.valid_to
                                or (stg.valid_to is null and tgt.valid_to is not null)
                                or (stg.valid_to is not null and tgt.valid_to is null))
                            or  (stg.client <> tgt.client
                                or (stg.client is null and tgt.client is not null)
                                or (stg.client is not null and tgt.client is null))
                            ) tmp
                        where tgt.account_num = tmp.account
                            and tgt.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                    and tgt.delete_flg = 'N';""")
print("Старые версии обновленных данных Accounts закрыты")


# Открытие "новой" версии
cursor_dwh.execute("""  insert into demipt3.mknn_dwh_dim_accounts_hist(
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            delete_flg)
                        select
                            stg.account,
                            stg.valid_to,
                            stg.client,
                            stg.update_dt as effective_from,
                            to_timestamp('9999-12-31', 'YYYY-MM-DD') as effective_to,
                            'N' as delete_flg
                        from demipt3.mknn_dwh_stg_accounts stg
                        inner join demipt3.mknn_dwh_dim_accounts_hist tgt
                            on stg.account = tgt.account_num
                            and tgt.effective_to = stg.update_dt - interval '1 second'
	                        and tgt.delete_flg = 'N'
                        where 1 = 0
                            or  (stg.valid_to <> tgt.valid_to
                                or (stg.valid_to is null and tgt.valid_to is not null)
                                or (stg.valid_to is not null and tgt.valid_to is null))
                            or  (stg.client <> tgt.client
                                or (stg.client is null and tgt.client is not null)
                                or (stg.client is not null and tgt.client is null));""")
print("Новые версии обновленных данных Accounts открыты")


# Обработка удалений (формат СКД2)
# Закрытие "старой версии"
cursor_dwh.execute("""  update demipt3.mknn_dwh_dim_accounts_hist tgt
                        set effective_to = tmp.effective_to
                        from (
                            select
                                tgt.account_num,
                                now() - interval '1 second' as effective_to
                            from demipt3.mknn_dwh_dim_accounts_hist tgt
                            left join demipt3.mknn_dwh_stg_accounts_del std
                                on tgt.account_num = std.account
                            where std.account is null
                            ) tmp
                        where tgt.account_num = tmp.account_num
                          and tgt.account_num not in (select account_num
                                                     from demipt3.mknn_dwh_dim_accounts_hist
                                                    where delete_flg = 'Y');""")
print("Старые версии удаленных данных Accounts закрыты")


# Открытие "новой" версии
cursor_dwh.execute("""  insert into demipt3.mknn_dwh_dim_accounts_hist(
                            account_num,
                            valid_to,
                            client,
                            effective_from,
                            effective_to,
                            delete_flg)
                        select
                            tgt.account_num,
                            tgt.valid_to,
                            tgt.client,
                            now() as effective_from,
                            to_timestamp('9999-12-31', 'YYYY-MM-DD') as effective_to,
                            'Y' as delete_flg
                        from demipt3.mknn_dwh_dim_accounts_hist tgt
                        left join demipt3.mknn_dwh_stg_accounts_del std
                            on tgt.account_num = std.account
                        where std.account is null
                        and tgt.account_num not in (select account_num
                                                     from demipt3.mknn_dwh_dim_cards_hist
                                                    where delete_flg = 'Y');""")
print("Новые версии удаленных данных Accounts открыты")


# Обновление метаданных
cursor_dwh.execute("""  update demipt3.mknn_meta_accounts
                        set max_update_dt = coalesce((select max(update_dt) from demipt3.mknn_dwh_stg_accounts), (select max_update_dt from demipt3.mknn_meta_accounts));""")
print("Метаданные Accounts обновлены")


connection_dwh.commit()
print("Транзакция зафиксирована")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # Работа с измерением clients # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Очистка стейдженговых таблиц
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_clients;')
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_clients_del;')
print("Стейджинги Clients очищены")


# Формирование переменной с метаданными
cursor_dwh.execute('select max_update_dt from demipt3.mknn_meta_clients')
clients_max_update_dt = cursor_dwh.fetchone()
print('Метаданные Clients захвачены')


# Захват данных из источника (изменненых с момента последней загрузки)
cursor_src.execute(f""" select
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            case
                                when update_dt is null then create_dt else update_dt
                            end as mod_dt
                        from info.clients
                        where update_dt is null and create_dt > to_timestamp('{clients_max_update_dt[0]}', 'YYYY-MM-DD HH24:MI:SS')
                           or update_dt is not null and update_dt > to_timestamp('{clients_max_update_dt[0]}', 'YYYY-MM-DD HH24:MI:SS');""")
print("Данные из источника захвачены")


# Формирование датафрейма с использованием пандас
records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )


# Загрузка данных из датафрейма в стейджинг
cursor_dwh.executemany("""  insert into demipt3.mknn_dwh_stg_clients(
                                client_id,
                                last_name,
                                first_name,
                                patronymic,
                                date_of_birth,
                                passport_num,
                                passport_valid_to,
                                phone,
                                update_dt)
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s);""", df.values.tolist())
print("Данные загружены в стейджинг Clients")


# Захват ключей для обработки удалений
cursor_src.execute(""" select client_id from info.clients;""")

records = cursor_src.fetchall()
names = [ x[0] for x in cursor_src.description ]
df = pd.DataFrame( records, columns = names )

cursor_dwh.executemany("""  insert into demipt3.mknn_dwh_stg_clients_del(client_id)
                            values(%s);""", df.values.tolist())
print("Ключи для обработки удалений Clients захвачены")


# Загрукзка данных в таргет (формат СКД2)
cursor_dwh.execute("""  insert into demipt3.mknn_dwh_dim_clients_hist(
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            delete_flg)
                        select
                            stg.client_id,
                            stg.last_name,
                            stg.first_name,
                            stg.patronymic,
                            stg.date_of_birth,
                            stg.passport_num,
                            stg.passport_valid_to,
                            stg.phone,
                            stg.update_dt,
                            to_date('9999-12-31', 'YYYY-MM-DD') as effective_to,
                            'N' as delete_flg
                        from demipt3.mknn_dwh_stg_clients stg
                        left join demipt3.mknn_dwh_dim_clients_hist tgt
                        on stg.client_id = tgt.client_id
                        where tgt.client_id is null""")
print("Новые данные загружены в таргет Clients")


# Обработка обновлений (формат СКД2)
# Закрытие "старой" версии
cursor_dwh.execute("""  update demipt3.mknn_dwh_dim_clients_hist tgt
                        set effective_to = tmp.effective_to
                        from (
                            select
                                stg.client_id,
                                stg.update_dt  - interval '1 second' as effective_to
                            from demipt3.mknn_dwh_stg_clients stg
                            inner join demipt3.mknn_dwh_dim_clients_hist tgt
                                on stg.client_id = tgt.client_id
                                and tgt.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                        and tgt.delete_flg = 'N'
                            where 1 = 0
                            or
                                (   stg.last_name <> tgt.last_name
                                or (stg.last_name is null and tgt.last_name is not null)
                                or (stg.last_name is not null and tgt.last_name is null)
                                )
                            or
                                (   stg.first_name <> tgt.first_name
                                or (stg.first_name is null and tgt.first_name is not null)
                                or (stg.first_name is not null and tgt.first_name is null)
                                )
                            or
                                (   stg.patronymic <> tgt.patronymic
                                or (stg.patronymic is null and tgt.patronymic is not null)
                                or (stg.patronymic is not null and tgt.patronymic is null)
                                )
                            or
                                (    stg.date_of_birth <> tgt.date_of_birth
                                or (stg.date_of_birth is null and tgt.date_of_birth is not null)
                                or (stg.date_of_birth is not null and tgt.date_of_birth is null)
                                )
                            or
                                (   stg.passport_num <> tgt.passport_num
                                or (stg.passport_num is null and tgt.passport_num is not null)
                                or (stg.passport_num is not null and tgt.passport_num is null)
                                )
                            or
                                (   stg.passport_valid_to <> tgt.passport_valid_to
                                or (stg.passport_valid_to is null and tgt.passport_valid_to is not null)
                                or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
                                )
                            or
                                (   stg.phone <> tgt.phone
                                or (stg.phone is null and tgt.phone is not null)
                                or (stg.phone is not null and tgt.phone is null)
                                )
                            ) tmp
                        where tgt.client_id = tmp.client_id
                            and tgt.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                    and tgt.delete_flg = 'N';""")
print("Старые версии обновленных данных Clients закрыты")


# Открытие "новой" версии
cursor_dwh.execute("""  insert into demipt3.mknn_dwh_dim_clients_hist(
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            delete_flg)
                        select
                            stg.client_id,
                            stg.last_name,
                            stg.first_name,
                            stg.patronymic,
                            stg.date_of_birth,
                            stg.passport_num,
                            stg.passport_valid_to,
                            stg.phone,
                            stg.update_dt as effective_from,
                            to_timestamp('9999-12-31', 'YYYY-MM-DD') as effective_to,
                            'N' as delete_flg
                        from demipt3.mknn_dwh_stg_clients stg
                        inner join demipt3.mknn_dwh_dim_clients_hist tgt
                            on stg.client_id = tgt.client_id
                            and tgt.effective_to = stg.update_dt - interval '1 second'
	                        and tgt.delete_flg = 'N'
                        where 1 = 0
                            or
                                (   stg.last_name <> tgt.last_name
                                or (stg.last_name is null and tgt.last_name is not null)
                                or (stg.last_name is not null and tgt.last_name is null)
                                )
                            or
                                (   stg.first_name <> tgt.first_name
                                or (stg.first_name is null and tgt.first_name is not null)
                                or (stg.first_name is not null and tgt.first_name is null)
                                )
                            or
                                (   stg.patronymic <> tgt.patronymic
                                or (stg.patronymic is null and tgt.patronymic is not null)
                                or (stg.patronymic is not null and tgt.patronymic is null)
                                )
                            or
                                (    stg.date_of_birth <> tgt.date_of_birth
                                or (stg.date_of_birth is null and tgt.date_of_birth is not null)
                                or (stg.date_of_birth is not null and tgt.date_of_birth is null)
                                )
                            or
                                (   stg.passport_num <> tgt.passport_num
                                or (stg.passport_num is null and tgt.passport_num is not null)
                                or (stg.passport_num is not null and tgt.passport_num is null)
                                )
                            or
                                (   stg.passport_valid_to <> tgt.passport_valid_to
                                or (stg.passport_valid_to is null and tgt.passport_valid_to is not null)
                                or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
                                )
                            or
                                (   stg.phone <> tgt.phone
                                or (stg.phone is null and tgt.phone is not null)
                                or (stg.phone is not null and tgt.phone is null)
                                );""")
print("Новые версии обновленных данных Clients открыты")


# Обработка удалений (формат СКД2)
# Закрытие "старой версии"
cursor_dwh.execute("""  update demipt3.mknn_dwh_dim_clients_hist tgt
                        set effective_to = tmp.effective_to
                        from (
                            select
                                tgt.client_id,
                                now() - interval '1 second' as effective_to
                            from demipt3.mknn_dwh_dim_clients_hist tgt
                            left join demipt3.mknn_dwh_stg_clients_del std
                                on tgt.client_id = std.client_id
                            where std.client_id is null
                            ) tmp
                        where tgt.client_id = tmp.client_id
                          and tgt.client_id not in (select client_id
                                                     from demipt3.mknn_dwh_dim_clients_hist
                                                    where delete_flg = 'Y');""")
print("Старые версии удаленных данных Clients закрыты")


# Открытие "новой" версии
cursor_dwh.execute("""  insert into demipt3.mknn_dwh_dim_clients_hist(
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            effective_from,
                            effective_to,
                            delete_flg)
                        select
                            tgt.client_id,
                            tgt.last_name,
                            tgt.first_name,
                            tgt.patronymic,
                            tgt.date_of_birth,
                            tgt.passport_num,
                            tgt.passport_valid_to,
                            tgt.phone,
                            now() as effective_from,
                            to_timestamp('9999-12-31', 'YYYY-MM-DD') as effective_to,
                            'Y' as delete_flg
                        from demipt3.mknn_dwh_dim_clients_hist tgt
                        left join demipt3.mknn_dwh_stg_clients_del std
                            on tgt.client_id = std.client_id
                        where std.client_id is null
                        and tgt.client_id not in (select client_id
                                                     from demipt3.mknn_dwh_dim_clients_hist
                                                    where delete_flg = 'Y');""")
print("Новые версии удаленных данных Clients открыты")


# Обновление метаданных
cursor_dwh.execute("""  update demipt3.mknn_meta_clients
                        set max_update_dt = coalesce((select max(update_dt) from demipt3.mknn_dwh_stg_clients), (select max_update_dt from demipt3.mknn_meta_clients));""")
print("Метаданные Clients обновлены")

connection_dwh.commit()
print("Транзакция зафиксирована")


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # Работа с измерением terminals # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Очистка стейдженговых таблиц
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_terminals;')
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_terminals_del;')
print("Стейджинги Terminals очищены")


# Захват данных из источника
# Поиск файла-источника
path = r"/home/demipt3/mknn/project"
backup = r"/home/demipt3/mknn/project/archive"
x_file = 'terminals'
src_file = ''

for root, dirs, files in os.walk(path):
    for file in sorted(files):
        if x_file in file:
            src_file = rf'{path}/{file}'
            print(f'Найден файл-источник: {src_file}')
            break
    break


# Захват данных из источника (xlsx-файл) с использованием пандас
df = pd.read_excel(rf'{src_file}', sheet_name='terminals', header=0, index_col=None)
print("Данные из источника захвачены")


# Загрузка данных из датафрейма в стейджинг
cursor_dwh.executemany("""  insert into demipt3.mknn_dwh_stg_terminals(
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address)
                            values(%s, %s, %s, %s);""", df.values.tolist())
print("Данные загружены в стейджинг Terminals")


# Захват ключей для обработки удалений
cursor_dwh.execute("""  insert into demipt3.mknn_dwh_stg_terminals_del(terminal_id)
                        select terminal_id from demipt3.mknn_dwh_stg_terminals;""")
print("Ключи для обработки удалений Terminals захвачены")


# Формирование переменной с временем обновления
cursor_dwh.execute('select now()')
now_time = cursor_dwh.fetchone()
print('Переменная времени обновления сформирована')


# Загрукзка данных в таргет (формат СКД2)
cursor_dwh.execute(f"""     insert into demipt3.mknn_dwh_dim_terminals_hist(
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                effective_from,
                                effective_to,
                                delete_flg)
                            select
                                stg.terminal_id,
                                stg.terminal_type,
                                stg.terminal_city,
                                stg.terminal_address,
                                to_timestamp('{now_time[0]}', 'YYYY-MM-DD HH24:MI:SS') as effective_from,
                                to_timestamp('9999-12-31', 'YYYY-MM-DD') as effective_to,
                                'N' as delete_flg
                            from demipt3.mknn_dwh_stg_terminals stg
                            left join demipt3.mknn_dwh_dim_terminals_hist tgt
                                on stg.terminal_id = tgt.terminal_id
                            where tgt.terminal_id is null;
                        """)
print("Новые данные загружены в таргет Terminals")


# Обработка обновлений (формат СКД2)
# Закрытие "старой" версии (не копировать, effective_to - now())
cursor_dwh.execute(f""" update demipt3.mknn_dwh_dim_terminals_hist tgt
                        set effective_to = tmp.effective_to
                        from (
                            select
                                stg.terminal_id,
                                to_timestamp('{now_time[0]}', 'YYYY-MM-DD HH24:MI:SS')  - interval '1 second' as effective_to
                            from demipt3.mknn_dwh_stg_terminals stg
                            inner join demipt3.mknn_dwh_dim_terminals_hist tgt
                                on stg.terminal_id = tgt.terminal_id
                                and tgt.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                        and tgt.delete_flg = 'N'
                            where   1 = 0
                            or
                                (   stg.terminal_type <> tgt.terminal_type
                                or (stg.terminal_type is null and tgt.terminal_type is not null)
                                or (stg.terminal_type is not null and tgt.terminal_type is null)
                                )
                            or
                                (   stg.terminal_city <> tgt.terminal_city
                                or (stg.terminal_city is null and tgt.terminal_city is not null)
                                or (stg.terminal_city is not null and tgt.terminal_city is null)
                                )
                            or
                                (   stg.terminal_address <> tgt.terminal_address
                                or (stg.terminal_address is null and tgt.terminal_address is not null)
                                or (stg.terminal_address is not null and tgt.terminal_address is null)
                                )
                            ) tmp
                        where tgt.terminal_id = tmp.terminal_id
                        and tgt.effective_to = to_timestamp('9999-12-31', 'YYYY-MM-DD')
		                and tgt.delete_flg = 'N';""")
print("Старые версии обновленных данных Terminals закрыты")


# Открытие "новой" версии
cursor_dwh.execute(f"""  insert into demipt3.mknn_dwh_dim_terminals_hist(
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            effective_from,
                            effective_to,
                            delete_flg)
                        select
                            stg.terminal_id,
                            stg.terminal_type,
                            stg.terminal_city,
                            stg.terminal_address,
                            to_timestamp('{now_time[0]}', 'YYYY-MM-DD HH24:MI:SS') as effective_from,
                            to_timestamp('9999-12-31', 'YYYY-MM-DD') as effective_to,
                            'N' as delete_flg
                        from demipt3.mknn_dwh_stg_terminals stg
                        inner join demipt3.mknn_dwh_dim_terminals_hist tgt
                            on stg.terminal_id = tgt.terminal_id
                            and tgt.effective_to = to_timestamp('{now_time[0]}', 'YYYY-MM-DD HH24:MI:SS') - interval '1 second'
                            and tgt.delete_flg = 'N'
                        where 1 = 0
                            or
                                (   stg.terminal_type <> tgt.terminal_type
                                or (stg.terminal_type is null and tgt.terminal_type is not null)
                                or (stg.terminal_type is not null and tgt.terminal_type is null)
                                )
                            or
                                (   stg.terminal_city <> tgt.terminal_city
                                or (stg.terminal_city is null and tgt.terminal_city is not null)
                                or (stg.terminal_city is not null and tgt.terminal_city is null)
                                )
                            or
                                (   stg.terminal_address <> tgt.terminal_address
                                or (stg.terminal_address is null and tgt.terminal_address is not null)
                                or (stg.terminal_address is not null and tgt.terminal_address is null)
                                );""")
print("Новые версии обновленных данных Terminals открыты")


# Обработка удалений (формат СКД2)
# Закрытие "старой версии"
cursor_dwh.execute(f"""  update demipt3.mknn_dwh_dim_terminals_hist tgt
                        set effective_to = tmp.effective_to
                        from (
                            select
                                tgt.terminal_id,
                                to_timestamp('{now_time[0]}', 'YYYY-MM-DD HH24:MI:SS')  - interval '1 second' as effective_to
                            from demipt3.mknn_dwh_dim_terminals_hist tgt
                            left join demipt3.mknn_dwh_stg_terminals_del std
                                on tgt.terminal_id = std.terminal_id
                            where std.terminal_id is null
                            ) tmp
                        where tgt.terminal_id = tmp.terminal_id
                          and tgt.terminal_id not in (select terminal_id
                                                     from demipt3.mknn_dwh_dim_terminals_hist
                                                    where delete_flg = 'Y');""")
print("Старые версии удаленных данных Terminals закрыты")


# Открытие "новой" версии
cursor_dwh.execute(f"""  insert into demipt3.mknn_dwh_dim_terminals_hist(
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address,
                            effective_from,
                            effective_to,
                            delete_flg)
                        select
                            tgt.terminal_id,
                            tgt.terminal_type,
                            tgt.terminal_city,
                            tgt.terminal_address,
                            to_timestamp('{now_time[0]}', 'YYYY-MM-DD HH24:MI:SS') as effective_from,
                            to_timestamp('9999-12-31', 'YYYY-MM-DD') as effective_to,
                            'Y' as delete_flg
                        from demipt3.mknn_dwh_dim_terminals_hist tgt
                        left join demipt3.mknn_dwh_stg_terminals_del std
                            on tgt.terminal_id = std.terminal_id
                        where std.terminal_id is null
                        and tgt.terminal_id not in (select terminal_id
                                                     from demipt3.mknn_dwh_dim_terminals_hist
                                                    where delete_flg = 'Y');""")
print("Новые версии удаленных данных Terminals открыты")


connection_dwh.commit()
print("Транзакция зафиксирована")


os.rename(rf'{src_file}', rf'{src_file}.backup')
print(rf'Создана архивная копия {file}.backup')


os.replace(rf'{src_file}.backup', rf'{backup}/{file}.backup')
print(rf'Архивная копия перемещена в каталог {backup}')

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # Работа с фактом blacklist # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

#Очистка стейдженговых таблиц
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_blacklist;')
print("Стейджинг Blacklist очищены")


# Захват данных из источника
# Поиск файла-источника
path = r"/home/demipt3/mknn/project"
backup = r"/home/demipt3/mknn/project/archive"
x_file = 'blacklist'
src_file = ''

for root, dirs, files in os.walk(path):
    for file in sorted(files):
    	if x_file in file:
            src_file = rf'{path}/{file}'
            print(f'Найден файл-источник: {src_file}')
            break
    break


# Создание датафрейма с использованием пандас
# Захват данных из источника (xlsx-файл) с использованием пандас
df = pd.read_excel(rf'{src_file}', sheet_name='blacklist', header=0, index_col=None)
print("Данные из источника захвачены")


# Загрузка данных из датафрейма в стейджинг
cursor_dwh.executemany("""  insert into demipt3.mknn_dwh_stg_blacklist(
                                entry_dt,
                                passport_num)
                            values(%s, %s);""", df.values.tolist())
print("Данные загружены в стейджинг Blacklist")


# Загрузка данных в факт
cursor_dwh.execute("""  insert into demipt3.mknn_dwh_fact_passport_blacklist(
                                entry_dt,
                                passport_num)
                        select
                            stg.entry_dt,
                            stg.passport_num
                        from demipt3.mknn_dwh_stg_blacklist stg
                        left join demipt3.mknn_dwh_fact_passport_blacklist tgt
                            on stg.passport_num = tgt.passport_num
                        where tgt.passport_num is null;""")
print("Новые данные загружены в факт Blacklist")


connection_dwh.commit()
print("Транзакция зафиксирована")


os.rename(rf'{src_file}', rf'{src_file}.backup')
print(rf'Создана архивная копия {file}.backup')


os.replace(rf'{src_file}.backup', rf'{backup}/{file}.backup')
print(rf'Архивная копия перемещена в каталог {backup}')


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # Работа с фактом transactions # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

#Захват данных из источника
#Поиск файла-источника
path = r"/home/demipt3/mknn/project"
backup = r"/home/demipt3/mknn/project/archive"
x_file = 'transactions'
src_file = ''

print('Поиск файла-источника')
for root, dirs, files in os.walk(path):
    for file in sorted(files):
        if x_file in file:
            src_file = rf'{path}/{file}'
            print(f'Найден файл-источник: {src_file}')
            break
    break


# Очистка данных из источника (приведение данных в нужный для загрузки вид)
with open(rf"{src_file}", 'r') as r:
    old_data = r.read()

replace_comma = old_data.replace(',', '.')

with open(rf"{src_file}", 'w') as w:
    w.write(replace_comma)


# Формирование датафрейма с использованием пандас
# Захват данных из источника (txt-файл) с использованием пандас
df = pd.read_csv(rf"{src_file}", sep=';', header=0)


# Загрузка данных в факт transactions
# bar = IncrementalBar('Countdown', max = len(df)) #Прогресс-бар

for row in df.itertuples():
    cursor_dwh.execute("""
                        insert into demipt3.mknn_dwh_fact_transactions(
                                trans_id,
                                trans_date,
                                amt,
                                card_num,
                                oper_type,
                                oper_result,
                                terminal)
                        values (%s, %s, %s, %s, %s, %s, %s)
                        """,
                       (row.transaction_id,
                        row.transaction_date,
                        row.amount,
                        row.card_num,
                        row.oper_type,
                        row.oper_result,
                        row.terminal))
#     bar.next()    # Прогресс-бар
#
# bar.finish()      # Прогресс-бар
# print()           # Прогресс-бар

#   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #
#   #   #   #   #   #   #   #   #   #   #   #   Второй вариант загрузки #   #   #   #   #   #   #   #   #   #   #   #
#   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #
# # # cursor_dwh.executemany("""  insert into demipt3.mknn_dwh_stg_transactions(
# # #                               transaction_id,
# # #                               transaction_date,
# # #                               amount,
# # #                               card_num,
# # #                               oper_type,
# # #                               oper_result,
# # #                               terminal)
# # #                             values (%s, %s, %s, %s, %s, %s, %s)""", df.values.tolist())
#   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #
#   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #
#   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #   #

connection_dwh.commit()
print("Транзакция зафиксирована")


os.rename(rf'{src_file}', rf'{src_file}.backup')
print(rf'Создана архивная копия {file}.backup')


os.replace(rf'{src_file}.backup', rf'{backup}/{file}.backup')
print(rf'Архивная копия перемещена в каталог {backup}')

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # Работа с отчетом fraud # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Формирование переменной с метаданными
cursor_dwh.execute('select rep_date from demipt3.mknn_meta_rep')
rep_date = cursor_dwh.fetchone()
print('Метаданные Report захвачены')


# # # # # # # # # # # # # # Операции при просроченном или заблокированном паспорте # # # # # # # # # # # # # # # # # #

#Очистка стейдженговых таблиц
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_rep_fraud;')
print("Стейджинг Report очищен")


cursor_dwh.execute(f""" insert into demipt3.mknn_dwh_stg_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
                        select
                            t.trans_date as event_dt,
                            cl.passport_num as passport,
                            cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic as fio,
                            cl.phone as phone,
                            trim(to_char(1,'9')) as event_type,
                            to_date('{rep_date[0]}', 'YYYY-MM-DD') as report_dt
                        from demipt3.mknn_dwh_fact_transactions t
                        inner join demipt3.mknn_dwh_dim_cards_hist c
                            on trim(t.card_num) = trim(c.card_num)
                        inner join demipt3.mknn_dwh_dim_accounts_hist a
                            on c.account_num = a.account_num
                        inner join demipt3.mknn_dwh_dim_clients_hist cl
                            on a.client = cl.client_id
                        where t.trans_date >= cl.passport_valid_to /* - interval '5 day'  Для проверки */
                            or cl.passport_num in (select passport_num from demipt3.mknn_dwh_fact_passport_blacklist where t.trans_date >= entry_dt);""")
print('Операции при просроченном или заблокированном паспорте загружены в стейджинг')


cursor_dwh.execute("""  insert into demipt3.mknn_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
                        select
                            stg.event_dt,
                            stg.passport,
                            stg.fio,
                            stg.phone,
                            stg.event_type,
                            stg.report_dt
                        from demipt3.mknn_dwh_stg_rep_fraud stg
                        left join demipt3.mknn_rep_fraud tgt
                            on stg.event_dt = tgt.event_dt
                            and stg.passport = tgt.passport
                        where tgt.event_dt is null;""")
print('Отчет по операциям при просроченном или заблокированном паспорте сформирован')


connection_dwh.commit()
print("Транзакция зафиксирована")


# # # # # # # # # # # # # # # # # # # Операции при недействующем договоре # # # # # # # # # # # # # # # # # # # # # # #


#Очистка стейдженговых таблиц
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_rep_fraud;')
print("Стейджинг Report очищен")


cursor_dwh.execute(f""" insert into demipt3.mknn_dwh_stg_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
                        select
                            t.trans_date as event_dt,
                            cl.passport_num as passport,
                            cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic as fio,
                            cl.phone as phone,
                            trim(to_char(2,'9')) as event_type,
                            to_date('{rep_date[0]}', 'YYYY-MM-DD') as report_dt
                        from demipt3.mknn_dwh_fact_transactions t
                        inner join demipt3.mknn_dwh_dim_cards_hist c
                            on trim(t.card_num) = trim(c.card_num)
                        inner join demipt3.mknn_dwh_dim_accounts_hist a
                            on c.account_num = a.account_num
                        inner join demipt3.mknn_dwh_dim_clients_hist cl
                            on a.client = cl.client_id
                        where t.trans_date > a.valid_to /* - interval '5 day'  Для проверки */;""")
print('Операции при недействующем договоре загружены в стейджинг')


cursor_dwh.execute("""  insert into demipt3.mknn_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
                        select
                            stg.event_dt,
                            stg.passport,
                            stg.fio,
                            stg.phone,
                            stg.event_type,
                            stg.report_dt
                        from demipt3.mknn_dwh_stg_rep_fraud stg
                        left join demipt3.mknn_rep_fraud tgt
                            on stg.event_dt = tgt.event_dt
                            and stg.passport = tgt.passport
                        where tgt.event_dt is null;""")
print('Отчет по операциям при недействующем договоре сформирован')


connection_dwh.commit()
print("Транзакция зафиксирована")


# # # # # # # # # # # # # # # # # # # # # Операции в разных городах # # # # # # # # # # # # # # # # # # # # # # # # # #

#Очистка стейдженговых таблиц
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_rep_fraud;')
print("Стейджинг Report очищен")


cursor_dwh.execute(f""" insert into demipt3.mknn_dwh_stg_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
                        select
                            tmp.lead_time as event_dt,
                            cl.passport_num as passport,
                            cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic as fio,
                            cl.phone as phone,
                            trim(to_char(3,'9')) as event_type,
                            to_date('{rep_date[0]}', 'YYYY-MM-DD') as report_dt
                        from (  select
                                    ts.trans_id,
                                    ts.trans_date,
                                    ts.card_num,
                                    tr.terminal_id,
                                    tr.terminal_city,
                                    lead(terminal_city) over (partition by card_num order by trans_date) as lead_city,
                                    lead(trans_date) over (partition by card_num order by trans_date) as lead_time,
                                    lead(trans_date) over (partition by card_num order by trans_date) - trans_date as diff_time
                                from demipt3.mknn_dwh_fact_transactions ts
                                inner join demipt3.mknn_dwh_dim_terminals_hist tr 
                                    on ts.terminal = tr.terminal_id
                             ) tmp
                        inner join demipt3.mknn_dwh_dim_cards_hist c
                            on trim(tmp.card_num) = trim(c.card_num)
                        inner join demipt3.mknn_dwh_dim_accounts_hist a
                            on c.account_num = a.account_num
                        inner join demipt3.mknn_dwh_dim_clients_hist cl
                            on a.client = cl.client_id
                        where terminal_city <> lead_city and diff_time <= interval '1 hour';""")
print('Операции в разных городах в течение одного часа добавлены в стейджинг')


cursor_dwh.execute("""  insert into demipt3.mknn_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
                        select
                            stg.event_dt,
                            stg.passport,
                            stg.fio,
                            stg.phone,
                            stg.event_type,
                            stg.report_dt
                        from demipt3.mknn_dwh_stg_rep_fraud stg
                        left join demipt3.mknn_rep_fraud tgt
                            on stg.event_dt = tgt.event_dt
                            and stg.passport = tgt.passport
                        where tgt.event_dt is null;""")
print('Отчет по операциям в разных городах в течение одного часа сформирован')


connection_dwh.commit()
print("Транзакция зафиксирована")


# # # # # # # # # # # # # # # # # # # # # # Попытка подбора суммы # # # # # # # # # # # # # # # # # # # # # # # # # # #

#Очистка стейдженговых таблиц
cursor_dwh.execute('delete from demipt3.mknn_dwh_stg_rep_fraud;')
print("Стейджинг Report очищен")


cursor_dwh.execute(f""" insert into demipt3.mknn_dwh_stg_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
                        select
                            tmp.trans_date as event_dt,
                            cl.passport_num as passport,
                            cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic as fio,
                            cl.phone as phone,
                            trim(to_char(4,'9')) as event_type,
                            to_date('{rep_date[0]}', 'YYYY-MM-DD') as report_dt
                        from (  select 
									trans_id,
									trans_date,
									lag(trans_date, 3) over (partition by card_num order by trans_date) as lag3_time,
									trans_date - lag(trans_date, 3) over (partition by card_num order by trans_date) as diff_time,
									card_num,
									oper_type,
									amt,
									oper_result,
									lag(amt) over (partition by card_num order by trans_date) as lag_amt,
									lag(oper_result) over (partition by card_num order by trans_date) as lag_result,
									lag(amt, 2) over (partition by card_num order by trans_date) as lag2_amt,
									lag(oper_result, 2) over (partition by card_num order by trans_date) as lag2_result,
									lag(amt, 3) over (partition by card_num order by trans_date) as lag3_amt,
									lag(oper_result, 3) over (partition by card_num order by trans_date) as lag3_result
								from demipt3.mknn_dwh_fact_transactions t
								where oper_type = 'WITHDRAW' or oper_type = 'PAYMENT'
                             ) tmp
                        inner join demipt3.mknn_dwh_dim_cards_hist c
                            on trim(tmp.card_num) = trim(c.card_num)
                        inner join demipt3.mknn_dwh_dim_accounts_hist a
                            on c.account_num = a.account_num
                        inner join demipt3.mknn_dwh_dim_clients_hist cl
                            on a.client = cl.client_id
                        where tmp.oper_result = 'SUCCESS'
							and tmp.lag_result = 'REJECT'
							and tmp.lag2_result = 'REJECT'
							and tmp.lag3_result = 'REJECT'
							and tmp.lag_amt > amt
							and tmp.lag2_amt > lag_amt
							and tmp.lag3_amt > lag2_amt
							and tmp.diff_time <= interval '20 minute';""")
print('Операции попыток подбора суммы добавлены в стейджинг')


cursor_dwh.execute("""  insert into demipt3.mknn_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
                        select
                            stg.event_dt,
                            stg.passport,
                            stg.fio,
                            stg.phone,
                            stg.event_type,
                            stg.report_dt
                        from demipt3.mknn_dwh_stg_rep_fraud stg
                        left join demipt3.mknn_rep_fraud tgt
                            on stg.event_dt = tgt.event_dt
                            and stg.passport = tgt.passport
                        where tgt.event_dt is null;""")
print('Отчет по операциям попыток подбора суммы сформирован')


# Обновление метаданных
cursor_dwh.execute("""  update demipt3.mknn_meta_rep
                        set rep_date = rep_date + interval '1 day';""")
print("Метаданные Report обновлены")

connection_dwh.commit()
print("Транзакция зафиксирована")

