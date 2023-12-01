import pandas as pd
import psycopg2
from datetime import date

from .connectors import VitalityBooster as vb
from .connectors import DataBase_rj as dbase

select = """select 	c.id,
		c."name",
		c.middle_name,
		c.surname,
		c.known_as,
		c.gender,
		c.nationality,
		c.place_of_birth,
		c.date_of_birth::varchar,
		p.id as pass_id,
		p.country as pass_country,
		p."number" as pass_number,
		p."name" as pass_name,
		p.surname as pass_surname,
		p.date_of_issue::varchar,
		p.expiry_date::varchar,
		p.is_default,
		p.notes as pass_note
			from clients_data.clients c 
			left join clients_data.clients_data.passport p on p.client_id = c.id 
			left join clients_data.clients_data.gender g on g.id = c.gender 
			{filter}"""

client_id_sql = """where c.id = {client_id}"""
surname_sql = """where c.surname ~* '{surname}'"""
pass_sql = """where p."number" ~* '{passport}'"""
surname_pass_sql = """where c."name" ~* '{surname}'
                        and p."number" ~* '{passport}'"""

sql_insert_deals = """insert into clients_data.deals 
values ('{deal_id_point}', {client_id_point}, '{leg_id_point}');"""

sql_insert_new_client = """INSERT INTO clients_data.clients  ("name", 
                                                            "middle_name", 
                                                            "surname", 
                                                            "known_as", 
                                                            "gender", 
                                                            "nationality", 
                                                            "place_of_birth", 
                                                            "date_of_birth") 
VALUES {insert_here}
                    ;
"""

sql_update_client = """INSERT INTO clients_data.clients  (id,
                                                            name,
                                                            middle_name,
                                                            surname,
                                                            known_as,
                                                            gender,
                                                            nationality,
                                                            place_of_birth,
                                                            date_of_birth) 
VALUES {insert_here}
on conflict (id) do update
    set 
        name = excluded.name,
        middle_name = excluded.middle_name,
        surname = excluded.surname,
        known_as = excluded.known_as,
        gender = excluded.gender,
        nationality = excluded.nationality,
        place_of_birth = excluded.place_of_birth,
        date_of_birth = excluded.date_of_birth
                    ;
"""

sql_select_max_seq_client = """SELECT currval('clients_data.clients_id_seq') as id;"""

sql_insert_new_pass = """INSERT INTO clients_data.passport  ("client_id", 
                                                            "country", 
                                                            "number", 
                                                            "name", 
                                                            "surname", 
                                                            "date_of_issue", 
                                                            "expiry_date", 
                                                            "is_default", 
                                                            "notes") 
VALUES {insert_here}
                    ;
"""

sql_update_pass = """INSERT INTO clients_data.passport  (id,
                                                        client_id,
                                                        country,
                                                        number,
                                                        name,
                                                        surname,
                                                        date_of_issue,
                                                        expiry_date,
                                                        is_default,
                                                        notes) 
VALUES {insert_here}
on conflict (id) do update
    set 
        client_id = excluded.client_id,
        country = excluded.country,
        number = excluded.number,
        name = excluded.name,
        surname = excluded.surname,
        date_of_issue = excluded.date_of_issue,
        expiry_date = excluded.expiry_date,
        is_default = excluded.is_default,
        notes = excluded.notes
                    ;
"""

sql_select_max_seq_pass = """SELECT currval('clients_data.passport_id_seq') as id;"""

sql_delete_client = """delete from clients_data.clients where id = {del_client_id}"""

def get_data(surname_get: str, password_get: str, client_id_get: int = None, type: str = "json") -> pd.DataFrame:

    #database connect
    db = vb.MessengerSQL(dbase.PostgreSQL())
    db.connect()

    if client_id_get is not None:
        sql = client_id_sql.format(client_id=client_id_get)
    elif surname_get != None and password_get != None:
        sql = surname_pass_sql.format(surname=surname_get, passport=password_get)
    elif surname_get != None and password_get == None:
        sql = surname_sql.format(surname=surname_get)
    elif surname_get == None and password_get != None:
        sql = pass_sql.format(passport=password_get)
    else:
        sql = ""

    dataframe = db.send_command(select.format(filter=sql))
    if type == "json":
        return dataframe.to_json(orient="records")
    elif type == "dict":
        return dataframe.to_dict(orient="records")


def insert_deals(deal_id: str, client_id: int, leg_id: str):

    # database connect
    db = vb.MessengerSQL(dbase.PostgreSQL())
    db.connect()

    insert_deals_with_values = sql_insert_deals.format(deal_id_point=deal_id, client_id_point=client_id, leg_id_point=leg_id)
    db.send_command_no_data(insert_deals_with_values)


def insert_client(name: str, middle_name: str, surname: str, known_as: str, gender: int, nationality: str,
                  place_of_birth: str, date_of_birth: date):
    
    insert_data = [{
                "name": name,
                 "middle_name": middle_name,
                 "surname": surname,
                 "known_as": known_as,
                 "gender": gender,
                 "nationality": nationality,
                 "place_of_birth": place_of_birth,
                 "date_of_birth": date_of_birth
    }]

    insert_dataframe = pd.DataFrame(insert_data)

    # database connect
    db = vb.MessengerSQL(dbase.PostgreSQL())
    db.connect()

    db.execute_method(sql_insert_new_client, insert_dataframe)
    created_id = db.send_command(sql_select_max_seq_client)['id'][0]
    return created_id


def update_client(client_id: int, name: str, middle_name: str, surname: str, known_as: str, gender: int, nationality: str,
                  place_of_birth: str, date_of_birth: date):
    insert_data = [{
        "id": client_id,
        "name": name,
        "middle_name": middle_name,
        "surname": surname,
        "known_as": known_as,
        "gender": gender,
        "nationality": nationality,
        "place_of_birth": place_of_birth,
        "date_of_birth": date_of_birth
    }]

    insert_dataframe = pd.DataFrame(insert_data)

    # database connect
    db = vb.MessengerSQL(dbase.PostgreSQL())
    db.connect()

    db.execute_method(sql_update_client, insert_dataframe)


def insert_pass(new_client_id: int, pass_country: str, pass_number: str, pass_name: str, pass_surname: str,
                date_of_issue: date, expiry_date: date, is_default: bool, pass_note: str):
    insert_data = [{
        "client_id": new_client_id,
        "country": pass_country,
        "number": pass_number,
        "name": pass_name,
        "surname": pass_surname,
        "date_of_issue": date_of_issue,
        "expiry_date": expiry_date,
        "is_default": is_default,
        "notes": pass_note
    }]

    insert_dataframe = pd.DataFrame(insert_data)

    # database connect
    db = vb.MessengerSQL(dbase.PostgreSQL())
    db.connect()

    db.execute_method(sql_insert_new_pass, insert_dataframe)
    created_id = db.send_command(sql_select_max_seq_pass)['id'][0]
    return created_id


def update_pass(pass_id: int, client_id: int, pass_country: str, pass_number: str, pass_name: str, pass_surname: str,
                date_of_issue: date, expiry_date: date, is_default: bool, pass_note: str):
    insert_data = [{
        "id": pass_id,
        "client_id": client_id,
        "country": pass_country,
        "number": pass_number,
        "name": pass_name,
        "surname": pass_surname,
        "date_of_issue": date_of_issue,
        "expiry_date": expiry_date,
        "is_default": is_default,
        "notes": pass_note
    }]

    insert_dataframe = pd.DataFrame(insert_data)

    # database connect
    db = vb.MessengerSQL(dbase.PostgreSQL())
    db.connect()

    try:
        db.execute_method(sql_update_pass, insert_dataframe)
        return "OK"
    except psycopg2.errors.UniqueViolation as err:
        print(err)
        return "UniqueViolationErr"


def delete_client(client_id: int):

    # database connect
    db = vb.MessengerSQL(dbase.PostgreSQL())
    db.connect()

    db.send_command_no_data(sql_delete_client.format(del_client_id=client_id))
