import os
import multiprocessing as mp
import json
import sqlite3
import numpy as np
from scipy.stats import norm

"""
Simulation Parameters
"""
ECONOMIC_SCENARIOS = 10_000_000
PORTFOLIO_SIZE = 500

RATINGS_IN_USE = 'SP'
YEARS_OF_INTEREST = [1, 3, 5, 10, 15]

RATES_OF_INTEREST = ['AA', 'A', 'BBB', 'BB', 'B','CCCtoC', 'inv_grad', 'spec_grad']

ASSET_CORRELATION = 0.16

BATCH_SIZE = 500
MAX_QUEUE_SIZE = 40

""" 
Filepaths
"""
if RATINGS_IN_USE == 'SP':
    CREDIT_RATING_FILEPATH = 'dependencies/sp_global_ratings_default.json'
elif RATINGS_IN_USE == 'FITCH':
    CREDIT_RATING_FILEPATH = 'dependencies/fitch_ratings_default.json'
else:
    print("Check RATINGS_IN_USE variable.")
    exit()

DB_FOLDER = 'database_files_mid_rho/'


"""
Lists to support SQL queries for the given simulation parameters
"""
create_table_base_columns = [
            "S_ID INTEGER PRIMARY KEY",
            "Y DECIMAL(8)"
           ]
insert_to_table_base_columns = [
                "S_ID",
                "Y"]
temp_columns = []
for i in RATES_OF_INTEREST:
    col_to_add = i+"_L_RATIO"
    temp_columns.append(col_to_add)
for i in temp_columns:
    col_to_add = i+" DECIMAL(6)"
    create_table_base_columns.append(col_to_add)
for i in temp_columns:
    insert_to_table_base_columns.append(i)

def create_sql_command(mode: str):
    if mode == "create":
        columns_sql = ",\n    ".join(create_table_base_columns)

        sql = f'''
            CREATE TABLE IF NOT EXISTS simulation (
                {columns_sql}
            )
        '''

    elif mode == "insert":
        columns_sql = ",\n    ".join(insert_to_table_base_columns)
        placeholders = ",\n    ".join([f":{col}" for col in insert_to_table_base_columns])

        sql = f'''
            INSERT INTO simulation (
                {columns_sql}
            ) VALUES (
                {placeholders}
            )
        '''
    return sql

def create_database(create_table_SQL: str, db_name: str):
    # One by one creates databases for the simulation, if not exist
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(create_table_SQL)
        conn.commit()

    except sqlite3.Error as e:
        print(f"An error occured while creating {db_name} at create_database function: {e} \n Program finished.")
        exit()
    finally:
        conn.close()

def data_consumer(queue: mp.Queue, db_name: str, insert_statement: str):
    # Uploads the fed data into a DB file
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    in_transaction = False
    try:
        while True:
            data = queue.get()
            if data is None:
                break

            if not in_transaction:
                cursor.execute('BEGIN TRANSACTION')
                in_transaction = True

            cursor.executemany(insert_statement, data)

            conn.commit()
            in_transaction = False

    except sqlite3.Error as e:
        print(f"An error occurred at data_consumer function ({db_name} data consumer). {e}. \n Program finished. ")
        if in_transaction:
            cursor.execute('ROLLBACK')
    finally:
        if in_transaction:
            conn.commit()
        conn.close()

def simulate_losses_monte_carlo(queue: mp.Queue, yearForRates: int,
                                asset_value_correlation: float = ASSET_CORRELATION):

    # load thresholds from json file
    with open(CREDIT_RATING_FILEPATH, 'r') as f:
        whole_rating_list = json.load(f)
        parsed_ratings_list = {}

        for i in whole_rating_list:
            if i['Year'] == yearForRates:
                rating_list = i
                for k in RATES_OF_INTEREST:
                    pct = rating_list[k] / 100
                    parsed_ratings_list[k] = {"pct":pct, "ppf":norm.ppf(pct), "default_count":0}
                break

        print(parsed_ratings_list)


    data_to_que = []
    db_updated_x_times = 0
    sim_id = 1

    np.random.seed(seed=25) # set seed so that the random values are replicated
    rho1 = np.sqrt(asset_value_correlation)
    rho2 = np.sqrt(1 - asset_value_correlation)

    for _ in range(0, ECONOMIC_SCENARIOS):
        Y = np.random.standard_normal()
        for _ in range(0, PORTFOLIO_SIZE):
            Z = np.random.standard_normal()
            X = Y * rho1 + Z * rho2

            for rate in parsed_ratings_list:
                values = parsed_ratings_list.get(rate)
                if X < values['ppf']:
                    parsed_ratings_list[rate]['default_count'] +=1

        dict_to_que = {"S_ID": sim_id, "Y": Y}

        for rate in parsed_ratings_list:
            values = parsed_ratings_list.get(rate)

            if values["default_count"] == 0:
                l_ratio = 0
            else:
                l_ratio = values["default_count"] / PORTFOLIO_SIZE

            dict_to_que[rate+"_L_RATIO"] = l_ratio
            parsed_ratings_list[rate]["default_count"] = 0
        data_to_que.append(dict_to_que)

        sim_id += 1

        if sim_id >= BATCH_SIZE * (db_updated_x_times + 1):
            queue.put(data_to_que)
            data_to_que = []
            db_updated_x_times += 1

    if len(data_to_que) > 0:
        queue.put(data_to_que)
    queue.put(None)

def main():
    os.makedirs(DB_FOLDER, exist_ok=True)
    for year in YEARS_OF_INTEREST:
        print(f"Processing year {year}")
        # Create db file for given year
        db_name = DB_FOLDER + "year_" +str(year) + ".db"
        create_database(create_sql_command(mode="create"), db_name=db_name)

        # Multiprocessing for generating and uploading data simultaneously
        queue = mp.Queue(maxsize=MAX_QUEUE_SIZE)
        data_producer = mp.Process(target=simulate_losses_monte_carlo,
                                   args=(queue, year, ASSET_CORRELATION))
        data_consumer_ = mp.Process(target=data_consumer,
                                    args=(queue, db_name, create_sql_command(mode="insert")))

        data_producer.start()
        data_consumer_.start()

        data_producer.join()
        data_consumer_.join()

    print(f"Database files uploaded to {os.path.realpath(DB_FOLDER)}")


if __name__ == "__main__":
    main()
    DB_FOLDER = 'database_files_higher_rho/'
    ASSET_CORRELATION = 0.24
    main()
    DB_FOLDER = 'database_files_lower_rho/'
    ASSET_CORRELATION = 0.12
    main()