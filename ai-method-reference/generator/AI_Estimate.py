import psycopg2
import numpy as np
import time
import tkinter as tk
from tkinter import ttk, filedialog
from dotenv import load_dotenv
import os

# Load environment variables from .env if present
load_dotenv()

def get_db_info():
    return {
        'dbname': os.environ.get('DB_NAME', 'imdb'),
        'user': os.environ.get('DB_USER', 'postgres'),
        'password': os.environ.get('DB_PASSWORD', ''),
        'host': os.environ.get('DB_HOST', 'localhost'),
        'port': os.environ.get('DB_PORT', '5432')
    }

def getStatRows(connection, cursor):
    """
    Gets all the rows of pg_statistic that belong to the public namespace

    Parameters:
    psycopg2.extensions.connection: connection
    psycopg2.extensions.cursor: cursor

    Returns:
    rows of pg_statistic

    """
    # Create the new table pg_statistics_noisy
    copy_public_namespace_stat_query = '''
    select * from pg_statistic s where s.starelid in
    (select c.oid as starelid from pg_class c join pg_namespace n on c.relnamespace=n.oid)
    '''

    # Execute the query to create the new table
    cursor.execute(copy_public_namespace_stat_query)

    # Commit the transaction
    connection.commit()
    print("Rels from namespace == public fetched successfully.\n\n")

    return cursor.fetchall()


def insert_vals_into_rtc(noisy_vals, rtc, col_idx):
    # copy everything, except index 3, which comes from noisy_vals
    changed_rtc = []
    row_len = len(rtc[0])
    for row_idx in range(len(rtc)):
        new_row = tuple()
        for field_idx in range(row_len):
            if field_idx == col_idx:
                new_row = new_row + (noisy_vals[row_idx],)
            else:
                new_row = new_row + (rtc[row_idx][field_idx],)
        changed_rtc.append(new_row)

    print("Sanity check: ")
    print("numrows {} and numcols {} in rtc.".format(len(rtc), len(rtc[0])))
    print("Example orig_val: {}.".format(rtc[3][3]))
    print("numrows {} and numcols {} in changed_rtc.".format(len(changed_rtc), len(changed_rtc[0])))
    print("Example orig_val: {}.".format(changed_rtc[3][3]))
    print("\n\n")

    return changed_rtc


def insert_single_val_into_rtc(values, rtc, oid, col_idx):
    """
    Inserts values sequentially into rows with the specified `oid` in the `rtc` data.

    Parameters:
    - values (list): List of values to insert sequentially.
    - rtc (list of tuples): The table data.
    - oid (int): The object ID to match for row selection.
    - col_idx (int): The column index to update.

    Returns:
    - list of tuples: The updated `rtc` data.
    """
    changed_rtc = []
    value_idx = 0  # Index to keep track of the current position in values

    for row in rtc:
        # Check if this row matches the given oid
        if row[0] == oid and value_idx < len(values):
            # Replace the specified column with the next value in the list
            new_row = tuple(
                values[value_idx] if idx == col_idx else row[idx] for idx in range(len(row))
            )
            print(f"Updated row with oid {oid} at column {col_idx} to value: {values[value_idx]}")
            value_idx += 1  # Move to the next value in the list
        else:
            new_row = row
        changed_rtc.append(new_row)

    # Sanity check output
    print("Sanity check:")
    print(f"Target OID: {oid}")
    print(f"New values in changed_rtc: {[row for row in changed_rtc if row[0] == oid]}")
    print("\n\n")

    return changed_rtc


def insert_cr_into_pg_statistic(cr, connection, cursor, col_idx):
    print("Sanity check:\n")
    probe_pg_statistic(connection, cursor)
    print("\n")
    if col_idx == 3:
        for row_idx in range(len(cr)):
            q_insert_noisy_val = '''
            update pg_statistic set stanullfrac={} where starelid={} and staattnum={} and stainherit = {};
            '''.format(cr[row_idx][3], cr[row_idx][0], cr[row_idx][1], cr[row_idx][2])
            cursor.execute(q_insert_noisy_val)

            # Execute the query to create the new table
            cursor.execute(q_insert_noisy_val)

            # Commit the transaction
            connection.commit()
    elif col_idx == 5:
        for row_idx in range(len(cr)):
            q_insert_noisy_val = '''
            update pg_statistic set stadistinct={} where starelid={} and staattnum={} and stainherit = {};
            '''.format(cr[row_idx][5], cr[row_idx][0], cr[row_idx][1], cr[row_idx][2])
            cursor.execute(q_insert_noisy_val)

            # Execute the query to create the new table
            cursor.execute(q_insert_noisy_val)

            # Commit the transaction
            connection.commit()
    elif col_idx == 21:
        for row_idx in range(len(cr)):
            q_insert_noisy_val = '''
            update pg_statistic set stanumbers1={} where starelid={} and staattnum={} and stainherit = {};
            '''.format(cr[row_idx][21], cr[row_idx][0], cr[row_idx][1], cr[row_idx][2])
            cursor.execute(q_insert_noisy_val)

            # Execute the query to create the new table
            cursor.execute(q_insert_noisy_val)

            # Commit the transaction
            connection.commit()
    probe_pg_statistic(connection, cursor)
    print("\n\n")


def change_pg_statistics(connection, cursor, epsilon=0.1):
    # 1.
    # Get rows of pg_statistic that we want to change
    # i.e rows that belong to the namespace 'public'
    # these are the user-created table's stat rows
    # rtc == rows to change
    rtc = getStatRows(connection, cursor)

    modified_vals1 = get_orig_vals(rtc, 3)  # col_idx = 3 is null_frac
    modified_vals2 = get_orig_vals(rtc, 5)  # col_idx = 5 is n_distinct
    modified_vals3 = get_orig_vals(rtc, 21)  # col_idx = 21 is
    print("***************************")
    print(modified_vals2)
    # 3.
    # insert the noisy value into pg_statistic
    # cr = changed rows
    print("going into my 15 replace function")
    cr = insert_single_val_into_rtc([{0.3, 0.2, 0.1}, {0.3, 0.2, 0.1}, {0.3, 0.2, 0.1}, {0.3, 0.2, 0.1}, {0.3, 0.2, 0.1}], rtc, 34739, 21)
    # cr = insert_single_val_into_rtc([1000000, 500000, 50, -1, 200000], rtc, 33539, 5)
    # cr = insert_single_val_into_rtc([{0.3, 0.2, 0.1}, {0.25, 0.2, 0.15}, {0.4, 0.3, 0.2}, "{null}", "{null}"], rtc, 33539, 21)
    insert_cr_into_pg_statistic(cr, connection, cursor, 21)

    # 4.
    # run queries and save plans and execution times
    # run_queries(cursor, 1, epsilon)

    # 5. undo change (DISABLED AS I AM TESTING INSERTING CUSTOM DATA)
    # cursor.execute("analyze")


def get_orig_vals(rtc, col_idx):
    orig_vals = []
    r_idx = 0
    for r in rtc:
        orig_vals.append(rtc[r_idx][col_idx])
        r_idx += 1
    return orig_vals


def get_query(fname):
    query_files_dir = "/Users/saraalam/Desktop/PrivOptCode/job/"
    f = open(query_files_dir + fname + ".sql", "r")
    q = ""
    for _ in f.readlines():
        q += _.strip()
    f.close()
    return q


def save_query_plan(cursor, q, fname, noisy, eps):
    cursor.execute(f"EXPLAIN {q}")
    execution_plan = cursor.fetchall()

    if (noisy == 0):
        f2 = open("exec_plans/plan_" + fname + ".txt", "w")
    else:
        f2 = open("exec_plans/plan_" + fname + "_noisy_" + str(eps) + ".txt", "w")

    for _ in execution_plan:
        f2.write(str(_))
        f2.write("\n")

    f2.close()


def get_execution_time(cursor, q):
    start = time.time()
    for i in range(10):
        cursor.execute(f"EXPLAIN {q}")
        execution_plan = cursor.fetchall()
        cursor.execute(q)
        output = cursor.fetchall()
    end = time.time()
    exec_time = (end - start) / 10
    return exec_time


def run_queries(cursor, noisy=0, eps=0):
    # query_files_dir = "/Users/saraalam/Desktop/PrivOptCode/job/"
    query_files = ["1a", "2a", "3a", "4a", "5a"]

    if (noisy == 0):
        f3 = open("exec_times.txt", "w")
    else:
        f3 = open("exec_times_noisy_" + str(eps) + ".txt", "w")

    for fname in query_files:
        q = get_query(fname)
        save_query_plan(cursor, q, fname, noisy, eps)
        exec_time = get_execution_time(cursor, q)
        f3.write(fname + ", " + str(exec_time) + " seconds\n")

    f3.close()


def probe_pg_statistic(connection, cursor):
    q_get_pg_stat_row = '''select * from pg_statistic limit 2'''

    cursor.execute(q_get_pg_stat_row)
    output = cursor.fetchall()
    idx_lst = output[1]
    print(idx_lst[21])
    for idx2 in idx_lst:
        print(idx2)


def on_submit():
    db_info = get_db_info()
    col_idx = int(col_idx_entry.get())
    oid = int(oid_entry.get())
    values = values_text.get("1.0", tk.END).strip().split('\n')
    
    try:
        connection = psycopg2.connect(**db_info)
        cursor = connection.cursor()
        
        # Get the rows to modify
        rtc = getStatRows(connection, cursor)
        
        # Insert the new values
        cr = insert_single_val_into_rtc(values, rtc, oid, col_idx)
        insert_cr_into_pg_statistic(cr, connection, cursor, col_idx)
        
        result_label.config(text="Statistics updated successfully")
        
    except Exception as e:
        result_label.config(text=f"Error: {str(e)}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

# Create the main window
root = tk.Tk()
root.title("PG Statistics Modifier")
root.geometry("800x800")

# Create and place widgets
row_idx = 0

# Database connection info
db_frame = ttk.LabelFrame(root, text="Database Connection Info", padding=10)
db_frame.grid(row=row_idx, column=0, columnspan=2, padx=10, pady=5, sticky="ew")
row_idx += 1

ttk.Label(db_frame, text="Note: Configure database connection in .env file").grid(row=0, column=0, columnspan=2, pady=5)

# OID input
ttk.Label(root, text="Table OID:").grid(row=row_idx, column=0, padx=10, pady=5, sticky="w")
oid_entry = ttk.Entry(root, width=20)
oid_entry.grid(row=row_idx, column=1, padx=10, pady=5)
row_idx += 1

# Column index input
ttk.Label(root, text="Column Index (3=null_frac, 5=n_distinct, 21=stanumbers1):").grid(row=row_idx, column=0, padx=10, pady=5, sticky="w")
col_idx_entry = ttk.Entry(root, width=20)
col_idx_entry.insert(0, "3")
col_idx_entry.grid(row=row_idx, column=1, padx=10, pady=5)
row_idx += 1

# Values input
ttk.Label(root, text="New Values (one per attname):").grid(row=row_idx, column=0, padx=10, pady=5, sticky="nw")
values_text = tk.Text(root, width=40, height=10)
values_text.grid(row=row_idx, column=1, padx=10, pady=5)
row_idx += 1

# Submit button
submit_button = ttk.Button(root, text="Update Statistics", command=on_submit)
submit_button.grid(row=row_idx, column=0, columnspan=2, pady=20)
row_idx += 1

# Result label
result_label = ttk.Label(root, text="")
result_label.grid(row=row_idx, column=0, columnspan=2, pady=10)

# Add explanatory note
note_text = """
Column Index Guide:
- 3: null_frac (fraction of null values)
- 5: n_distinct (number of distinct values)
- 21: stanumbers1 (statistical numbers)

For stanumbers1 (col_idx=21), input values as arrays like:
{0.3, 0.2, 0.1}
{0.25, 0.2, 0.15}

For null_frac (col_idx=3) or n_distinct (col_idx=5), input single values like:
0.5
0.75
1000000
"""
note_label = ttk.Label(root, text=note_text, justify="left")
note_label.grid(row=row_idx, column=0, columnspan=2, padx=10, pady=10)

root.mainloop()

# Notes:
# # Create the new table pg_statistics_noisy
# create_table_query = '''
# update pg_statistic set stanullfrac=1
# where starelid=1247 and staattnum=1 and stainherit = 'f';
# '''

# # Execute the query to create the new table
# cursor.execute(create_table_query)

# # Commit the transaction
# connection.commit()
# print("Table pg_statistics_noisy created successfully.")

'''
    functions to write:
    1. getStatRows(relname)
       output: row of pg_statistics for this key

    2. call getStatRows for list of pairs of relnames and attrnames
        and make sql query stringing the relnames with ORs

    3. transform statistics
        transformed = transform(copy, cols_to_change)

    4. update(transformed)
    '''