import sqlite3
import csv
import json
import numpy as np
import pandas as pd

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

def table_details(database_name, table_name, csv_file_path):
    # Establish connection
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # Select all data from the specified table
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Get column names for the CSV header
    column_names = [description[0] for description in cursor.description]

    # Open CSV file and write data
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)

        # Write the header row
        csv_writer.writerow(column_names)

        # Write the data rows
        csv_writer.writerows(rows)

    # Close the database connection
    conn.close()

    print(f"Data from table '{table_name}' in '{database_name}' successfully exported to '{csv_file_path}'.")

# --- Usage Example (assuming 'mydatabase.db' and 'employees' table exist) ---
# (You'd still need the create_sample_db() function from the first response
# if you haven't created the database yet.)
# create_sample_db() # Call this if you need to set up the sample DB

database = 'testing.db'
table1 = 'clients'
table2 = 'payments'
output_clients_csv = 'client_details.csv'
output_payments_csv = 'payments.csv'

table_details(database, table1, output_clients_csv)
table_details(database, table2, output_payments_csv)

db_file = 'testing.db'

with sqlite3.connect(database) as conn:
    print(f"Successfully connected to {database}")
        # --- Option 1: Read an entire table ---
    print(f"\n--- Copying data from '{table1}' table using pd.read_sql_table() ---")
    # df_clients = pd.read_sql_table(table1, conn)
    df_clients = pd.read_sql_query(f"SELECT * FROM {table1}", conn)
    #print(df_clients)
    #print(df_clients.info())

    df_payments = pd.read_sql_query(f"SELECT * FROM {table2} where client_id in (SELECT client_id FROM {table1})", conn)
    #print(df_payments)


summary = {}

    # Convert 'payment_date' to datetime objects for proper calculations
df_payments['transaction_date'] = pd.to_datetime(df_payments['transaction_date'], unit='s')

    # Total clients (from the clients DataFrame, as df_payments might not have all clients)
summary["total_clients"] = df_clients['client_id'].nunique()

    # Total payments (number of rows in df_payments)
summary["total_payments"] = len(df_payments)

# Oldest and newest payment dates
oldest_payment_dt = df_payments['transaction_date'].min()
newest_payment_dt = df_payments['transaction_date'].max()
summary["oldest_payment"] = oldest_payment_dt.strftime("%Y-%m-%dT%H:%M:%S")
summary["newest_payment"] = newest_payment_dt.strftime("%Y-%m-%dT%H:%M:%S")

    # Sum, average, min, max, and quartiles of payments
sum_all_payments = df_payments['payment_amt'].sum()
average_payment = df_payments['payment_amt'].mean()
payment_description = df_payments['payment_amt'].describe()
#print(payment_description)


summary["sum_all_payments"] = f"${sum_all_payments:,.2f}"
summary["average_payment"] = f"${average_payment:,.2f}"
summary["payment_min"] = f"${payment_description['min']:,.2f}"
summary["payment_quartile_1"] = f"${payment_description['25%']:,.2f}"
summary["payment_median"] = f"${payment_description['50%']:,.2f}"
summary["payment_quartile_3"] = f"${payment_description['75%']:,.2f}"
summary["payment_max"] = f"${payment_description['max']:,.2f}"


# Total amount paid in June and July
june_july_payments = df_payments[
    (df_payments['transaction_date'].dt.month == 6) |
    (df_payments['transaction_date'].dt.month == 7)
]
total_amt_paid_in_june_and_july = june_july_payments['payment_amt'].sum()
summary["total_amt_paid_in_june_and_july"] = f"${total_amt_paid_in_june_and_july:,.2f}"

    # Total number of payments under 1 dollar
total_num_payments_under_1_dollar = df_payments[df_payments['payment_amt'] < 1].shape[0]
summary["total_num_payments_under_1_dollar"] = total_num_payments_under_1_dollar

# Calculate total private companies
private_companies = df_clients[df_clients['entity_type'] == 'Australian Private Company']
summary["total_private_companies"] = private_companies['client_id'].nunique()


df_merged = pd.merge(df_payments, df_clients[['client_id', 'entity_type']], on='client_id', how='left')
sole_trader_payments_2017 = df_merged[
    (df_merged['entity_type'] == 'Individual/Sole Trader') &
    (df_merged['transaction_date'].dt.year == 2017)
]
    
average_sole_trader_payment_in_2017 = sole_trader_payments_2017['payment_amt'].mean()
summary["average_sole_trader_payment_in_2017"] = f"${average_sole_trader_payment_in_2017:,.2f}"

# print("summary: ",summary)

records = []
df_merged = pd.merge(df_clients, df_payments, on='client_id', how='inner')

    # Group by client to aggregate total payments and amounts
grouped_clients = df_merged.groupby('client_id')

for client_id, client_group in grouped_clients:
        # Get client-specific details (first row of the group is sufficient)
    client_info = client_group.iloc[0]
        
        # Initialize payments list for the current client
    payments_list = []
    for _, payment_row in client_group.iterrows():
        payments_list.append({
                "transaction_id": payment_row['transaction_id'], # Mapping payment_id to transaction_id
                "contract_id": payment_row['contract_id'],
                "transaction_date": payment_row['transaction_date'].strftime("%Y-%m-%dT%H:%M:%S"),
                "payment_amt": f"${payment_row['payment_amt']:,.2f}", # Mapping amount to payment_amt
                "payment_code": payment_row['payment_code']
            })

        # Calculate total payments and total amount paid for the client
    total_payments_count = len(client_group)
    total_amt_paid_sum = client_group['payment_amt'].sum()

    records.append({
            "client_id": client_id,
            "entity_type": client_info['entity_type'],
            "entity_year_established": client_info['entity_year_established'],
            "total_payments": total_payments_count,
            "total_amt_paid": f"${total_amt_paid_sum:,.2f}",
            "payments": payments_list
        })
    
    # Sort records by client_id for consistent output
records.sort(key=lambda x: x['client_id'])

#print("records: ",records)
output = {}
output["summary"] = summary
output['records'] = records
output_pretty = json.dumps(output, indent=4, cls=NumpyEncoder)

output_filename = "client_payment_records.json"

try:
    with open(output_filename, 'w') as f:
        json.dump(output, f, indent=4, cls=NumpyEncoder)
    print(f"Successfully wrote pretty-printed JSON to '{output_filename}'")
except IOError as e:
    print(f"Error writing to file '{output_filename}': {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")




  

