from openai import OpenAI
import tkinter as tk
from tkinter import ttk, filedialog
import csv
import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2

# Load environment variables from .env if present
load_dotenv()

ground_truth_file_path = None
output_accuracy_file = "Outputs/accuracy_results_for_graph.csv"

def get_db_info():
    return {
        'dbname': os.environ.get('DB_NAME', 'imdb'),
        'user': os.environ.get('DB_USER', 'postgres'),
        'password': os.environ.get('DB_PASSWORD', ''),
        'host': os.environ.get('DB_HOST', 'localhost'),
        'port': os.environ.get('DB_PORT', '5432')
    }
#moved api key to .env file, if needed reach out to me

def get_openai_api_key():
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set in environment or .env file.")
    return api_key

#get model from UI

def get_selected_model():
    return model_entry.get().strip() or "gpt-3.5-turbo"

def get_table_oid():
    return table_oid_entry.get().strip()

#Get prompt list from UI, note that these will not use other fields on the UI

def get_prompt_list():
    prompts = prompt_list_text.get("1.0", tk.END).strip()
    if prompts:
        return [p for p in prompts.split("\n") if p.strip()]
    return []

def on_submit():
    db_name = db_name_entry.get().strip()
    database_info = database_info_text.get("1.0", tk.END).strip()
    col_names = col_names_entry.get()
    size = size_entry.get()
    sample = sample_entry.get()
    sample_rows = sample_row_entry.get("1.0", tk.END).strip()
    model = get_selected_model()
    prompt_list = get_prompt_list()

    root.quit()
    root.destroy()

    print("Running main code, form completed")
    run_pg_stats(database_info, col_names, size, db_name, sample, sample_rows, model, prompt_list)

def run_pg_stats(database_info, col_names, size, db_name, sample, sample_rows, model, prompt_list):
    if not os.path.exists("Outputs/" + db_name):
        os.makedirs("Outputs/" + db_name)
    filename = "Outputs/" + db_name + "/" + db_name
    num_iterations = 10
    prediction_files = []
    column_accuracies_overall = {col: 0 for col in range(1, 9)}

    # Use prompt_list if provided, else use default prompt
    prompts = prompt_list if prompt_list else [build_pg_stats_prompt(database_info, col_names, size, sample_rows)]

    for prompt in prompts:
        print(f"Running prompt: {prompt[:100]}...")
        for i in range(num_iterations):
            print(f"Iteration {i+1} out of {num_iterations}")
            
            # Generate response with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                response_message = get_pg_stats(prompt, model)
                numberedfilename = filename + f"_{prompt_list.index(prompt) if prompt_list else 0}_{i}.csv"
                write_response_to_csv(response_message, numberedfilename)
                
                # Post-process and check if regeneration is needed
                if not post_process_csv(numberedfilename):
                    break
                else:
                    print(f"Attempt {attempt + 1}: Regenerating due to post-processing checks")
                    if attempt == max_retries - 1:
                        print(f"Warning: Max retries reached for {numberedfilename}")
            
            prediction_files.append(numberedfilename)
            trim_extra_columns(numberedfilename, ground_truth_file_path)
            ground_truth = pd.read_csv(ground_truth_file_path)
            column_accuracies = compare_csvs(ground_truth, pd.read_csv(numberedfilename), columns=list(range(1, 9)))
            sheet_accuracy = sum(column_accuracies.values()) / len(column_accuracies)
            append_accuracy_to_file(numberedfilename, sheet_accuracy, sample)
            
            for col, accuracy in column_accuracies.items():
                print(f"Column {col} Accuracy: {accuracy:.2f}%")
                if accuracy > column_accuracies_overall[col]:
                    column_accuracies_overall[col] = accuracy
            print(f"Sheet Accuracy for iteration {i+1}: {sheet_accuracy:.2f}%")

    # best_guesses = find_best_guesses(ground_truth, prediction_files, columns=list(range(1, 9)))
    # best_guess_filename = f"Outputs/{db_name}/best_guess.csv"
    # best_guesses.to_csv(best_guess_filename, index=False)
    # print(f"Best guess CSV saved to {best_guess_filename}")
    # with open(output_accuracy_file, 'a', newline='') as file:
    #     writer = csv.writer(file)
    #     writer.writerow(["Best Guess CSV", "Accuracy", "Sample Rows"])
    #     for col, accuracy in column_accuracies_overall.items():
    #         writer.writerow([f"Best Guess - Column {col}", accuracy, sample])

def trim_extra_columns(filepath, ground_truth_file_path):
    ground_truth = pd.read_csv(ground_truth_file_path)
    with open(filepath, 'r') as file:
        lines = file.readlines()

    # Adjust the number of columns to match the ground truth
    num_cols = len(ground_truth.columns)
    trimmed_lines = [','.join(line.split(',')[:num_cols]) for line in lines]

    with open(filepath, 'w') as file:
        file.writelines(trimmed_lines)

def import_csv_to_pg_statistic(csv_path, table_oid):
    db_info = get_db_info()
    supported_cols = {
        'null_frac': 3,
        'n_distinct': 5,
        'correlation': 7,
    }
    try:
        connection = psycopg2.connect(**db_info)
        cursor = connection.cursor()
        df = pd.read_csv(csv_path)
        for idx, row in df.iterrows():
            attname = row['attname'] if 'attname' in row else row[0]
            for col, col_idx in supported_cols.items():
                if col in df.columns:
                    value = row[col]
                    # Find staattnum for this attname
                    cursor.execute(
                        "SELECT attnum FROM pg_attribute WHERE attrelid = %s AND attname = %s",
                        (table_oid, attname)
                    )
                    result = cursor.fetchone()
                    if result:
                        staattnum = result[0]
                        # Update the supported column
                        cursor.execute(
                            f"UPDATE pg_statistic SET {col} = %s WHERE starelid = %s AND staattnum = %s",
                            (value, table_oid, staattnum)
                        )
        connection.commit()
        print(f"Imported {csv_path} into pg_statistic for OID {table_oid}")
    except Exception as e:
        print(f"Error importing CSV: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
def post_process_csv(filepath):
    """
    Post-process the generated CSV file with various checks and fixes.
    Returns True if the file needs to be regenerated, False otherwise.
    """
    try:
        with open(filepath, 'r') as file:
            content = file.read()
        
        # Check 1: Remove trailing commas
        if content.endswith(','):
            content = content.rstrip(',')
            with open(filepath, 'w') as file:
                file.write(content)
            print(f"Removed trailing comma from {filepath}")
        
        # Read the CSV for further checks
        df = pd.read_csv(filepath)
        
        # Check 2: If more than 70% NULL/null, regenerate from scratch
        null_count = df.isnull().sum().sum()  # pandas NaN values
        null_count += (df == 'NULL').sum().sum()  # string "NULL" values
        null_count += (df == 'null').sum().sum()  # string "null" values
        total_cells = df.shape[0] * df.shape[1]
        null_percentage = (null_count / total_cells) * 100
        if null_percentage > 70:
            print(f"File {filepath} has {null_percentage:.1f}% NULL values - needs full regeneration")
            return True  # Full regeneration
        
        # Check 3: If less than 8 columns, try to fix with AI
        if len(df.columns) < 8:
            print(f"File {filepath} has only {len(df.columns)} columns - sending to AI for repair")
            correction_prompt = f"The following CSV is missing columns. Please regenerate a valid pg_stats CSV with at least 8 columns, fixing only the missing columns and preserving the rest as much as possible.\nCSV:\n{content}"
            fixed_csv = get_pg_stats(correction_prompt, model)
            write_response_to_csv(fixed_csv, filepath)
            if attempt < max_attempts:
                return post_process_csv(filepath, prompt, model, attempt+1, max_attempts)
            else:
                print("Max post-processing attempts reached.")
                return True
            return False
        
        # Check 4: Clamp correlation values
        if 'correlation' in df.columns:
            df['correlation'] = df['correlation'].clip(-1, 1)
            print(f"Clamped correlation values in {filepath}")
        
        # Check 5: Clamp n_distinct values
        if 'n_distinct' in df.columns:
            df['n_distinct'] = df['n_distinct'].clip(-1, 1)
            print(f"Clamped n_distinct values in {filepath}")
        
        # Save the processed file
        df.to_csv(filepath, index=False)
        return False
        
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return True

def append_accuracy_to_file(csv_name, accuracy, sample):
    if not os.path.exists(output_accuracy_file):
        with open(output_accuracy_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["CSV Name", "Accuracy", "Sample Rows"])

    with open(output_accuracy_file, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([csv_name, accuracy, sample])

# Compare two csvs calculate accuracies with column inputs using absolute percent error
def compare_csvs(ground_truth, predicted, columns):
    # Initialize dictionaries to track accuracy, total absolute percentage error, and count of valid entries per column
    column_accuracies = {col: 0 for col in columns}
    total_absolute_percentage_error = {col: 0 for col in columns}
    total_cells = {col: 0 for col in columns}

    for col in columns:
        try:
            #access col-1 column (basically going to 0 index from 1 index)
            ground_truth_col = ground_truth.iloc[:, col - 1]
            predicted_col = predicted.iloc[:, col - 1]
        except IndexError:
            #if out of range, skip
            continue

        #compare each col value
        for gt_val, pred_val in zip(ground_truth_col, predicted_col):
            if pd.isna(gt_val) or pd.isna(pred_val):
                #if either is null, continue
                continue
            else:
                try:

                    gt_val = float(gt_val)
                    pred_val = float(pred_val)
                    total_cells[col] += 1
                    # if ground truth isnt 0 (prevent divide by 0) then find the error
                    if gt_val != 0:
                        total_absolute_percentage_error[col] += abs((gt_val - pred_val) / abs(gt_val))
                    elif gt_val == 0 and pred_val == 0:
                        total_absolute_percentage_error[col] += 0
                    else:
                        #if pred is 0 and gt is not, then say 100% wrong (this needs to be improved upon)
                        total_absolute_percentage_error[col] += 1
                except ValueError:
                    continue
    # Compute accuracy for each column based on the average percentage error
    for col in columns:
        if total_cells[col] == 0:
            column_accuracies[col] = 0
        else:
            percent_wrong = (total_absolute_percentage_error[col] / total_cells[col]) * 100
            # Accuracy is 100% minus the error percentage, bounded below by 0 to prevent negatives
            column_accuracies[col] = max(100 - percent_wrong, 0)

    return column_accuracies

# This func is not currently being used: not really in scope of project
# This function takes a ground truth dataset and a list of prediction files,
# and selects the best prediction (with highest accuracy) for each column.
def find_best_guesses(ground_truth, prediction_files, columns):
    best_guesses = pd.DataFrame(columns=ground_truth.columns)
    best_accuracies = {col: 0 for col in columns}

    # Iterate through each prediction file
    for pred_file in prediction_files:
        # Read the prediction CSV file into a DataFrame
        predicted = pd.read_csv(pred_file)
        # Compare the prediction with the ground truth to get accuracy scores
        column_accuracies = compare_csvs(ground_truth, predicted, columns)

        # Update best guesses for each column if this prediction is better
        for col in columns:
            if column_accuracies[col] > best_accuracies[col]:
                best_accuracies[col] = column_accuracies[col]
                # Store the best column values from the current prediction
                best_guesses[ground_truth.columns[col - 1]] = predicted.iloc[:, col - 1]

    # Ensure that the 'attname' column is retained from the ground truth
    best_guesses['attname'] = ground_truth['attname']
    return best_guesses

def load_csv():
    global ground_truth_file_path
    file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    if file_path:
        ground_truth_file_path = file_path
        ground_truth = pd.read_csv(file_path)
        column_types = detect_column_types(ground_truth)
        print("Column types:")
        for col, col_type in column_types.items():
            print(f"{col}: {col_type}")

def detect_column_types(data):
    column_types = {}
    for column in data.columns:
        if pd.api.types.is_numeric_dtype(data[column]):
            column_types[column] = 'number'
        elif pd.api.types.is_list_like(data[column]):
            column_types[column] = 'list'
        else:
            column_types[column] = 'word'
    return column_types

def load_prompts_from_file():
    #load prompts from file, only take in text files
    file_path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
    if file_path:
        try:
            with open(file_path, 'r') as file:
                prompts = file.read()
                prompt_list_text.delete("1.0", tk.END)  # Clear existing content in the box
                prompt_list_text.insert("1.0", prompts) # insert the prompts into the box
        except Exception as e:
            print(f"Error loading prompts from file: {e}")

# Build prompt from form fields

def build_pg_stats_prompt(database_info, col_names, size, sample_rows):
    return (f"I have a postgres sql database that I want you to estimate the pg_stats for. PLEASE MAKE SURE THAT THE CSVS ARE SEMICOLON SEPARATED AND NOT COMMA SEPARATED"
            f"The column names and descriptions for pg_stats are: attname name (references pg_attribute.attname): Name of column described by this row, null_frac float4: Fraction of column entries that are null avg_width int4 Average width in bytes of columns entries n_distinct float4 If greater than zero, the estimated number of distinct values in the column. If less than zero, the negative of the number of distinct values divided by the number of rows. (The negated form is used when ANALYZE believes that the number of distinct values is likely to increase as the table grows; the positive form is used when the column seems to have a fixed number of possible values.) For example, -1 indicates a unique column in which the number of distinct values is the same as the number of rows. most_common_vals anyarray A list of the most common values in the column. (Null if no values seem to be more common than any others.) most_common_freqs float4[] A list of the frequencies of the most common values, i.e., number of occurrences of each divided by total number of rows. (Null when most_common_vals is.) histogram_bounds anyarray A list of values that divide the columns values into groups of approximately equal population. The values in most_common_vals, if present, are omitted from this histogram calculation. (This column is null if the column data type does not have a < operator or if the most_common_vals list accounts for the entire population.) correlation float4 Statistical correlation between physical row ordering and logical ordering of the column values. This ranges from -1 to +1. When the value is near -1 or +1, an index scan on the column will be estimated to be cheaper than when it is near zero, due to reduction of random access to the disk. (This column is null if the column data type does not have a < operator.) The column names in the database are {col_names}. The total size of the database is {size}. Please do not use elipses in your histogram predictions and make guesses whenever possible based on patterns in this style of database, do not guess randomly. This dataset {database_info} Record your answer in csv format. Here are some sample rows {sample_rows}, DO NOT COPY THIS AND ALWAYS GENERATE PG_STATS.")

# Write response to CSV

def write_response_to_csv(response_message, filename):
    with open(filename, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        for line in response_message.split('\n'):
            csvwriter.writerow(line.split(';'))

# Refactored get_pg_stats to take prompt and model

def get_pg_stats(prompt, model):
    client = OpenAI(api_key=get_openai_api_key())
    messages = [
        {"role": "system", "content": 'You make predictions about pg_stats tables for postgres databases...'},
        {"role": "user", "content": prompt},
    ]
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.3,
        seed=30
    )
    return response.choices[0].message.content

# ALL OF THE CODE BELOW CREATES THE UI
root = tk.Tk()
root.title("Data Collection Form")
root.geometry("900x900")

row_idx = 0

tk.Label(root, text="Database Name:").grid(row=row_idx, column=0, padx=10, pady=5, sticky="w")
db_name_entry = ttk.Entry(root, width=50)
db_name_entry.grid(row=row_idx, column=1, padx=10, pady=5)
row_idx += 1

# Add Table OID input
tk.Label(root, text="Table OID:").grid(row=row_idx, column=0, padx=10, pady=5, sticky="w")
table_oid_entry = ttk.Entry(root, width=50)
table_oid_entry.grid(row=row_idx, column=1, padx=10, pady=5)
row_idx += 1

# Continue with Database Information input
tk.Label(root, text="Database Information:").grid(row=row_idx, column=0, padx=10, pady=5, sticky="nw")
database_info_text = tk.Text(root, width=60, height=10)
database_info_text.grid(row=row_idx, column=1, padx=10, pady=5)
row_idx += 1

tk.Label(root, text="Column Names (comma-separated):").grid(row=row_idx, column=0, padx=10, pady=5, sticky="w")
col_names_entry = ttk.Entry(root, width=50)
col_names_entry.grid(row=row_idx, column=1, padx=10, pady=5)
row_idx += 1

tk.Label(root, text="Size:").grid(row=row_idx, column=0, padx=10, pady=5, sticky="w")
size_entry = ttk.Entry(root, width=50)
size_entry.grid(row=row_idx, column=1, padx=10, pady=5)
row_idx += 1

tk.Label(root, text="Sample Rows").grid(row=row_idx, column=0, padx=10, pady=5, sticky="nw")
sample_row_entry = tk.Text(root, width=60, height=10)
sample_row_entry.grid(row=row_idx, column=1, padx=10, pady=5)
row_idx += 1

tk.Label(root, text="Num Rows Inputted:").grid(row=row_idx, column=0, padx=10, pady=5, sticky="w")
sample_entry = ttk.Entry(root, width=50)
sample_entry.grid(row=row_idx, column=1, padx=10, pady=5)
row_idx += 1

# Prompt list input with file loading option
prompt_list_frame = tk.Frame(root)
prompt_list_frame.grid(row=row_idx, column=0, columnspan=2, padx=10, pady=5, sticky="nw")
row_idx += 1

prompt_list_label = tk.Label(prompt_list_frame, text="Prompt List (one per line):")
prompt_list_label.grid(row=0, column=0, padx=10, pady=5, sticky="nw")

load_prompts_button = ttk.Button(prompt_list_frame, text="Load from File", command=load_prompts_from_file)
load_prompts_button.grid(row=0, column=1, padx=10, pady=5)

prompt_list_text = tk.Text(root, width=60, height=5)
prompt_list_text.grid(row=row_idx, column=0, columnspan=2, padx=10, pady=5)
row_idx += 1

# Add note about prompt list behavior
prompt_list_note = tk.Label(root, text="Note: When used, only database name will be used from other fields", fg="gray")
prompt_list_note.grid(row=row_idx, column=0, columnspan=2, padx=10, pady=(0, 5), sticky="w")
row_idx += 1

# Model selection input
model_label = tk.Label(root, text="Model (e.g., gpt-3.5-turbo, gpt-4):")
model_label.grid(row=row_idx, column=0, padx=10, pady=5, sticky="w")
model_entry = ttk.Entry(root, width=50)
model_entry.insert(0, "gpt-3.5-turbo")
model_entry.grid(row=row_idx, column=1, padx=10, pady=5)
row_idx += 1

load_csv_button = ttk.Button(root, text="Load Ground Truth CSV", command=load_csv)
load_csv_button.grid(row=row_idx, column=1, pady=10)
row_idx += 1

submit_button = ttk.Button(root, text="Submit", command=on_submit)
submit_button.grid(row=row_idx, column=1, pady=10)
row_idx += 1

def on_import_csv():
    table_oid = get_table_oid()
    # You can prompt for a file or use the last generated CSV
    csv_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    if csv_path and table_oid:
        import_csv_to_pg_statistic(csv_path, int(table_oid))

import_button = ttk.Button(root, text="Import CSV to pg_statistic", command=on_import_csv)
import_button.grid(row=row_idx, column=1, pady=10)
row_idx += 1

root.mainloop()

