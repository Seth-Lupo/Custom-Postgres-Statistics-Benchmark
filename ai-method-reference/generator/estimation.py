from openai import OpenAI
import tkinter as tk
from tkinter import ttk, filedialog
import csv
import os
import pandas as pd
from PIL import Image, ImageTk


ground_truth_file_path = None
output_accuracy_file = "Outputs/accuracy_results_for_graph.csv"

def on_submit():
    # Get data from form and turn into usable variables
    db_name = db_name_entry.get().strip()
    database_info = database_info_text.get("1.0", tk.END).strip()
    col_names = col_names_entry.get()
    size = size_entry.get()
    sample = sample_entry.get()
    sample_rows = sample_row_entry.get("1.0", tk.END).strip()

    # Close the window after submission
    root.quit()
    root.destroy()

    print("Running main code, form completed")

    # After collecting from the form, run the rest of the code
    run_pg_stats(database_info, col_names, size, db_name, sample, sample_rows)
def run_pg_stats(database_info, col_names, size, db_name, sample, sample_rows):
    def get_pg_stats(database_info, col_names, size):
        # Prompt definition
        prompt = (f"I have a postgres sql database that I want you to estimate the pg_stats for. PLEASE MAKE SURE THAT THE CSVS ARE SEMICOLON SEPARATED AND NOT COMMA SEPARATED"
                  f"The column names and descriptions for pg_stats are: attname name (references pg_attribute.attname): Name of column described by this row, null_frac float4: Fraction of column entries that are null avg_width int4 Average width in bytes of columns entries n_distinct float4 If greater than zero, the estimated number of distinct values in the column. If less than zero, the negative of the number of distinct values divided by the number of rows. (The negated form is used when ANALYZE believes that the number of distinct values is likely to increase as the table grows; the positive form is used when the column seems to have a fixed number of possible values.) For example, -1 indicates a unique column in which the number of distinct values is the same as the number of rows. most_common_vals anyarray A list of the most common values in the column. (Null if no values seem to be more common than any others.) most_common_freqs float4[] A list of the frequencies of the most common values, i.e., number of occurrences of each divided by total number of rows. (Null when most_common_vals is.) histogram_bounds anyarray A list of values that divide the columns values into groups of approximately equal population. The values in most_common_vals, if present, are omitted from this histogram calculation. (This column is null if the column data type does not have a < operator or if the most_common_vals list accounts for the entire population.) correlation float4 Statistical correlation between physical row ordering and logical ordering of the column values. This ranges from -1 to +1. When the value is near -1 or +1, an index scan on the column will be estimated to be cheaper than when it is near zero, due to reduction of random access to the disk. (This column is null if the column data type does not have a < operator.) The column names in the database are {col_names}. The total size of the database is {size}. Please do not use elipses in your histogram predictions and make guesses whenever possible based on patterns in this style of database, do not guess randomly. This dataset {database_info} Record your answer in csv format. Here are some sample rows {sample_rows}, DO NOT COPY THIS AND ALWAYS GENERATE PG_STATS.")
        # API Key Setup with New OpenAI documentation
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        # Model to use
        GPT_MODEL = "gpt-3.5-turbo"
        messages = [
            {"role": "system",
             # Baseline prompt for the AI
             "content": 'You make predictions about pg_stats tables for postgres databases. You will always make a guess and never guess randomly. You will always output a semicolon, never comma, separated csv with no other information but the csv. Please do not guess NULL for the list columns unless very necessary, please always generate a pg_stats table and never the raw data.'},
            # Actual user prompt
            {"role": "user", "content": prompt},
        ]
        response = client.chat.completions.create(
            model=GPT_MODEL,
            messages=messages,
            # seed and temp settings
            # lower temp = less random
            temperature=0.3,
            seed=30
        )
        response_message = response.choices[0].message.content
        return response_message

    #Make directory in Outputs for the db_name, this should be changed later
    if not os.path.exists("Outputs/" + db_name):
        os.makedirs("Outputs/" + db_name)

    # Filename without the .csv
    filename = "Outputs/" + db_name + "/" + db_name

    # Number of times to loop for an average
    num_iterations = 10
    output_list = []
    for i in range(0, num_iterations):
        print("Iteration " + str(i) + " Out of " + str(num_iterations))
        # Function call
        response_message = get_pg_stats(database_info, col_names, size)
        output_list.append(response_message)
        # Name files for output
        numberedfilename = filename + str(i) + ".csv"
        # Write response_message to a CSV file
        with open(numberedfilename, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            for line in response_message.split('\n'):
                #splits with ; because of commas in lists
                csvwriter.writerow(line.split(';'))

        # Trim extra columns if any
        trim_extra_columns(numberedfilename, ground_truth_file_path)

    #Had to rework print formatting because Python things
    print(f"{num_iterations} CSV files have been created successfully.")

    # Load the ground truth CSV
    ground_truth = pd.read_csv(ground_truth_file_path)

    # Compare each predicted CSV with the ground truth
    accuracy_list = []
    for i in range(num_iterations):
        predicted_csv = pd.read_csv(filename + str(i) + ".csv")
        #1,2,3,7 are the currently used cols (null_frac, avg_width, n_distinct, correlation)
        accuracy = compare_csvs(ground_truth, predicted_csv, columns=[1, 2, 3, 7])
        accuracy_list.append(accuracy)
        append_accuracy_to_file(filename + str(i) + ".csv", accuracy, sample)
        # Magic print statement to only print to 2 decimal places
        print(f"Accuracy for iteration {i}: {accuracy:.2f}%")

    # Print average accuracy
    avg_accuracy = sum(accuracy_list) / num_iterations
    append_accuracy_to_file("Average", avg_accuracy, sample)
    print(f"Average accuracy: {avg_accuracy:.2f}%")


def trim_extra_columns(filepath, ground_truth_file_path):
    # Read the ground truth file to determine the expected number of columns
    ground_truth = pd.read_csv(ground_truth_file_path)

    # Read the entire file content to a list of lines
    with open(filepath, 'r') as file:
        lines = ""
        for line in file:
            lines += line
        print(lines)
        print("val is")
        print(lines[-2])
    # Check if the last line ends with a comma and remove it if necessary
    if lines[-2] == ',':
        print("trimming")
        lines = lines[:-2]

    # Write the updated content back to the file
    with open(filepath, 'w') as file:
        file.write(lines)
def append_accuracy_to_file(csv_name, accuracy, sample):
    if not os.path.exists(output_accuracy_file):
        with open(output_accuracy_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["CSV Name", "Accuracy", "Sample Rows"])

    with open(output_accuracy_file, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([csv_name, accuracy, sample])
def compare_csvs(ground_truth, predicted, columns):
    total_absolute_percentage_error = 0
    total_cells = 0

    for col in columns:
        try:
            # 0 index the columns but not sure if it actually helps much
            ground_truth_col = ground_truth.iloc[:, col - 1]
            predicted_col = predicted.iloc[:, col - 1]
        except IndexError:
            return 0

        for gt_val, pred_val in zip(ground_truth_col, predicted_col):
            if pd.isna(gt_val) or pd.isna(pred_val):
                continue
            else:
                try:
                    gt_val = float(gt_val)
                    pred_val = float(pred_val)
                    total_cells += 1
                    if gt_val != 0:
                        total_absolute_percentage_error += abs((gt_val - pred_val) / abs(gt_val))
                    elif gt_val == 0 and pred_val == 0:
                        total_absolute_percentage_error += 0
                    else:
                        total_absolute_percentage_error += 1
                except ValueError:
                    continue

    if total_cells == 0:
        return 0
    #Calculate the % incorrect and then subtract from 100 to get the accuracy
    percent_wrong = (total_absolute_percentage_error / total_cells) * 100
    accuracy = 100 - percent_wrong
    if (accuracy < 0):
        return 0
    return accuracy


def load_csv():
    # Save file path so it can be opened later (this function is called by the UI so before other parsing)
    global ground_truth_file_path
    # Ensure it is a csv
    file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    if file_path:
        ground_truth_file_path = file_path
        ground_truth = pd.read_csv(file_path)
        column_types = detect_column_types(ground_truth)
        print("Column types:")
        for col, col_type in column_types.items():
            print(f"{col}: {col_type}")



# Tries to detect which type of data is in each column
# Returns a 1d array of strings that maps columns and their types
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

root = tk.Tk()
root.title("Data Collection Form")
root.geometry("900x800")


# Load and resize TSP logo
logo_png = Image.open("/Users/harrison/Documents/tsp logo.png")
logo_png = logo_png.resize((400, 150), Image.LANCZOS)
logo = ImageTk.PhotoImage(logo_png)

# Place the logo
logo_label = tk.Label(root, image=logo)
logo_label.grid(row=0, column = 1, pady=10, sticky="n")


# DB Name Field
tk.Label(root, text="Database Name:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
db_name_entry = ttk.Entry(root, width=50)
db_name_entry.grid(row=1, column=1, padx=10, pady=5)

# DB INFO Field
tk.Label(root, text="Database Information:").grid(row=2, column=0, padx=10, pady=5, sticky="nw")
database_info_text = tk.Text(root, width=60, height=10)
database_info_text.grid(row=2, column=1, padx=10, pady=5)

# Col_name Field
tk.Label(root, text="Column Names (comma-separated):").grid(row=3, column=0, padx=10, pady=5, sticky="w")
col_names_entry = ttk.Entry(root, width=50)
col_names_entry.grid(row=3, column=1, padx=10, pady=5)

# Size field
tk.Label(root, text="Size:").grid(row=4, column=0, padx=10, pady=5, sticky="w")
size_entry = ttk.Entry(root, width=50)
size_entry.grid(row=4, column=1, padx=10, pady=5)

tk.Label(root, text="Sample Rows").grid(row=5, column=0, padx=10, pady=5, sticky="nw")
sample_row_entry = tk.Text(root, width=60, height=10)
sample_row_entry.grid(row=5, column=1, padx=10, pady=5)

tk.Label(root, text="Num Rows Inputted:").grid(row=6, column=0, padx=10, pady=5, sticky="w")
sample_entry = ttk.Entry(root, width=50)
sample_entry.grid(row=6, column=1, padx=10, pady=5)

# (WIP) csv ground truth input
load_csv_button = ttk.Button(root, text="Load Ground Truth CSV", command=load_csv)
load_csv_button.grid(row=7, column=1, pady=10)

# Create and place the submit button
submit_button = ttk.Button(root, text="Submit", command=on_submit)
submit_button.grid(row=8, column=1, pady=10)



# Run the application
root.mainloop()