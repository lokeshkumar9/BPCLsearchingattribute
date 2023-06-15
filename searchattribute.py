from flask import Flask, render_template, request
import pyodbc
import pandas as pd

app = Flask(__name__)

# Define the connection details
server = 'DESKTOP-CPG55GV\\MSSQLSERVER01'
database = 'master'
username = 'lokesh'
password = '1234'

# Establish the connection to the SQL Server
conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};"
conn = pyodbc.connect(conn_str)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    csv_file = request.files['csv_file']
    csv_file_path = 'uploads/sampledata.csv'  # Path to save the uploaded file

    # Save the uploaded CSV file
    csv_file.save(csv_file_path)

    # Read the CSV file using pandas
    df = pd.read_csv(csv_file_path)

    # Extract the attribute names from the first row of the CSV file
    attribute_names = df.columns.tolist()

    # Function to retrieve table names and corresponding column names and data types based on attribute names input
    def get_table_with_attributes(attribute_names):
        tables = []
        cursor = conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        table_rows = cursor.fetchall()
        for row in table_rows:
            table_name = row.TABLE_NAME
            cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
            column_rows = cursor.fetchall()
            column_data = [(column_row.COLUMN_NAME.lower(), column_row.DATA_TYPE) for column_row in column_rows]
            matching_attributes = []
            for attribute_name in attribute_names:
                for column_name, data_type in column_data:
                    if attribute_name.lower() == column_name:
                        matching_attributes.append((column_name, data_type))
                        break
            if matching_attributes:
                tables.append((table_name, matching_attributes))
        return tables

    # Call the 'get_table_with_attributes' function with attribute_names
    tables = get_table_with_attributes(attribute_names)

    # Prepare the results to pass to the template
    results = []
    if tables:
        for attribute_name in attribute_names:
            attribute_tables = []
            for table in tables:
                table_name, attributes = table
                for attribute in attributes:
                    column_name, data_type = attribute
                    if attribute_name.lower() == column_name:
                        attribute_tables.append((table_name, data_type))
                        break
            results.append((attribute_name, attribute_tables))
    else:
        results.append(("No tables found", []))

    return render_template('results.html', results=results)

if __name__ == '__main__':
    app.run(debug=True)
