# Generate the data

import csv

# Define the sample data
sample_data = [
    {'id': 1, 'name': 'John Doe', 'updated_at': '2023-05-01 12:00:00'},
    {'id': 2, 'name': 'Jane Smith', 'updated_at': '2023-05-02 10:30:00'},
    {'id': 3, 'name': 'Alice Johnson', 'updated_at': '2023-05-03 15:45:00'}
]

# Define the path and filename for the CSV file
csv_file_path = 'external_table.csv'

# Define the CSV field names
field_names = ['id', 'name', 'updated_at']

# Write the data to the CSV file
with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=field_names)
    writer.writeheader()
    writer.writerows(sample_data)

print(f"CSV file '{csv_file_path}' generated successfully!")