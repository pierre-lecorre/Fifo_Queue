import pandas as pd


# Function to load data
def load_data(file_name):
    try:
        return pd.read_csv(file_name)
    except FileNotFoundError:
        print(f"Error: {file_name} not found.")
        exit()


# Load data
issues_df = load_data('stock_issues.csv')
receipts_df = load_data('stock_receipts.csv')

# Convert and clean data
issues_df['Quantity'] = pd.to_numeric(issues_df['Quantity'], errors='coerce')
receipts_df['Quantity'] = pd.to_numeric(receipts_df['Quantity'], errors='coerce')

issues_df['Date'] = pd.to_datetime(issues_df['Date'],dayfirst=False)
receipts_df['Date'] = pd.to_datetime(receipts_df['Date'],dayfirst=False)

# Sort data
issues_df.sort_values(by='Date', inplace=True)
receipts_df.sort_values(by='Date', inplace=True)

# Prepare to track quantities from receipts
receipts_df['Remaining Quantity'] = receipts_df['Quantity']

# Result container
results = []

# Process each issue
for issue_index, issue in issues_df.iterrows():
    issue_code = issue['Document Code']
    product = issue['Product']
    issue_date = issue['Date']
    quantity_needed = issue['Quantity']

    if quantity_needed <= 0:  # Skip incorrect entries or corrections
        continue

    valid_receipts = receipts_df[(receipts_df['Product'] == product) &
                                 (receipts_df['Remaining Quantity'] > 0)]

    for receipt_index, receipt in valid_receipts.iterrows():
        if quantity_needed <= 0:
            break

        available_quantity = receipt['Remaining Quantity']
        used_quantity = min(quantity_needed, available_quantity)

        # Record usage
        results.append({
            'Issue Document Code': issue_code,
            'Product': product,
            'Received Date': receipt['Date'],
            'Issued Date': issue_date,
            'Quantity Issued': used_quantity,
            'Receipt Document Code': receipt['Document Code']
        })

        # Update the remaining quantities
        receipts_df.at[receipt_index, 'Remaining Quantity'] -= used_quantity
        quantity_needed -= used_quantity

    # Check if the issue was fully fulfilled
    if quantity_needed > 0:
        print(f"Warning: Issue {issue_code}, {product} was not fully fulfilled. {quantity_needed} units remain unmatched.")

# Create a DataFrame from results
result_df = pd.DataFrame(results)
result_df.to_csv('issue_receipt_links.csv', index=False)

# Display some of the results
print(result_df.head())
print(len(result_df))