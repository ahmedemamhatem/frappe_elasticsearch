import frappe

def execute(filters=None):
    """
    Generate the report for GL Entries using SQL and include all columns as data.
    """
    # Extract filters
    from_date = filters.get("from_date")
    to_date = filters.get("to_date")

    # Define the SQL query
    query = """
        SELECT 
            *
        FROM 
            `tabGL Entry`
        WHERE
            (%(from_date)s IS NULL OR posting_date >= %(from_date)s)
            AND (%(to_date)s IS NULL OR posting_date <= %(to_date)s)
        ORDER BY 
            posting_date ASC
    """

    # Execute the query with filters
    data = frappe.db.sql(query, {"from_date": from_date, "to_date": to_date}, as_dict=True)

    # Dynamically generate columns based on the keys in the result
    if data:
        first_row = data[0]  # Get the first row to determine columns
        columns = [
            {
                "fieldname": field,
                "label": field.replace("_", " ").title(),  # Convert field names to human-readable labels
                "fieldtype": "Data",  # Default field type as Data
                "width": 150,
            }
            for field in first_row.keys()
        ]
    else:
        columns = []

    # Return columns and data
    return columns, data
