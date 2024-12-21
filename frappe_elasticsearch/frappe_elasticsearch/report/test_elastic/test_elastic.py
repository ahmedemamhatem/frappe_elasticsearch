import frappe
from frappe_elasticsearch.utils import search_documents

def execute(filters=None):
    """Fetch data from Elasticsearch for GL Entries."""
    
    from_date = filters.get('from_date') if filters else None
    to_date = filters.get('to_date') if filters else None

    # Build the filter conditions
    filter_conditions = []
    if from_date:
        filter_conditions.append({"range": {"posting_date": {"gte": from_date}}})
    if to_date:
        filter_conditions.append({"range": {"posting_date": {"lte": to_date}}})

    # Define the query
    query = {
        "size": 10000,
        "query": {
            "bool": {
                "filter": filter_conditions  # Use the dynamically built filter
            }
        }
    }
    
    # Fetch initial results
    results = search_documents("gl_entries", query)
    scroll_id = results.get("_scroll_id")
    all_hits = results["hits"]["hits"]
    
    # Handle scrolling
    while True:
        hits = search_documents("gl_entries", None, scroll_id)["hits"]["hits"]
        if not hits:
            break
        all_hits.extend(hits)
    
    # Define columns
    columns = [
        {"fieldname": "voucher_no", "label": "Voucher Number", "fieldtype": "Data", "width": 150},
        {"fieldname": "posting_date", "label": "Posting Date", "fieldtype": "Date", "width": 100},
        {"fieldname": "account", "label": "Account", "fieldtype": "Data", "width": 150},
        {"fieldname": "account_currency", "label": "Currency", "fieldtype": "Data", "width": 100},
        {"fieldname": "against", "label": "Against", "fieldtype": "Data", "width": 150},
        {"fieldname": "voucher_type", "label": "Voucher Type", "fieldtype": "Data", "width": 150},
        {"fieldname": "transaction_currency", "label": "Transaction Currency", "fieldtype": "Data", "width": 150},
        {"fieldname": "debit", "label": "Debit", "fieldtype": "Currency", "width": 120},
        {"fieldname": "credit", "label": "Credit", "fieldtype": "Currency", "width": 120},
        {"fieldname": "cost_center", "label": "Cost Center", "fieldtype": "Data", "width": 150},
        {"fieldname": "company", "label": "Company", "fieldtype": "Data", "width": 150},
        {"fieldname": "remarks", "label": "Remarks", "fieldtype": "Text", "width": 250},
        {"fieldname": "branch", "label": "Branch", "fieldtype": "Data", "width": 150},
        {"fieldname": "department", "label": "Department", "fieldtype": "Data", "width": 150},
    ]

    # Extract data
    data = [
        {
            "voucher_no": hit["_source"].get("voucher_no", ""),
            "posting_date": hit["_source"].get("posting_date", ""),
            "account": hit["_source"].get("account", ""),
            "account_currency": hit["_source"].get("account_currency", ""),
            "against": hit["_source"].get("against", ""),
            "voucher_type": hit["_source"].get("voucher_type", ""),
            "transaction_currency": hit["_source"].get("transaction_currency", ""),
            "debit": hit["_source"].get("debit", 0),
            "credit": hit["_source"].get("credit", 0),
            "cost_center": hit["_source"].get("cost_center", ""),
            "company": hit["_source"].get("company", ""),
            "remarks": hit["_source"].get("remarks", ""),
            "branch": hit["_source"].get("branch", ""),
            "department": hit["_source"].get("department", ""),
        }
        for hit in all_hits
    ]
    
    return columns, data, len(all_hits)


