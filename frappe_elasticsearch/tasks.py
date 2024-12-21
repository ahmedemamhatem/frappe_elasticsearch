import frappe
from frappe_elasticsearch.utils import index_document


@frappe.whitelist(allow_guest=True)
def index_all_gl_entries():
    """Fetch all GL Entries and index them into Elasticsearch in batches of 100."""
    # Fetch all GL Entries with custom_elasticsearch == 0
    gl_entries = frappe.get_all(
        'GL Entry', 
        fields=["name", "voucher_no", "posting_date", "fiscal_year", "account", 
                "account_currency", "against", "voucher_type", "transaction_currency", 
                "debit", "credit", "cost_center", "company", "remarks", 
                "branch", "department", "is_opening", "is_advance", "is_cancelled"],
        filters={"custom_elasticsearch": 0}
    )

    # Ensure Elasticsearch configuration is loaded and accessible
    es_config = frappe.conf.get('elasticsearch')
    if not es_config:
        frappe.throw(_("Elasticsearch is not configured properly."))

    # Initialize batch size
    batch_size = 1000
    total_entries = len(gl_entries)
    processed_entries = 0

    # Iterate over the GL Entries in batches of 100
    for i in range(0, total_entries, batch_size):
        batch = gl_entries[i:i + batch_size]
        
        # Index each GL Entry in the batch
        for entry in batch:
            gl_entry_data = {
                "voucher_no": entry.voucher_no,
                "posting_date": str(entry.posting_date),
                "fiscal_year": entry.fiscal_year,
                "account": entry.account,
                "account_currency": entry.account_currency,
                "against": entry.against,
                "voucher_type": entry.voucher_type,
                "transaction_currency": entry.transaction_currency,
                "debit": entry.debit,
                "credit": entry.credit,
                "cost_center": entry.cost_center,
                "company": entry.company,
                "remarks": entry.remarks,
                "branch": entry.branch,
                "department": entry.department,
                "is_opening": entry.is_opening,
                "is_advance": entry.is_advance,
                "is_cancelled": entry.is_cancelled,
            }

            # Index document in Elasticsearch
            try:
                index_document("gl_entries", entry.name, gl_entry_data)
                # Set custom_elasticsearch to 1 for successfully indexed records
                frappe.db.set_value("GL Entry", entry.name, "custom_elasticsearch", 1)
                frappe.db.commit()  # Commit changes to ensure updates are saved
            except Exception as e:
                frappe.log_error(f"Failed to index GL Entry {entry.name} in Elasticsearch", str(e))
                continue  # Continue indexing the next record even if one fails

        # Update processed entries and print progress after each batch
        processed_entries += len(batch)
        print(f"Processed {processed_entries}/{total_entries} GL Entries.")  # Print progress for each batch
    
    
    return {"message": "GL Entries indexed successfully."}

    
def index_gl_entry(doc, method):
    """Index GL Entry in Elasticsearch after insertion."""
    # Ensure Elasticsearch configuration is loaded and accessible
    es_config = frappe.conf.get('elasticsearch')

    if not es_config:
        frappe.throw("Elasticsearch is not configured properly.")
    
    # Prepare the data to index (mapping GL Entry fields)
    gl_entry_data = {
        "voucher_no": doc.voucher_no,
        "posting_date": str(doc.posting_date),
        "fiscal_year": doc.fiscal_year,
        "account": doc.account,
        "account_currency": doc.account_currency,
        "against": doc.against,
        "voucher_type": doc.voucher_type,
        "transaction_currency": doc.transaction_currency,
        "debit": doc.debit,
        "credit": doc.credit,
        "cost_center": doc.cost_center,
        "company": doc.company,
        "remarks": doc.remarks,
        "branch": doc.branch,
        "department": doc.department,
        "is_opening": doc.is_opening,
        "is_advance": doc.is_advance,
        "is_cancelled": doc.is_cancelled,
    }

    # Index document in Elasticsearch
    try:
        index_document("gl_entries", doc.name, gl_entry_data)
        frappe.db.set_value("GL Entry", doc.name, "custom_elasticsearch", 1)
    except Exception as e:
        frappe.log_error(f"Failed to index GL Entry {doc.name} in Elasticsearch", str(e))
        raise


def index_sales_invoice(doc, method):
    """Index Sales Invoice in Elasticsearch after submission."""
    # Ensure Elasticsearch configuration is loaded and accessible
    es_config = frappe.conf.get('elasticsearch')

    if not es_config:
        frappe.throw("Elasticsearch is not configured properly.")
    
    # Prepare the data to index
    sales_invoice_data = {
        "invoice_number": doc.name,
        "customer": doc.customer,
        "posting_date": str(doc.posting_date),
        "grand_total": doc.grand_total,
        "items": [{"item_code": i.item_code, "qty": i.qty, "rate": i.rate} for i in doc.items],
    }

    # Index document in Elasticsearch
    try:
        index_document("sales_invoices", doc.name, sales_invoice_data)
    except Exception as e:
        frappe.log_error(f"Failed to index Sales Invoice {doc.name} in Elasticsearch", str(e))
        raise


