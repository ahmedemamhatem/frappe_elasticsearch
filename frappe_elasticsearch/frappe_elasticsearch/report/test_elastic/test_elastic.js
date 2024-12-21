// Copyright (c) 2024, Ahmed Emam and contributors
// For license information, please see license.txt

frappe.query_reports["test elastic"] = {
	"filters": [
        {
            fieldname: "from_date",
            label: __("From Date"),
            fieldtype: "Date"
        },
        {
            fieldname: "to_date",
            label: __("To Date"),
            fieldtype: "Date"
        }
    ]
};
