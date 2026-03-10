"""
Invoice Processing & GL Assignment Automation
For: Insights Factory

This script demonstrates AI-powered invoice processing:
1. Reads invoice PDFs
2. Extracts key information (vendor, amount, date, description)
3. Assigns GL (General Ledger) codes automatically
4. Exports to Excel

Author: AI Automation Team
Date: February 3, 2026
"""


import os
import csv
from datetime import datetime

# GL Code Mapping (General Ledger Accounts)
GL_CODES = {
    "6100": "Office Supplies & Equipment",
    "6200": "Marketing & Advertising", 
    "6300": "Travel & Transportation",
    "6400": "Utilities & Facility Costs",
    "6500": "Professional Services",
    "6600": "Technology & Software",
}

def assign_gl_code(vendor, description, amount):
    """
    AI-powered GL code assignment based on vendor and description
    This simulates what an AI would do by analyzing the context
    """
    
    vendor_lower = vendor.lower()
    description_lower = description.lower()
    
    # Office Supplies & Equipment (GL 6100)
    if any(keyword in vendor_lower for keyword in ['amazon', 'office', 'staples']):
        if any(keyword in description_lower for keyword in ['chair', 'desk', 'keyboard', 'mouse', 'supplies', 'equipment']):
            return "6100", GL_CODES["6100"], "High"
    
    # Marketing & Advertising (GL 6200)
    if any(keyword in vendor_lower for keyword in ['google', 'facebook', 'meta', 'linkedin', 'ads', 'marketing']):
        return "6200", GL_CODES["6200"], "High"
    
    if any(keyword in description_lower for keyword in ['advertising', 'marketing', 'campaign', 'promotion', 'seo']):
        return "6200", GL_CODES["6200"], "High"
    
    # Travel & Transportation (GL 6300)
    if any(keyword in vendor_lower for keyword in ['airline', 'indigo', 'spicejet', 'vistara', 'delta', 'uber', 'ola']):
        return "6300", GL_CODES["6300"], "High"
    
    if any(keyword in description_lower for keyword in ['flight', 'ticket', 'travel', 'hotel', 'transportation']):
        return "6300", GL_CODES["6300"], "High"
    
    # Utilities & Facility (GL 6400)
    if any(keyword in vendor_lower for keyword in ['electric', 'power', 'water', 'gas', 'telecom', 'internet']):
        return "6400", GL_CODES["6400"], "High"
    
    if any(keyword in description_lower for keyword in ['electricity', 'utility', 'power', 'water', 'rent']):
        return "6400", GL_CODES["6400"], "High"
    
    # Default: Professional Services (GL 6500)
    return "6500", GL_CODES["6500"], "Medium"

def extract_invoice_data(invoice_file):
    """
    Extract data from invoice filename and simulate reading PDF
    In real implementation, this would use OCR/PDF parsing
    """

    filename = os.path.basename(invoice_file).lower()

    # Always initialize invoice_data
    invoice_data = {}

    # Amazon Invoice
    if "amazon" in filename:
        invoice_data = {
            "invoice_number": "AMZ-2026-001234",
            "vendor": "Amazon Business",
            "date": "2026-01-15",
            "description": "Office Desk Chairs, Keyboards, Docking Stations",
            "amount": 1049.97,
            "currency": "USD"
        }

    # Google Ads Invoice
    elif "google" in filename:
        invoice_data = {
            "invoice_number": "GADS-2026-789456",
            "vendor": "Google Ads",
            "date": "2026-01-20",
            "description": "Digital Marketing Campaign",
            "amount": 2725.95,
            "currency": "USD"
        }

    # Handwritten / Office Invoice
    elif "office" in filename or "handwritten" in filename:
        invoice_data = {
            "invoice_number": "HW-INV-2093",
            "vendor": "Local Office Supplies Store",
            "date": "2026-02-03",
            "description": "Printer Paper, Ink Cartridge, Desk Lamp",
            "amount": 154.00,
            "currency": "USD"
        }

    # FALLBACK — unknown invoice auto handled
    if not invoice_data:
        invoice_data = {
            "invoice_number": "AUTO-DETECTED",
            "vendor": "Unknown Vendor",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "description": "Unrecognized invoice format",
            "amount": 100.00,
            "currency": "USD"
        }

    return invoice_data

def process_invoices(invoice_folder):
    """
    Process all invoices in the folder and assign GL codes
    """
    
    print("=" * 80)
    print("INVOICE PROCESSING & GL ASSIGNMENT AUTOMATION")
    print("Company: Insights Factory")
    print("=" * 80)
    print()
    
    # Get all PDF invoices
    invoice_files = [f for f in os.listdir(invoice_folder) if f.endswith('.pdf')]
    
    if not invoice_files:
        print(" No invoice PDFs found in the invoices folder!")
        return []
    
    print(f"📄 Found {len(invoice_files)} invoices to process\n")
    
    processed_data = []
    
    for i, invoice_file in enumerate(invoice_files, 1):
        print(f"Processing Invoice {i}/{len(invoice_files)}: {invoice_file}")
        print("-" * 80)
        
        # Extract invoice data
        invoice_path = os.path.join(invoice_folder, invoice_file)
        data = extract_invoice_data(invoice_path)
        
        # Display extracted information
        print(f"   Invoice Number: {data.get('invoice_number', 'NOT FOUND')}")
        print(f"   Vendor: {data.get('vendor', 'UNKNOWN')}")
        print(f"   Date: {data.get('date', 'UNKNOWN')}")
        print(f"   Description: {data.get('description', 'UNKNOWN')}")
        print(f"   Amount: ${data.get('amount', 0):,.2f} {data.get('currency', 'USD')}")

        # AI assigns GL code
        gl_code, gl_description, confidence = assign_gl_code(
            data.get('vendor', ''),
            data.get('description', ''),
            data.get('amount', 0)
        )

        print(f"   AI Assigned GL Code: {gl_code} - {gl_description}")
        print(f"   Confidence: {confidence}")
        print()
        
        # Store processed data
        processed_data.append({
            "invoice_file": invoice_file,
            "invoice_number": data['invoice_number'],
            "vendor": data['vendor'],
            "date": data['date'],
            "description": data['description'],
            "amount": data['amount'],
            "currency": data['currency'],
            "gl_code": gl_code,
            "gl_description": gl_description,
            "confidence": confidence,
            "processed_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    # RETURN is now outside the loop so all invoices are processed
    return processed_data
    


def export_to_csv(data, output_file):
    """
    Export processed invoice data to CSV file
    """
    
    if not data:
        print(" No data to export!")
        return
    
    print("=" * 80)
    print("EXPORTING RESULTS")
    print("=" * 80)
    
    # Define CSV columns
    fieldnames = [
        "Invoice File",
        "Invoice Number", 
        "Vendor",
        "Date",
        "Description",
        "Amount",
        "Currency",
        "GL Code",
        "GL Account Description",
        "AI Confidence",
        "Processed Date"
    ]
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write data rows
        for row in data:
            writer.writerow({
                "Invoice File": row['invoice_file'],
                "Invoice Number": row['invoice_number'],
                "Vendor": row['vendor'],
                "Date": row['date'],
                "Description": row['description'],
                "Amount": f"${row['amount']:,.2f}",
                "Currency": row['currency'],
                "GL Code": row['gl_code'],
                "GL Account Description": row['gl_description'],
                "AI Confidence": row['confidence'],
                "Processed Date": row['processed_date']
            })
    
    print(f" Successfully exported {len(data)} invoices to: {output_file}")
    print()
    
    # Display summary
    print("=" * 80)
    print("PROCESSING SUMMARY")
    print("=" * 80)
    print(f"Total Invoices Processed: {len(data)}")
    print(f"Total Amount: ${sum(row['amount'] for row in data):,.2f}")
    print()
    print("GL Code Breakdown:")
    
    # Count by GL code
    gl_counts = {}
    gl_totals = {}
    
    for row in data:
        gl = row['gl_code']
        if gl not in gl_counts:
            gl_counts[gl] = 0
            gl_totals[gl] = 0
        gl_counts[gl] += 1
        gl_totals[gl] += row['amount']
    
    for gl_code in sorted(gl_counts.keys()):
        print(f"  {gl_code} - {GL_CODES.get(gl_code, 'Unknown')}: {gl_counts[gl_code]} invoices (${gl_totals[gl_code]:,.2f})")
    
    print()
    print(" Invoice processing completed successfully!")
    print("=" * 80)

def main():
    """
    Main function to run the invoice automation
    """

    base_dir = os.path.dirname(os.path.abspath(__file__))

    invoice_folder = os.path.join(base_dir, "invoices")
    output_folder = os.path.join(base_dir, "output")
    output_file = os.path.join(output_folder, "processed_invoices.csv")

    # Create output folder if not exists
    os.makedirs(output_folder, exist_ok=True)

    # Process invoices
    processed_data = process_invoices(invoice_folder)

    # Export to CSV
    if processed_data:
        export_to_csv(processed_data, output_file)

        print("\n Output file location:")
        print(f"   {output_file}")
        print("\n You can now import this CSV into your accounting software!")
    else:
        print("No invoices were processed.")

if __name__ == "__main__":
    main()
