"""
AllPets RDS MCP Server
Connects Claude (web) to the AllPets MySQL database on AWS RDS.
Transport: SSE (Server-Sent Events) for Claude web integration.
Deploy on Render — PORT and DB credentials come from environment variables.
"""

import os
from mcp.server.fastmcp import FastMCP
import mysql.connector
from decimal import Decimal
from datetime import datetime, date

# ── Database Configuration ────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "database-2-cohort-sandbox-db.cdokim6aw6op.ap-south-1.rds.amazonaws.com"),
    "port":     int(os.environ.get("DB_PORT", "3306")),
    "user":     os.environ.get("DB_USER", "cohort_student"),
    "password": os.environ.get("DB_PASSWORD", "AlumnxLearn@2026"),
    "database": os.environ.get("DB_NAME", "cohort_main"),
    "charset":  "utf8mb4",
}

PORT = int(os.environ.get("PORT", "8000"))

# ── Schema Documentation (fed to Claude as context) ───────────────────────────

SCHEMA = """
========================================================
  ALLPETS VETERINARY DATABASE — cohort_main (AWS RDS)
  Data Period: April 2025 – April 2026
  Location: India | Currency: Indian Rupees (₹)
  IMPORTANT: Always display all monetary amounts in ₹ (Indian Rupees), never in $ (dollars).
========================================================

TABLE 1: allpets_invoice_line_items  (26,194 rows)
─────────────────────────────────────────────────────
PURPOSE: Every invoice line item billed at the clinic.
         One invoice = multiple rows (one row per product/service sold).

⚠️  CRITICAL RULES — read before writing any query:

  1. INVOICE-LEVEL REVENUE (total billed per invoice, monthly revenue, client spend):
     Use MAX(invoice_amount) with GROUP BY invoice_id
     because invoice_amount is the same on every line item of the same invoice.

  2. CATEGORY / PRODUCT / DOCTOR LEVEL REVENUE (breakdown by category, item, provider):
     Use SUM(total) — the `total` column is the actual line item amount.
     Always add: WHERE sales_id != '' — rows where sales_id='' are invoice header rows with no line item.
     NEVER use invoice_amount for category-level breakdowns — it will give wrong inflated results.

  3. ALWAYS filter cancelled = 'FALSE' for any revenue or visit count query.

  4. DATE FILTERING: Use invoice_date >= 'YYYY-MM-01' AND invoice_date < 'YYYY-MM-01' (next month).

COLUMNS:
  id                      INT AUTO_INCREMENT PRIMARY KEY
  sales_id                VARCHAR — line item ID (empty string for invoice-only rows)
  invoice_id              VARCHAR — groups all line items of same invoice
  patient_id              VARCHAR
  invoice_no              VARCHAR — e.g. 'S4946' (Service), 'P12787' (Pharmacy)
  invoice_date            DATETIME — e.g. '2025-04-01 10:49:07'
  invoice_amount          DECIMAL(14,2) — total invoice value (same on every row of same invoice)
  invoice_type            VARCHAR — 'Service' or 'Pharmacy'
  cancelled               VARCHAR — 'FALSE' = active, 'TRUE' = cancelled/returned
  invoice_balance         DECIMAL(14,2)
  invoice_discount        DECIMAL(14,2)
  invoice_taxable_amount  DECIMAL(14,2)
  roundoff                DECIMAL(10,4)
  otc                     VARCHAR — over-the-counter flag
  gstin, client_gstn      VARCHAR — GST numbers
  invoice_pdf_url         TEXT
  biller_id, biller_name  VARCHAR
  invoice_cgst_rate, invoice_cgst_amount   DECIMAL
  invoice_sgst_rate, invoice_sgst_amount   DECIMAL
  invoice_igst_rate, invoice_igst_amount   DECIMAL

  CLIENT: client_id, client_unique_id, client_first_name, client_last_name,
          mobile_phone, email_id

  PATIENT: patient_name, patient_species (Canine/Feline/Avian/Rabbit etc.),
           patient_breed, patient_gender, patient_birth_date, patient_weight

  LINE ITEM:
    plan_item_id, plan_item_name       — e.g. 'x-ray x 3 views', 'Nexgard 25-50kgs'
    plan_category_id, plan_category_name   — e.g. 'Imaging', 'Grooming', 'Prescription 18%'
    plan_sub_category_id, plan_sub_category_name  — e.g. 'Abdominal Radiography', 'NSAID'
    brand_id, brand_name
    quantity        DECIMAL(14,4)
    fees            DECIMAL(14,2) — selling price per unit (before discount)
    item_discount   DECIMAL(14,2)
    item_tax_amount DECIMAL(14,2)
    total           DECIMAL(14,2) — final line item amount
    purchase_cost   DECIMAL(14,2) — cost price
    item_cgst_rate, item_cgst_amount  DECIMAL
    item_sgst_rate, item_sgst_amount  DECIMAL
    item_igst_rate, item_igst_amount  DECIMAL
    mfr, lot, expiry  VARCHAR — batch/expiry info for pharmacy

  PROVIDER: provider_id, provider_name, performed_clinic_id, performed_clinic_name,
            performed_date, visit_id, visit_name, created_by, req_id
  clinic_id, clinic_name, vetbuddy_instance_id

⚠️  KNOWN ITEM NAMES FOR EQUIPMENT QUERIES:
  X-Ray items   : 'X-ray', 'x-ray x 2 views', 'x-ray x 3 views', 'X-ray Print'
                  Use: WHERE plan_item_name IN ('X-ray','x-ray x 2 views','x-ray x 3 views','X-ray Print')
  Ultrasound    : 'Ultrasound', 'AFAST (Abdominal Focused Assessment with Sonography)', 'TFAST (Thoracic Focused Assessment with Sonography)'
                  Use: WHERE plan_item_name LIKE '%ltrasound%' OR plan_item_name LIKE '%FAST%'
  Laser machine : NOT in database — tell user this data is unavailable.

⚠️  FOR MEDICINE QUERIES:
  Use plan_category_name IN ('Prescription 18%','Prescription 12%','Prescription','Pharmacy') to filter medicines.
  Use SUM(quantity) for units sold, SUM(total) for revenue.

⚠️  FOR TIME-BASED SPLITS (9am–9pm vs 9pm–9am):
  Use HOUR(invoice_date) to extract hour from invoice_date.
  Day shift  : HOUR(invoice_date) >= 9 AND HOUR(invoice_date) < 21
  Night shift : HOUR(invoice_date) < 9 OR HOUR(invoice_date) >= 21

⚠️  FOR NEW vs RETURNING CUSTOMERS (daily/monthly):
  New customer    = client whose MIN(invoice_date) falls on that day (first ever invoice)
  Returning       = client who has invoices before that day
  Use allpets_clients.first_activity for first visit date.

USEFUL QUERY PATTERNS:

  -- Monthly revenue (non-cancelled invoices):
  SELECT DATE_FORMAT(invoice_date,'%Y-%m') AS month, ROUND(SUM(m),2) AS revenue
  FROM (SELECT invoice_id, invoice_date, MAX(invoice_amount) m
        FROM allpets_invoice_line_items WHERE cancelled='FALSE'
        GROUP BY invoice_id, invoice_date) t
  GROUP BY month ORDER BY month;

  -- Revenue by invoice type (Service vs Pharmacy):
  SELECT invoice_type, ROUND(SUM(m),2) AS revenue
  FROM (SELECT invoice_id, invoice_type, MAX(invoice_amount) m
        FROM allpets_invoice_line_items WHERE cancelled='FALSE'
        GROUP BY invoice_id, invoice_type) t
  GROUP BY invoice_type;

  -- Revenue by category (use line item total, NOT invoice_amount):
  SELECT plan_category_name, ROUND(SUM(total),2) AS revenue, COUNT(*) AS items_sold
  FROM allpets_invoice_line_items
  WHERE cancelled='FALSE' AND sales_id != ''
  GROUP BY plan_category_name ORDER BY revenue DESC;

  -- Top patients/species by visit count:
  SELECT patient_species, COUNT(DISTINCT invoice_id) AS visits
  FROM allpets_invoice_line_items WHERE cancelled='FALSE'
  GROUP BY patient_species ORDER BY visits DESC;

  -- Top clients by spend:
  SELECT client_first_name, client_last_name, mobile_phone,
         COUNT(DISTINCT invoice_id) AS invoices, ROUND(SUM(m),2) AS total_spent
  FROM (SELECT invoice_id, client_first_name, client_last_name, mobile_phone,
               MAX(invoice_amount) m
        FROM allpets_invoice_line_items WHERE cancelled='FALSE'
        GROUP BY invoice_id, client_first_name, client_last_name, mobile_phone) t
  GROUP BY client_first_name, client_last_name, mobile_phone
  ORDER BY total_spent DESC LIMIT 10;

  -- Top selling products:
  SELECT plan_item_name, SUM(quantity) AS qty_sold, ROUND(SUM(total),2) AS revenue
  FROM allpets_invoice_line_items
  WHERE cancelled='FALSE' AND sales_id != ''
  GROUP BY plan_item_name ORDER BY revenue DESC LIMIT 20;

  -- Doctor/provider performance:
  SELECT provider_name, COUNT(DISTINCT invoice_id) AS cases,
         ROUND(SUM(total),2) AS revenue_generated
  FROM allpets_invoice_line_items
  WHERE cancelled='FALSE' AND sales_id != '' AND provider_name != ''
  GROUP BY provider_name ORDER BY revenue_generated DESC;


TABLE 2: allpets_clients  (1,361 rows)
─────────────────────────────────────────────────────
PURPOSE: Unique client (pet owner) master records.

COLUMNS:
  client_id         VARCHAR PRIMARY KEY
  vetbuddy_instance_id VARCHAR
  clinic_id, clinic_name
  client_unique_id, crm_client_id
  first_name, last_name
  home_phone, mobile_phone, work_phone, email_id
  address1, address2, city, state, zip
  first_activity    DATETIME — date of first visit
  last_activity     DATETIME — do NOT use this for last visit queries, it includes cancelled invoices
  status            VARCHAR — 'Active'
  gstn, is_otc_client

⚠️  CRITICAL RULES for client visit queries:
  - Always JOIN allpets_clients with allpets_invoice_line_items ON client_id
  - Always filter WHERE cancelled = 'FALSE' to exclude cancelled/returned invoices
  - Always use MAX(invoice_date) from allpets_invoice_line_items as the true last visit date
  - Never use last_activity from allpets_clients — it is unreliable (includes cancelled visits)
  - Use INNER JOIN (not LEFT JOIN) when querying active visiting clients

USEFUL PATTERNS:
  -- New clients per month:
  SELECT DATE_FORMAT(first_activity,'%Y-%m') AS month, COUNT(*) AS new_clients
  FROM allpets_clients GROUP BY month ORDER BY month;

  -- Client retention (visited more than once):
  SELECT c.client_id, c.first_name, c.last_name,
         COUNT(DISTINCT i.invoice_id) AS total_visits
  FROM allpets_clients c
  JOIN allpets_invoice_line_items i ON c.client_id = i.client_id
  WHERE i.cancelled='FALSE'
  GROUP BY c.client_id, c.first_name, c.last_name
  HAVING total_visits > 1 ORDER BY total_visits DESC;


TABLE 3: allpets_appointments  (6,078 rows)
─────────────────────────────────────────────────────
PURPOSE: All appointment bookings.

COLUMNS:
  appointment_id            VARCHAR PRIMARY KEY
  vetbuddy_instance_id
  client_id, client_unique_id
  patient_id, patient_name
  appointment_type_id, appointment_type_name  — e.g. 'Consultation', 'Grooming', 'Surgery'
  reason_for_visit_id, reason_for_visit_name  — actual values in DB:
    Vaccination types: 'Routine Vaccination - Adult Annual', 'Routine Vaccination - Rabies', 'Routine Vaccination - Feline Annual'
    Diseases/Ailments: 'skin infection', 'Not Doing Well', 'Vomiting', 'Not Eating', 'Limping'
    Procedures: 'Sales', 'Review', 'Check up', 'General Exam', 'Follow up', 'dressing', 'injection', 'castration'
    Other: 'Bath', 'hair cut', 'Swimming'
  visit_id, visit_name
  clinic_id, clinic_name
  provider_id, provider_name
  appointment_start_time   DATETIME
  appointment_end_time     DATETIME
  appointment_status       VARCHAR — e.g. 'Completed', 'Cancelled', 'No Show'
  check_in_time            DATETIME
  check_out_time           DATETIME
  completed_time           DATETIME
  is_no_show               VARCHAR

USEFUL PATTERNS:
  -- Daily appointment count:
  SELECT DATE(appointment_start_time) AS day, COUNT(*) AS appointments
  FROM allpets_appointments
  GROUP BY day ORDER BY day;

  -- Appointments by type:
  SELECT appointment_type_name, COUNT(*) AS count
  FROM allpets_appointments
  GROUP BY appointment_type_name ORDER BY count DESC;

  -- No-show rate:
  SELECT COUNT(*) AS total,
         SUM(is_no_show IN ('true','TRUE','1','Yes')) AS no_shows,
         ROUND(SUM(is_no_show IN ('true','TRUE','1','Yes'))*100.0/COUNT(*),1) AS no_show_pct
  FROM allpets_appointments;


TABLE 4: allpets_payments  (10,957 rows)
─────────────────────────────────────────────────────
PURPOSE: Actual payment transactions received.
         Use this for cash collected, not invoice_amount for billed amounts.

COLUMNS:
  payment_id            VARCHAR PRIMARY KEY
  vetbuddy_instance_id
  clinic_id, clinic_name
  client_id, client_name, client_unique_id
  invoice_id, invoice_no, invoice_amount, invoice_type
  payment_amount        DECIMAL(14,2) — actual amount paid
  receipt_no            VARCHAR
  payment_date          DATETIME
  payment_type_id, payment_type_name  — e.g. 'Cash', 'Card', 'UPI', 'Online'
  creator               VARCHAR
  returned              VARCHAR

USEFUL PATTERNS:
  -- Monthly cash collected:
  SELECT DATE_FORMAT(payment_date,'%Y-%m') AS month,
         ROUND(SUM(payment_amount),2) AS collected
  FROM allpets_payments GROUP BY month ORDER BY month;

  -- Payment method breakdown:
  SELECT payment_type_name, COUNT(*) AS transactions,
         ROUND(SUM(payment_amount),2) AS total
  FROM allpets_payments GROUP BY payment_type_name ORDER BY total DESC;

  -- Outstanding balances (invoiced but not fully paid):
  SELECT i.invoice_id, MAX(i.invoice_amount) AS billed,
         COALESCE(SUM(p.payment_amount),0) AS paid,
         MAX(i.invoice_amount) - COALESCE(SUM(p.payment_amount),0) AS balance
  FROM allpets_invoice_line_items i
  LEFT JOIN allpets_payments p ON i.invoice_id = p.invoice_id
  WHERE i.cancelled='FALSE'
  GROUP BY i.invoice_id
  HAVING balance > 0 ORDER BY balance DESC LIMIT 20;


TABLE 5: allpets_stock  (9,111 rows)
─────────────────────────────────────────────────────
PURPOSE: Current inventory/stock snapshot.

COLUMNS:
  stock_consumed_id       VARCHAR PRIMARY KEY
  stock_id, stock_name
  is_group                VARCHAR
  clinic_id, clinic_name
  plan_item_id, plan_item_name
  plan_category_id, plan_category_name
  plan_sub_category_id, plan_sub_category_name
  onhand_qty              DECIMAL(14,4) — current stock level
  threshold_qty           DECIMAL(14,4) — minimum stock (reorder point)
  reorder_qty             DECIMAL(14,4) — quantity to reorder
  purchase_cost           DECIMAL(14,2) — cost price per unit
  sales_markup            DECIMAL(10,2) — markup percentage
  orderable               VARCHAR
  bin_id, bin_name, bin_location

USEFUL PATTERNS:
  -- Items below reorder threshold:
  SELECT stock_name, plan_category_name, onhand_qty, threshold_qty, reorder_qty
  FROM allpets_stock
  WHERE onhand_qty < threshold_qty AND threshold_qty > 0
  ORDER BY (threshold_qty - onhand_qty) DESC;

  -- Inventory value by category:
  SELECT plan_category_name,
         COUNT(*) AS items,
         ROUND(SUM(onhand_qty * purchase_cost),2) AS inventory_value
  FROM allpets_stock
  WHERE purchase_cost > 0
  GROUP BY plan_category_name ORDER BY inventory_value DESC;

TABLE 6: allpets_patient_diagnosis  (lab reports)
─────────────────────────────────────────────────────
PURPOSE: Patient lab/diagnostic reports (Pathology, outside labs etc.)
         One row per lab report. Links to invoice via sales_id.

COLUMNS:
  lab_report_id           VARCHAR PRIMARY KEY
  vetbuddy_instance_id    VARCHAR
  lab_report_no           VARCHAR
  clinic_id, clinic_name  VARCHAR
  visit_id, visit_name    VARCHAR
  patient_id, patient_name VARCHAR
  client_id, client_name, client_unique_id VARCHAR
  plan_item_id, plan_item_name    VARCHAR — e.g. 'Antech - CBC'
  plan_sub_category_id, plan_sub_category_name  VARCHAR — e.g. 'Antech'
  plan_category_id, plan_category_name  VARCHAR — e.g. 'Pathology / Lab - Outside'
  form_type               VARCHAR — e.g. 'Diagnostic'
  sales_id                VARCHAR — links to allpets_invoice_line_items.sales_id
  report_date             DATETIME
  result_date             DATETIME
  status                  VARCHAR — e.g. 'Stored', 'Pending'
  reported_by             VARCHAR
  provider_id, provider_name VARCHAR
  print_pdf_link          TEXT

USEFUL PATTERNS:
  -- Lab reports by category:
  SELECT plan_category_name, COUNT(*) AS reports
  FROM allpets_patient_diagnosis
  GROUP BY plan_category_name ORDER BY reports DESC;

  -- Pending lab reports:
  SELECT patient_name, client_name, plan_item_name, report_date
  FROM allpets_patient_diagnosis
  WHERE status = 'Pending' ORDER BY report_date DESC;

========================================================
"""

# ── MCP Server Setup ──────────────────────────────────────────────────────────

SYSTEM_PROMPT = """
You are an expert veterinary business analyst for AllPets clinic in India.
You have direct access to the AllPets MySQL database via the tools provided.

DATABASE KNOWLEDGE (memorise this — do NOT call get_database_schema again in the same conversation):
""" + SCHEMA + """

BEHAVIOUR RULES:
- Call get_database_schema ONLY at the start of a brand new conversation, never again after that.
- For every follow-up question in the same conversation, use the schema you already know.
- Always display monetary values in Indian Rupees (₹), never in dollars ($).
- Always use run_sql_query to fetch live data — never guess or make up numbers.
- Keep responses concise: show the data table first, then a short 2-3 line insight below it.
- If a query returns more than 50 rows, summarise the top results and mention the total count.
"""

mcp = FastMCP("AllPets RDS Connector", host="0.0.0.0", port=PORT, instructions=SYSTEM_PROMPT)

# ── Helper Functions ──────────────────────────────────────────────────────────

def serialize(val):
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (datetime, date)):
        return str(val)
    return val


def execute_sql(sql: str):
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql)
        if cursor.description:
            columns = [d[0] for d in cursor.description]
            rows = [[serialize(v) for v in row] for row in cursor.fetchall()]
            return columns, rows, None
        else:
            conn.commit()
            return None, cursor.rowcount, None
    except mysql.connector.Error as e:
        return None, None, str(e)
    finally:
        if conn:
            conn.close()


def format_as_table(columns, rows, max_rows=200):
    if not rows:
        return "Query returned 0 rows."

    display = rows[:max_rows]
    str_rows = [[str(v) if v is not None else "NULL" for v in row] for row in display]
    widths = [len(c) for c in columns]
    for row in str_rows:
        for i, v in enumerate(row):
            widths[i] = max(widths[i], len(v))

    sep    = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header = "|" + "|".join(f" {c.ljust(w)} " for c, w in zip(columns, widths)) + "|"
    lines  = [sep, header, sep]
    for row in str_rows:
        lines.append("|" + "|".join(f" {v.ljust(w)} " for v, w in zip(row, widths)) + "|")
    lines.append(sep)

    note = f"\nShowing {len(display)} of {len(rows)} rows." if len(rows) > max_rows else f"\nTotal: {len(rows)} rows."
    return "\n".join(lines) + note

# ── Tools ─────────────────────────────────────────────────────────────────────

@mcp.tool()
def get_database_schema() -> str:
    """
    Returns the complete schema of all 5 AllPets tables — column names, data types,
    relationships, and ready-to-use SQL query patterns.
    ALWAYS call this first before writing any SQL query to understand the data structure.
    """
    return SCHEMA


@mcp.tool()
def run_sql_query(sql: str) -> str:
    """
    Executes any SQL query on the AllPets RDS MySQL database (cohort_main)
    and returns formatted results.

    For SELECT queries: returns a formatted table with column headers and rows.
    For INSERT/UPDATE/DELETE: returns rows affected count.

    Args:
        sql: Any valid MySQL SQL statement.

    Important notes:
    - All 5 tables are in database cohort_main (already selected by default)
    - Table names: allpets_invoice_line_items, allpets_clients,
                   allpets_appointments, allpets_payments, allpets_stock
    - For revenue queries: use GROUP BY invoice_id + MAX(invoice_amount)
      because invoice_amount repeats on every line item row of the same invoice
    - cancelled='FALSE' for active invoices, 'TRUE' for cancelled/returned
    - invoice_date is DATETIME format: '2025-04-01 10:49:07'
    """
    columns, result, error = execute_sql(sql)

    if error:
        return f"SQL Error: {error}\n\nQuery:\n{sql}"

    if columns is None:
        return f"Query executed. Rows affected: {result}"

    return format_as_table(columns, result)


@mcp.tool()
def get_sample_data(table_name: str, limit: int = 5) -> str:
    """
    Returns sample rows from any AllPets table to understand its data format.

    Args:
        table_name: One of:
                    allpets_invoice_line_items
                    allpets_clients
                    allpets_appointments
                    allpets_payments
                    allpets_stock
        limit: Number of rows (default 5, max 20)
    """
    ALLOWED = {
        "allpets_invoice_line_items",
        "allpets_clients",
        "allpets_appointments",
        "allpets_payments",
        "allpets_stock",
        "allpets_patient_diagnosis",
    }
    if table_name not in ALLOWED:
        return f"Invalid table. Choose from:\n" + "\n".join(sorted(ALLOWED))

    limit = max(1, min(limit, 20))
    columns, rows, error = execute_sql(
        f"SELECT * FROM `{table_name}` LIMIT {limit}"
    )
    if error:
        return f"Error: {error}"
    return format_as_table(columns, rows)


@mcp.tool()
def get_table_stats() -> str:
    """
    Returns row counts, date ranges, and key statistics for all 6 AllPets tables.
    Use this to understand the scope and freshness of available data.
    """
    queries = {
        "allpets_invoice_line_items": """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT invoice_id) AS unique_invoices,
                SUM(cancelled='FALSE') AS active_rows,
                SUM(cancelled='TRUE') AS cancelled_rows,
                MIN(invoice_date) AS earliest_invoice,
                MAX(invoice_date) AS latest_invoice,
                ROUND(SUM(CASE WHEN cancelled='FALSE' THEN 0 END),2) AS placeholder
            FROM allpets_invoice_line_items
        """,
        "allpets_clients": """
            SELECT COUNT(*) AS total_clients,
                   MIN(first_activity) AS first_seen,
                   MAX(last_activity) AS last_seen
            FROM allpets_clients
        """,
        "allpets_appointments": """
            SELECT COUNT(*) AS total_appointments,
                   MIN(appointment_start_time) AS earliest,
                   MAX(appointment_start_time) AS latest,
                   COUNT(DISTINCT appointment_status) AS status_types
            FROM allpets_appointments
        """,
        "allpets_payments": """
            SELECT COUNT(*) AS total_payments,
                   ROUND(SUM(payment_amount),2) AS total_collected,
                   MIN(payment_date) AS earliest,
                   MAX(payment_date) AS latest
            FROM allpets_payments
        """,
        "allpets_stock": """
            SELECT COUNT(*) AS total_items,
                   COUNT(DISTINCT plan_category_name) AS categories,
                   SUM(onhand_qty < threshold_qty AND threshold_qty > 0) AS items_below_threshold,
                   ROUND(SUM(onhand_qty * purchase_cost),2) AS total_inventory_value
            FROM allpets_stock
        """,
        "allpets_patient_diagnosis": """
            SELECT COUNT(*) AS total_reports,
                   COUNT(DISTINCT client_id) AS unique_clients,
                   COUNT(DISTINCT plan_category_name) AS categories,
                   MIN(report_date) AS earliest_report,
                   MAX(report_date) AS latest_report
            FROM allpets_patient_diagnosis
        """,
    }

    output = []
    for table, query in queries.items():
        output.append(f"\n{'='*50}\n  {table}\n{'='*50}")
        columns, rows, error = execute_sql(query.strip())
        if error:
            output.append(f"Error: {error}")
        else:
            output.append(format_as_table(columns, rows))

    return "\n".join(output)


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"Starting AllPets RDS MCP Server on port {PORT}")
    mcp.run(transport="sse")
