import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify

app = Flask(__name__, template_folder='../templates')
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-me')

MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
FULL_MONTHS = ['January','February','March','April','May','June',
               'July','August','September','October','November','December']


def get_db():
    url = os.environ.get('POSTGRES_URL', '')
    if url.startswith('postgres://'):
        url = url.replace('postgres://', 'postgresql://', 1)
    conn = psycopg2.connect(url, cursor_factory=RealDictCursor)
    conn.autocommit = False
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            company_name TEXT DEFAULT 'YOUR COMPANY NAME',
            company_address TEXT DEFAULT 'Address Line 1 | City, State ZIP | Phone | Email',
            current_year INTEGER DEFAULT 2025
        )
    ''')
    cur.execute('INSERT INTO settings (id) VALUES (1) ON CONFLICT DO NOTHING')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS renters (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            unit TEXT NOT NULL,
            monthly_rent NUMERIC(10,2) DEFAULT 0,
            phone TEXT DEFAULT '',
            email TEXT DEFAULT ''
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            renter_id INTEGER NOT NULL REFERENCES renters(id) ON DELETE CASCADE,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            amount_paid NUMERIC(10,2) DEFAULT 0,
            fees NUMERIC(10,2) DEFAULT 0,
            UNIQUE(renter_id, year, month)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS fee_schedule (
            id SERIAL PRIMARY KEY,
            fee_type TEXT NOT NULL,
            amount NUMERIC(10,2) DEFAULT 0,
            description TEXT DEFAULT ''
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS invoices (
            id SERIAL PRIMARY KEY,
            invoice_number TEXT NOT NULL,
            renter_id INTEGER NOT NULL REFERENCES renters(id),
            invoice_date TEXT,
            due_date TEXT,
            period TEXT,
            notes TEXT DEFAULT 'Payment is due by the due date. Late fees may apply for late payments.',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Add new columns to invoices table (safe if already exist)
    cur.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS auto_generated BOOLEAN DEFAULT FALSE")
    cur.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS late_fee_day6_applied BOOLEAN DEFAULT FALSE")
    cur.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS late_fee_day10_applied BOOLEAN DEFAULT FALSE")
    cur.execute("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS month_year TEXT DEFAULT ''")

    cur.execute('''
        CREATE TABLE IF NOT EXISTS invoice_items (
            id SERIAL PRIMARY KEY,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
            description TEXT,
            qty INTEGER DEFAULT 1,
            unit_price NUMERIC(10,2) DEFAULT 0
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS receipts (
            id SERIAL PRIMARY KEY,
            receipt_number TEXT NOT NULL,
            renter_id INTEGER NOT NULL REFERENCES renters(id),
            payment_date TEXT,
            payment_method TEXT DEFAULT '',
            month TEXT,
            invoice_ref TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Add new columns to receipts table (safe if already exist)
    cur.execute("ALTER TABLE receipts ADD COLUMN IF NOT EXISTS deposit_confirmed BOOLEAN DEFAULT FALSE")
    cur.execute("ALTER TABLE receipts ADD COLUMN IF NOT EXISTS deposit_date TEXT DEFAULT ''")
    cur.execute("ALTER TABLE receipts ADD COLUMN IF NOT EXISTS receipt_type TEXT DEFAULT 'payment'")

    cur.execute('''
        CREATE TABLE IF NOT EXISTS receipt_items (
            id SERIAL PRIMARY KEY,
            receipt_id INTEGER NOT NULL REFERENCES receipts(id) ON DELETE CASCADE,
            description TEXT,
            period TEXT,
            amount NUMERIC(10,2) DEFAULT 0
        )
    ''')

    # Credits / Refunds table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS credits (
            id SERIAL PRIMARY KEY,
            renter_id INTEGER NOT NULL REFERENCES renters(id) ON DELETE CASCADE,
            credit_date TEXT NOT NULL,
            amount NUMERIC(10,2) DEFAULT 0,
            description TEXT DEFAULT '',
            credit_type TEXT DEFAULT 'credit',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Petty cash / Coins table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS petty_cash (
            id SERIAL PRIMARY KEY,
            transaction_date TEXT NOT NULL,
            description TEXT NOT NULL,
            amount NUMERIC(10,2) DEFAULT 0,
            transaction_type TEXT DEFAULT 'expense',
            category TEXT DEFAULT 'miscellaneous',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Seed default fee schedule if empty
    cur.execute("SELECT COUNT(*) as count FROM fee_schedule")
    count = cur.fetchone()['count']
    if count == 0:
        fees = [
            ('Late Payment Fee', 50, 'Manually apply when rent is received after the due date'),
            ('Returned Check Fee', 35, 'Applied when a payment check is returned/bounced'),
            ('Maintenance Fee', 0, 'One-time maintenance or repair charges'),
            ('Cleaning Fee', 0, 'Applied for cleaning services after inspection'),
            ('Pet Fee', 0, 'Monthly or one-time pet surcharge'),
            ('Parking Fee', 0, 'Additional parking space charge'),
            ('Storage Fee', 0, 'Storage unit or extra space fee'),
            ('Utility Overage', 0, 'Charges exceeding included utility allowance'),
        ]
        for ft, amt, desc in fees:
            cur.execute(
                "INSERT INTO fee_schedule (fee_type, amount, description) VALUES (%s, %s, %s)",
                (ft, amt, desc)
            )

    conn.commit()
    conn.close()


def get_payment_status(monthly_rent, amount_paid, fees):
    monthly_rent = float(monthly_rent or 0)
    amount_paid = float(amount_paid or 0)
    fees = float(fees or 0)
    if monthly_rent <= 0:
        return '--'
    total_due = monthly_rent + fees
    if amount_paid >= total_due:
        return 'Paid'
    elif amount_paid > 0:
        return 'Partial'
    return 'Unpaid'


def get_settings(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM settings WHERE id=1")
    return cur.fetchone()


def get_next_invoice_number(cur):
    cur.execute("SELECT invoice_number FROM invoices ORDER BY id DESC LIMIT 1")
    last = cur.fetchone()
    if last:
        try:
            num = int(last['invoice_number'].split('-')[1]) + 1
        except (IndexError, ValueError):
            num = 1
    else:
        num = 1
    return num


# ── ROUTES ──

@app.route('/')
def dashboard():
    conn = get_db()
    settings = get_settings(conn)
    year = request.args.get('year', settings['current_year'], type=int)
    cur = conn.cursor()
    cur.execute("SELECT * FROM renters ORDER BY id")
    renters = cur.fetchall()

    monthly_data = []
    for m in range(1, 13):
        expected = 0
        collected = 0
        fees_total = 0
        paid_count = 0
        unpaid_count = 0
        partial_count = 0
        for r in renters:
            if float(r['monthly_rent']) <= 0:
                continue
            expected += float(r['monthly_rent'])
            cur.execute(
                "SELECT amount_paid, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
                (r['id'], year, m)
            )
            pay = cur.fetchone()
            amt = float(pay['amount_paid']) if pay else 0
            fee = float(pay['fees']) if pay else 0
            collected += amt
            fees_total += fee
            status = get_payment_status(r['monthly_rent'], amt, fee)
            if status == 'Paid':
                paid_count += 1
            elif status == 'Partial':
                partial_count += 1
            elif status == 'Unpaid':
                unpaid_count += 1
        outstanding = expected + fees_total - collected
        rate = (collected / (expected + fees_total) * 100) if (expected + fees_total) > 0 else 0
        monthly_data.append({
            'month': MONTHS[m-1], 'expected': expected, 'collected': collected,
            'fees': fees_total, 'outstanding': outstanding,
            'paid': paid_count, 'unpaid': unpaid_count, 'partial': partial_count,
            'rate': rate
        })

    totals = {
        'expected': sum(d['expected'] for d in monthly_data),
        'collected': sum(d['collected'] for d in monthly_data),
        'fees': sum(d['fees'] for d in monthly_data),
        'outstanding': sum(d['outstanding'] for d in monthly_data),
        'paid': sum(d['paid'] for d in monthly_data),
        'unpaid': sum(d['unpaid'] for d in monthly_data),
        'partial': sum(d['partial'] for d in monthly_data),
    }
    totals['rate'] = (totals['collected'] / (totals['expected'] + totals['fees']) * 100) if (totals['expected'] + totals['fees']) > 0 else 0

    conn.close()
    return render_template('dashboard.html', monthly_data=monthly_data, totals=totals,
                           year=year, settings=settings, months=MONTHS)


@app.route('/renters')
def renters_list():
    conn = get_db()
    settings = get_settings(conn)
    year = request.args.get('year', settings['current_year'], type=int)
    cur = conn.cursor()
    cur.execute("SELECT * FROM renters ORDER BY id")
    renters = cur.fetchall()

    renter_data = []
    for r in renters:
        months = []
        total_paid = 0
        total_fees = 0
        for m in range(1, 13):
            cur.execute(
                "SELECT amount_paid, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
                (r['id'], year, m)
            )
            pay = cur.fetchone()
            amt = float(pay['amount_paid']) if pay else 0
            fee = float(pay['fees']) if pay else 0
            status = get_payment_status(r['monthly_rent'], amt, fee)
            total_paid += amt
            total_fees += fee
            months.append({'amt': amt, 'fee': fee, 'status': status})
        annual_rent = float(r['monthly_rent']) * 12
        balance = annual_rent + total_fees - total_paid
        renter_data.append({
            'renter': r, 'months': months,
            'total_paid': total_paid, 'total_fees': total_fees,
            'annual_rent': annual_rent, 'balance': balance
        })
    conn.close()
    return render_template('renters.html', renter_data=renter_data, year=year,
                           settings=settings, months=MONTHS)


@app.route('/renters/add', methods=['GET','POST'])
def add_renter():
    if request.method == 'POST':
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO renters (name, unit, monthly_rent, phone, email) VALUES (%s,%s,%s,%s,%s)",
            (request.form['name'], request.form['unit'],
             float(request.form.get('monthly_rent', 0)),
             request.form.get('phone', ''), request.form.get('email', ''))
        )
        conn.commit()
        conn.close()
        flash('Renter added successfully.', 'success')
        return redirect(url_for('renters_list'))
    conn = get_db()
    settings = get_settings(conn)
    conn.close()
    return render_template('renter_form.html', renter=None, settings=settings)


@app.route('/renters/<int:renter_id>/edit', methods=['GET','POST'])
def edit_renter(renter_id):
    conn = get_db()
    cur = conn.cursor()
    if request.method == 'POST':
        cur.execute(
            "UPDATE renters SET name=%s, unit=%s, monthly_rent=%s, phone=%s, email=%s WHERE id=%s",
            (request.form['name'], request.form['unit'],
             float(request.form.get('monthly_rent', 0)),
             request.form.get('phone', ''), request.form.get('email', ''), renter_id)
        )
        conn.commit()
        flash('Renter updated.', 'success')
        conn.close()
        return redirect(url_for('renters_list'))
    cur.execute("SELECT * FROM renters WHERE id=%s", (renter_id,))
    renter = cur.fetchone()
    settings = get_settings(conn)
    conn.close()
    return render_template('renter_form.html', renter=renter, settings=settings)


@app.route('/renters/<int:renter_id>/delete', methods=['POST'])
def delete_renter(renter_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM renters WHERE id=%s", (renter_id,))
    conn.commit()
    conn.close()
    flash('Renter deleted.', 'success')
    return redirect(url_for('renters_list'))


@app.route('/payments/<int:renter_id>', methods=['GET','POST'])
def manage_payments(renter_id):
    conn = get_db()
    settings = get_settings(conn)
    year = request.args.get('year', settings['current_year'], type=int)
    cur = conn.cursor()
    cur.execute("SELECT * FROM renters WHERE id=%s", (renter_id,))
    renter = cur.fetchone()

    if request.method == 'POST':
        year = int(request.form.get('year', year))
        for m in range(1, 13):
            amt = float(request.form.get(f'amt_{m}', 0))
            fee = float(request.form.get(f'fee_{m}', 0))
            cur.execute('''
                INSERT INTO payments (renter_id, year, month, amount_paid, fees)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (renter_id, year, month)
                DO UPDATE SET amount_paid=EXCLUDED.amount_paid, fees=EXCLUDED.fees
            ''', (renter_id, year, m, amt, fee))
        conn.commit()
        flash('Payments updated.', 'success')
        conn.close()
        return redirect(url_for('manage_payments', renter_id=renter_id, year=year))

    payments = []
    for m in range(1, 13):
        cur.execute(
            "SELECT amount_paid, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
            (renter_id, year, m)
        )
        pay = cur.fetchone()
        amt = float(pay['amount_paid']) if pay else 0
        fee = float(pay['fees']) if pay else 0
        status = get_payment_status(renter['monthly_rent'], amt, fee)
        payments.append({'month': MONTHS[m-1], 'month_num': m, 'amt': amt, 'fee': fee, 'status': status})

    conn.close()
    return render_template('payments.html', renter=renter, payments=payments,
                           year=year, settings=settings, months=MONTHS)


@app.route('/unpaid')
def unpaid_list():
    conn = get_db()
    settings = get_settings(conn)
    year = request.args.get('year', settings['current_year'], type=int)
    month = request.args.get('month', datetime.now().month, type=int)
    cur = conn.cursor()
    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 ORDER BY id")
    renters = cur.fetchall()

    unpaid = []
    for r in renters:
        cur.execute(
            "SELECT amount_paid, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
            (r['id'], year, month)
        )
        pay = cur.fetchone()
        amt = float(pay['amount_paid']) if pay else 0
        fee = float(pay['fees']) if pay else 0
        status = get_payment_status(r['monthly_rent'], amt, fee)
        unpaid.append({
            'renter': r, 'amt': amt, 'fee': fee, 'status': status
        })

    conn.close()
    return render_template('unpaid.html', unpaid=unpaid, year=year, month=month,
                           settings=settings, months=MONTHS)


# ── INVOICES ──

@app.route('/invoices')
def invoices_list():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute('''
        SELECT invoices.*, renters.name as renter_name, renters.unit,
               COALESCE(SUM(ii.qty * ii.unit_price), 0) as total
        FROM invoices
        JOIN renters ON invoices.renter_id = renters.id
        LEFT JOIN invoice_items ii ON ii.invoice_id = invoices.id
        GROUP BY invoices.id, renters.name, renters.unit
        ORDER BY invoices.id DESC
    ''')
    invoices = cur.fetchall()
    conn.close()
    return render_template('invoices_list.html', invoices=invoices, settings=settings,
                           months=MONTHS, full_months=FULL_MONTHS,
                           current_year=date.today().year,
                           current_month=date.today().month)


@app.route('/invoices/generate-monthly', methods=['POST'])
def generate_monthly_invoices():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()

    month = int(request.form.get('month', date.today().month))
    year = int(request.form.get('year', settings['current_year']))

    period = f"{FULL_MONTHS[month-1]} {year}"
    invoice_date = f"{year}-{month:02d}-01"
    due_date = f"{year}-{month:02d}-05"

    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 ORDER BY id")
    renters = cur.fetchall()

    num = get_next_invoice_number(cur)
    created = 0
    skipped = 0

    for renter in renters:
        # Skip if already generated for this renter/period
        cur.execute(
            "SELECT id FROM invoices WHERE renter_id=%s AND period=%s AND auto_generated=TRUE",
            (renter['id'], period)
        )
        if cur.fetchone():
            skipped += 1
            continue

        inv_num = f"INV-{num:04d}"
        cur.execute(
            """INSERT INTO invoices
               (invoice_number, renter_id, invoice_date, due_date, period, notes,
                auto_generated, month_year)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (inv_num, renter['id'], invoice_date, due_date, period,
             f"Payment due by {due_date}. A 10% late fee plus $75 magistrate fee applies on day 6. An additional 10% applies on day 10.",
             True, f"{year}-{month:02d}")
        )
        invoice_id = cur.fetchone()['id']
        cur.execute(
            "INSERT INTO invoice_items (invoice_id, description, qty, unit_price) VALUES (%s,%s,%s,%s)",
            (invoice_id, 'Monthly Rent', 1, float(renter['monthly_rent']))
        )
        num += 1
        created += 1

    conn.commit()
    conn.close()
    flash(f'Generated {created} invoice(s) for {period}. {skipped} skipped (already exist).', 'success')
    return redirect(url_for('invoices_list'))


@app.route('/invoices/create', methods=['GET','POST'])
def create_invoice():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 ORDER BY name")
    renters = cur.fetchall()

    if request.method == 'POST':
        renter_id = int(request.form['renter_id'])
        inv_num = request.form['invoice_number']
        inv_date = request.form.get('invoice_date', '')
        due_date = request.form.get('due_date', '')
        period = request.form.get('period', '')
        notes = request.form.get('notes', '')
        extra_fee = float(request.form.get('extra_fee', 0))

        cur.execute("SELECT * FROM renters WHERE id=%s", (renter_id,))
        renter = cur.fetchone()

        cur.execute(
            "INSERT INTO invoices (invoice_number, renter_id, invoice_date, due_date, period, notes) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
            (inv_num, renter_id, inv_date, due_date, period, notes)
        )
        invoice_id = cur.fetchone()['id']
        cur.execute(
            "INSERT INTO invoice_items (invoice_id, description, qty, unit_price) VALUES (%s,%s,%s,%s)",
            (invoice_id, 'Monthly Rent', 1, float(renter['monthly_rent']))
        )
        if extra_fee > 0:
            fee_desc = request.form.get('fee_description', 'Additional Fee')
            cur.execute(
                "INSERT INTO invoice_items (invoice_id, description, qty, unit_price) VALUES (%s,%s,%s,%s)",
                (invoice_id, fee_desc, 1, extra_fee)
            )
        conn.commit()
        conn.close()
        flash('Invoice created.', 'success')
        return redirect(url_for('view_invoice', invoice_id=invoice_id))

    num = get_next_invoice_number(cur)
    next_num = f"INV-{num:04d}"
    cur.execute("SELECT * FROM fee_schedule ORDER BY id")
    fee_types = cur.fetchall()
    conn.close()
    return render_template('invoice_form.html', renters=renters, next_num=next_num,
                           fee_types=fee_types, settings=settings)


@app.route('/invoices/<int:invoice_id>')
def view_invoice(invoice_id):
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute('''
        SELECT invoices.*, renters.name, renters.unit, renters.monthly_rent,
               renters.phone, renters.email
        FROM invoices JOIN renters ON invoices.renter_id = renters.id
        WHERE invoices.id=%s
    ''', (invoice_id,))
    invoice = cur.fetchone()
    cur.execute("SELECT * FROM invoice_items WHERE invoice_id=%s", (invoice_id,))
    items = cur.fetchall()
    subtotal = sum(float(i['qty']) * float(i['unit_price']) for i in items)
    today = date.today()
    days_overdue = 0
    if invoice and invoice['due_date']:
        try:
            due = datetime.strptime(invoice['due_date'], '%Y-%m-%d').date()
            days_overdue = (today - due).days
        except (ValueError, TypeError):
            pass
    conn.close()
    return render_template('invoice_view.html', invoice=invoice, items=items,
                           subtotal=subtotal, settings=settings,
                           days_overdue=days_overdue, today=today)


@app.route('/invoices/<int:invoice_id>/apply-late-fees', methods=['POST'])
def apply_late_fees(invoice_id):
    conn = get_db()
    cur = conn.cursor()

    cur.execute('''
        SELECT invoices.*, renters.monthly_rent
        FROM invoices JOIN renters ON invoices.renter_id = renters.id
        WHERE invoices.id=%s
    ''', (invoice_id,))
    invoice = cur.fetchone()

    if not invoice:
        conn.close()
        flash('Invoice not found.', 'danger')
        return redirect(url_for('invoices_list'))

    today = date.today()
    try:
        due_date = datetime.strptime(invoice['due_date'], '%Y-%m-%d').date()
    except (ValueError, TypeError):
        conn.close()
        flash('Invalid due date on invoice.', 'danger')
        return redirect(url_for('view_invoice', invoice_id=invoice_id))

    days_overdue = (today - due_date).days
    changes = []

    # Get current invoice items
    cur.execute("SELECT * FROM invoice_items WHERE invoice_id=%s", (invoice_id,))
    items = cur.fetchall()

    # Base items = everything that isn't a late fee or magistrate fee
    late_tags = ['Late Fee', 'Magistrate Fee']
    base_items = [i for i in items if not any(tag in i['description'] for tag in late_tags)]
    base_total = sum(float(i['qty']) * float(i['unit_price']) for i in base_items)

    # Day 6 (1+ days past due date of 5th = 6th of month)
    if days_overdue >= 1 and not invoice['late_fee_day6_applied']:
        day6_pct = round(base_total * 0.10, 2)
        cur.execute(
            "INSERT INTO invoice_items (invoice_id, description, qty, unit_price) VALUES (%s,%s,%s,%s)",
            (invoice_id, 'Late Fee – Day 6 (10%)', 1, day6_pct)
        )
        cur.execute(
            "INSERT INTO invoice_items (invoice_id, description, qty, unit_price) VALUES (%s,%s,%s,%s)",
            (invoice_id, 'Magistrate Fee', 1, 75.00)
        )
        cur.execute("UPDATE invoices SET late_fee_day6_applied=TRUE WHERE id=%s", (invoice_id,))
        changes.append(f'Day 6: +${day6_pct:.2f} (10%) + $75.00 magistrate fee')

    # Day 10 (5+ days past due date = 10th of month)
    if days_overdue >= 5 and not invoice['late_fee_day10_applied']:
        # Get running total including any day 6 fees just added
        cur.execute(
            "SELECT COALESCE(SUM(qty * unit_price), 0) as total FROM invoice_items WHERE invoice_id=%s",
            (invoice_id,)
        )
        current_total = float(cur.fetchone()['total'])
        day10_pct = round(current_total * 0.10, 2)
        cur.execute(
            "INSERT INTO invoice_items (invoice_id, description, qty, unit_price) VALUES (%s,%s,%s,%s)",
            (invoice_id, 'Late Fee – Day 10 (10%)', 1, day10_pct)
        )
        cur.execute("UPDATE invoices SET late_fee_day10_applied=TRUE WHERE id=%s", (invoice_id,))
        changes.append(f'Day 10: +${day10_pct:.2f} (10% of ${current_total:.2f})')

    if changes:
        conn.commit()
        flash('Late fees applied: ' + '; '.join(changes), 'success')
    else:
        flash('No new late fees to apply (check that invoice is overdue and fees not already applied).', 'info')

    conn.close()
    return redirect(url_for('view_invoice', invoice_id=invoice_id))


# ── RECEIPTS ──

@app.route('/receipts')
def receipts_list():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute('''
        SELECT receipts.*, renters.name as renter_name, renters.unit,
               COALESCE(SUM(ri.amount), 0) as total
        FROM receipts
        JOIN renters ON receipts.renter_id = renters.id
        LEFT JOIN receipt_items ri ON ri.receipt_id = receipts.id
        GROUP BY receipts.id, renters.name, renters.unit
        ORDER BY receipts.id DESC
    ''')
    receipts = cur.fetchall()
    conn.close()
    return render_template('receipts_list.html', receipts=receipts, settings=settings)


@app.route('/receipts/create', methods=['GET','POST'])
def create_receipt():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 ORDER BY name")
    renters = cur.fetchall()

    if request.method == 'POST':
        renter_id = int(request.form['renter_id'])
        rec_num = request.form['receipt_number']
        pay_date = request.form.get('payment_date', '')
        pay_method = request.form.get('payment_method', '')
        month = request.form.get('month', '')
        from_invoice = request.form.get('from_invoice', '')
        receipt_type = request.form.get('receipt_type', 'payment')

        cur.execute("SELECT * FROM renters WHERE id=%s", (renter_id,))
        renter = cur.fetchone()

        # Collect line items from form
        line_items = []
        i = 0
        while f'item_desc_{i}' in request.form:
            desc = request.form.get(f'item_desc_{i}', '').strip()
            amt = float(request.form.get(f'item_amt_{i}', 0))
            if desc and amt > 0:
                line_items.append((desc, amt))
            i += 1
        if not line_items:
            amount = float(request.form.get('amount', 0))
            if amount > 0:
                line_items.append(('Rent', amount))

        total_amount = sum(amt for _, amt in line_items)

        # Server-side validation for regular payments (not deposits)
        month_num = MONTHS.index(month) + 1 if month in MONTHS else None
        if month_num and renter and receipt_type == 'payment':
            pay_year = settings['current_year']
            if pay_date:
                try:
                    pay_year = int(pay_date.split('-')[0])
                except (IndexError, ValueError):
                    pass
            cur.execute(
                "SELECT amount_paid, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
                (renter_id, pay_year, month_num)
            )
            pay = cur.fetchone()
            already_paid = float(pay['amount_paid']) if pay else 0
            fees = float(pay['fees']) if pay else 0
            total_due = float(renter['monthly_rent']) + fees

            if from_invoice:
                cur.execute("SELECT id FROM invoices WHERE invoice_number=%s", (from_invoice,))
                inv = cur.fetchone()
                if inv:
                    cur.execute(
                        "SELECT SUM(qty * unit_price) as total FROM invoice_items WHERE invoice_id=%s",
                        (inv['id'],)
                    )
                    inv_total = cur.fetchone()
                    if inv_total and inv_total['total']:
                        total_due = max(float(inv_total['total']), total_due)

            remaining = total_due - already_paid
            if total_amount > remaining:
                flash(f'Payment of ${total_amount:,.2f} exceeds remaining balance of ${remaining:,.2f}. Receipt not created.', 'danger')
                conn.close()
                return redirect(url_for('create_receipt'))

        cur.execute(
            "INSERT INTO receipts (receipt_number, renter_id, payment_date, payment_method, month, invoice_ref, receipt_type) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
            (rec_num, renter_id, pay_date, pay_method, month, from_invoice, receipt_type)
        )
        receipt_id = cur.fetchone()['id']

        for desc, amt in line_items:
            cur.execute(
                "INSERT INTO receipt_items (receipt_id, description, period, amount) VALUES (%s,%s,%s,%s)",
                (receipt_id, desc, month, amt)
            )

        # Update payments table for standard payment receipts
        if month_num and receipt_type == 'payment':
            pay_year = settings['current_year']
            if pay_date:
                try:
                    pay_year = int(pay_date.split('-')[0])
                except (IndexError, ValueError):
                    pass

            cur.execute(
                "SELECT amount_paid, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
                (renter_id, pay_year, month_num)
            )
            existing = cur.fetchone()

            if existing:
                new_amt = float(existing['amount_paid']) + total_amount
                cur.execute(
                    "UPDATE payments SET amount_paid=%s WHERE renter_id=%s AND year=%s AND month=%s",
                    (new_amt, renter_id, pay_year, month_num)
                )
            else:
                cur.execute(
                    "INSERT INTO payments (renter_id, year, month, amount_paid, fees) VALUES (%s,%s,%s,%s,%s)",
                    (renter_id, pay_year, month_num, total_amount, 0)
                )

        conn.commit()
        conn.close()
        return redirect(url_for('view_receipt', receipt_id=receipt_id))

    cur.execute("SELECT receipt_number FROM receipts ORDER BY id DESC LIMIT 1")
    last = cur.fetchone()
    if last:
        try:
            num = int(last['receipt_number'].split('-')[1]) + 1
        except (IndexError, ValueError):
            num = 1
    else:
        num = 1
    next_num = f"REC-{num:04d}"
    cur.execute("SELECT * FROM fee_schedule ORDER BY id")
    fee_types = cur.fetchall()
    cur.execute('''
        SELECT invoices.id, invoices.invoice_number, invoices.renter_id, renters.name as renter_name
        FROM invoices JOIN renters ON invoices.renter_id = renters.id
        ORDER BY invoices.id DESC
    ''')
    invoices = cur.fetchall()

    conn.close()
    return render_template('receipt_form.html', renters=renters, next_num=next_num,
                           settings=settings, months=MONTHS, fee_types=fee_types,
                           invoices=invoices)


@app.route('/receipts/<int:receipt_id>')
def view_receipt(receipt_id):
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute('''
        SELECT receipts.*, renters.name, renters.unit, renters.monthly_rent,
               renters.phone, renters.email
        FROM receipts JOIN renters ON receipts.renter_id = renters.id
        WHERE receipts.id=%s
    ''', (receipt_id,))
    receipt = cur.fetchone()
    cur.execute("SELECT * FROM receipt_items WHERE receipt_id=%s", (receipt_id,))
    items = cur.fetchall()
    total = sum(float(i['amount']) for i in items)

    month_name = receipt['month']
    month_num = MONTHS.index(month_name) + 1 if month_name in MONTHS else None
    total_paid_month = 0
    fees_month = 0
    if month_num:
        pay_year = settings['current_year']
        if receipt['payment_date']:
            try:
                pay_year = int(receipt['payment_date'].split('-')[0])
            except (IndexError, ValueError):
                pass

        cur.execute('''
            SELECT COALESCE(SUM(ri.amount), 0) as total
            FROM receipt_items ri
            JOIN receipts r ON ri.receipt_id = r.id
            WHERE r.renter_id = %s AND r.month = %s AND r.id <= %s
        ''', (receipt['renter_id'], month_name, receipt_id))
        result = cur.fetchone()
        total_paid_month = float(result['total'])

        cur.execute(
            "SELECT fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
            (receipt['renter_id'], pay_year, month_num)
        )
        pay = cur.fetchone()
        if pay:
            fees_month = float(pay['fees'])

    conn.close()
    return render_template('receipt_view.html', receipt=receipt, items=items,
                           total=total, total_paid_month=total_paid_month,
                           fees_month=fees_month, settings=settings)


@app.route('/receipts/<int:receipt_id>/confirm-deposit', methods=['POST'])
def confirm_deposit(receipt_id):
    conn = get_db()
    cur = conn.cursor()
    deposit_date = request.form.get('deposit_date', date.today().isoformat())
    cur.execute(
        "UPDATE receipts SET deposit_confirmed=TRUE, deposit_date=%s WHERE id=%s",
        (deposit_date, receipt_id)
    )
    conn.commit()
    conn.close()
    flash('Deposit confirmed.', 'success')
    return redirect(url_for('deposits_list'))


@app.route('/receipts/<int:receipt_id>/unconfirm-deposit', methods=['POST'])
def unconfirm_deposit(receipt_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE receipts SET deposit_confirmed=FALSE, deposit_date='' WHERE id=%s",
        (receipt_id,)
    )
    conn.commit()
    conn.close()
    flash('Deposit confirmation removed.', 'info')
    return redirect(url_for('deposits_list'))


# ── DEPOSITS ──

@app.route('/deposits')
def deposits_list():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute('''
        SELECT receipts.*, renters.name as renter_name, renters.unit,
               COALESCE(SUM(ri.amount), 0) as total
        FROM receipts
        JOIN renters ON receipts.renter_id = renters.id
        LEFT JOIN receipt_items ri ON ri.receipt_id = receipts.id
        GROUP BY receipts.id, renters.name, renters.unit
        ORDER BY receipts.deposit_confirmed ASC, receipts.payment_date DESC
    ''')
    receipts = cur.fetchall()
    pending = [r for r in receipts if not r['deposit_confirmed']]
    confirmed = [r for r in receipts if r['deposit_confirmed']]
    pending_total = sum(float(r['total']) for r in pending)
    conn.close()
    return render_template('deposits.html', pending=pending, confirmed=confirmed,
                           pending_total=pending_total, settings=settings,
                           today=date.today().isoformat())


# ── CREDITS / REFUNDS ──

@app.route('/credits')
def credits_list():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute('''
        SELECT credits.*, renters.name as renter_name, renters.unit
        FROM credits
        JOIN renters ON credits.renter_id = renters.id
        ORDER BY credits.id DESC
    ''')
    credits = cur.fetchall()
    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 ORDER BY name")
    renters = cur.fetchall()
    total_credits = sum(float(c['amount']) for c in credits)
    conn.close()
    return render_template('credits.html', credits=credits, renters=renters,
                           total_credits=total_credits, settings=settings,
                           today=date.today().isoformat())


@app.route('/credits/add', methods=['POST'])
def add_credit():
    conn = get_db()
    cur = conn.cursor()
    renter_id = int(request.form['renter_id'])
    amount = float(request.form.get('amount', 0))
    description = request.form.get('description', '')
    credit_date = request.form.get('credit_date', date.today().isoformat())
    credit_type = request.form.get('credit_type', 'credit')

    if amount <= 0:
        flash('Amount must be greater than zero.', 'danger')
        conn.close()
        return redirect(url_for('credits_list'))

    cur.execute(
        "INSERT INTO credits (renter_id, credit_date, amount, description, credit_type) VALUES (%s,%s,%s,%s,%s)",
        (renter_id, credit_date, amount, description, credit_type)
    )
    conn.commit()
    conn.close()
    flash('Credit/refund added.', 'success')
    return redirect(url_for('credits_list'))


@app.route('/credits/<int:credit_id>/delete', methods=['POST'])
def delete_credit(credit_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM credits WHERE id=%s", (credit_id,))
    conn.commit()
    conn.close()
    flash('Credit deleted.', 'success')
    return redirect(url_for('credits_list'))


# ── PETTY CASH / COINS ──

@app.route('/petty-cash')
def petty_cash_list():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM petty_cash ORDER BY transaction_date DESC, id DESC")
    transactions = cur.fetchall()
    total_in = sum(float(t['amount']) for t in transactions if t['transaction_type'] == 'in')
    total_out = sum(float(t['amount']) for t in transactions if t['transaction_type'] == 'expense')
    balance = total_in - total_out
    conn.close()
    return render_template('petty_cash.html', transactions=transactions,
                           total_in=total_in, total_out=total_out, balance=balance,
                           settings=settings, today=date.today().isoformat())


@app.route('/petty-cash/add', methods=['POST'])
def add_petty_cash():
    conn = get_db()
    cur = conn.cursor()
    txn_date = request.form.get('transaction_date', date.today().isoformat())
    description = request.form.get('description', '').strip()
    amount = float(request.form.get('amount', 0))
    txn_type = request.form.get('transaction_type', 'expense')
    category = request.form.get('category', 'miscellaneous')

    if not description or amount <= 0:
        flash('Description and amount are required.', 'danger')
        conn.close()
        return redirect(url_for('petty_cash_list'))

    cur.execute(
        "INSERT INTO petty_cash (transaction_date, description, amount, transaction_type, category) VALUES (%s,%s,%s,%s,%s)",
        (txn_date, description, amount, txn_type, category)
    )
    conn.commit()
    conn.close()
    flash('Petty cash entry added.', 'success')
    return redirect(url_for('petty_cash_list'))


@app.route('/petty-cash/<int:item_id>/delete', methods=['POST'])
def delete_petty_cash(item_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM petty_cash WHERE id=%s", (item_id,))
    conn.commit()
    conn.close()
    flash('Entry deleted.', 'success')
    return redirect(url_for('petty_cash_list'))


# ── STATEMENTS ──

@app.route('/statements')
def statements_index():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 ORDER BY name")
    renters = cur.fetchall()
    conn.close()
    return render_template('statements_index.html', renters=renters, settings=settings)


@app.route('/statements/<int:renter_id>')
def renter_statement(renter_id):
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()

    cur.execute("SELECT * FROM renters WHERE id=%s", (renter_id,))
    renter = cur.fetchone()

    year = request.args.get('year', settings['current_year'], type=int)

    # Get all invoices for this renter in the year
    cur.execute('''
        SELECT invoices.*, COALESCE(SUM(ii.qty * ii.unit_price), 0) as total
        FROM invoices
        LEFT JOIN invoice_items ii ON ii.invoice_id = invoices.id
        WHERE invoices.renter_id=%s AND (
            invoices.invoice_date LIKE %s OR invoices.period LIKE %s
        )
        GROUP BY invoices.id
        ORDER BY invoices.invoice_date ASC
    ''', (renter_id, f"{year}%", f"%{year}%"))
    invoices = cur.fetchall()

    # Get all receipts for this renter in the year
    cur.execute('''
        SELECT receipts.*, COALESCE(SUM(ri.amount), 0) as total
        FROM receipts
        LEFT JOIN receipt_items ri ON ri.receipt_id = receipts.id
        WHERE receipts.renter_id=%s AND (
            receipts.payment_date LIKE %s OR receipts.month != ''
        )
        GROUP BY receipts.id
        ORDER BY receipts.payment_date ASC
    ''', (renter_id, f"{year}%"))
    receipts = cur.fetchall()

    # Get all credits for this renter in the year
    cur.execute(
        "SELECT * FROM credits WHERE renter_id=%s AND credit_date LIKE %s ORDER BY credit_date ASC",
        (renter_id, f"{year}%")
    )
    credits = cur.fetchall()

    # Build statement ledger entries
    ledger = []
    for inv in invoices:
        ledger.append({
            'date': inv['invoice_date'] or '',
            'type': 'invoice',
            'ref': inv['invoice_number'],
            'description': f"Invoice – {inv['period']}",
            'charge': float(inv['total']),
            'payment': 0,
            'id': inv['id']
        })
    for rec in receipts:
        ledger.append({
            'date': rec['payment_date'] or '',
            'type': 'payment',
            'ref': rec['receipt_number'],
            'description': f"Payment – {rec['month']} ({rec['payment_method']})",
            'charge': 0,
            'payment': float(rec['total']),
            'id': rec['id']
        })
    for cr in credits:
        ledger.append({
            'date': cr['credit_date'],
            'type': 'credit',
            'ref': f"CR-{cr['id']:04d}",
            'description': f"{cr['credit_type'].title()} – {cr['description']}",
            'charge': 0,
            'payment': float(cr['amount']),
            'id': cr['id']
        })

    # Sort by date
    ledger.sort(key=lambda x: x['date'] or '0000')

    # Calculate running balance
    running_balance = 0
    for entry in ledger:
        running_balance += entry['charge'] - entry['payment']
        entry['balance'] = running_balance

    total_charges = sum(e['charge'] for e in ledger)
    total_payments = sum(e['payment'] for e in ledger)
    balance_due = total_charges - total_payments

    conn.close()
    return render_template('statement.html', renter=renter, ledger=ledger,
                           total_charges=total_charges, total_payments=total_payments,
                           balance_due=balance_due, year=year, settings=settings)


# ── FEE SCHEDULE ──

@app.route('/fees', methods=['GET','POST'])
def fee_schedule():
    conn = get_db()
    cur = conn.cursor()
    if request.method == 'POST':
        cur.execute("DELETE FROM fee_schedule")
        i = 0
        while f'fee_type_{i}' in request.form:
            cur.execute(
                "INSERT INTO fee_schedule (fee_type, amount, description) VALUES (%s,%s,%s)",
                (request.form[f'fee_type_{i}'],
                 float(request.form.get(f'fee_amount_{i}', 0)),
                 request.form.get(f'fee_desc_{i}', ''))
            )
            i += 1
        conn.commit()
        flash('Fee schedule updated.', 'success')
        conn.close()
        return redirect(url_for('fee_schedule'))

    cur.execute("SELECT * FROM fee_schedule ORDER BY id")
    fees = cur.fetchall()
    settings = get_settings(conn)
    conn.close()
    return render_template('fees.html', fees=fees, settings=settings)


# ── SETTINGS ──

@app.route('/settings', methods=['GET','POST'])
def settings_page():
    conn = get_db()
    cur = conn.cursor()
    if request.method == 'POST':
        cur.execute(
            "UPDATE settings SET company_name=%s, company_address=%s, current_year=%s WHERE id=1",
            (request.form['company_name'], request.form['company_address'],
             int(request.form.get('current_year', 2025)))
        )
        conn.commit()
        flash('Settings updated.', 'success')
        conn.close()
        return redirect(url_for('settings_page'))
    settings = get_settings(conn)
    conn.close()
    return render_template('settings.html', settings=settings)


# ── JSON APIs ──

@app.route('/api/invoice-details/<int:invoice_id>')
def api_invoice_details(invoice_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT invoices.*, renters.name, renters.unit, renters.monthly_rent
        FROM invoices JOIN renters ON invoices.renter_id = renters.id
        WHERE invoices.id=%s
    ''', (invoice_id,))
    invoice = cur.fetchone()
    if not invoice:
        conn.close()
        return jsonify({'error': 'not found'}), 404

    cur.execute("SELECT * FROM invoice_items WHERE invoice_id=%s", (invoice_id,))
    items = cur.fetchall()

    inv_month = ''
    if invoice['period']:
        for i, fm in enumerate(FULL_MONTHS):
            if fm.lower() in invoice['period'].lower():
                inv_month = MONTHS[i]
                break
        if not inv_month:
            for m in MONTHS:
                if invoice['period'].lower().startswith(m.lower()):
                    inv_month = m
                    break

    conn.close()
    return jsonify({
        'invoice_id': invoice_id,
        'invoice_number': invoice['invoice_number'],
        'renter_id': invoice['renter_id'],
        'renter_name': invoice['name'],
        'unit': invoice['unit'],
        'monthly_rent': float(invoice['monthly_rent']),
        'period': invoice['period'],
        'month': inv_month,
        'items': [{'description': it['description'], 'amount': float(it['qty']) * float(it['unit_price'])} for it in items],
        'total': sum(float(it['qty']) * float(it['unit_price']) for it in items)
    })


@app.route('/api/remaining-balance')
def api_remaining_balance():
    renter_id = request.args.get('renter_id', 0, type=int)
    month = request.args.get('month', '')
    invoice_id = request.args.get('invoice_id', 0, type=int)
    conn = get_db()
    settings = get_settings(conn)
    year = settings['current_year']
    cur = conn.cursor()

    cur.execute("SELECT * FROM renters WHERE id=%s", (renter_id,))
    renter = cur.fetchone()
    if not renter or month not in MONTHS:
        conn.close()
        return jsonify({'remaining': 0, 'rent': 0, 'paid': 0, 'fees': 0, 'total_due': 0})

    month_num = MONTHS.index(month) + 1
    cur.execute(
        "SELECT amount_paid, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
        (renter_id, year, month_num)
    )
    pay = cur.fetchone()
    paid = float(pay['amount_paid']) if pay else 0
    fees = float(pay['fees']) if pay else 0

    if invoice_id:
        cur.execute(
            "SELECT SUM(qty * unit_price) as total FROM invoice_items WHERE invoice_id=%s",
            (invoice_id,)
        )
        inv_items = cur.fetchone()
        invoice_total = float(inv_items['total']) if inv_items and inv_items['total'] else 0
        total_due = max(invoice_total, float(renter['monthly_rent']) + fees)
    else:
        total_due = float(renter['monthly_rent']) + fees

    remaining = max(total_due - paid, 0)

    conn.close()
    return jsonify({
        'remaining': remaining,
        'rent': float(renter['monthly_rent']),
        'paid': paid,
        'fees': fees,
        'total_due': total_due
    })


# Initialize database tables on first request
_db_initialized = False

@app.before_request
def ensure_db():
    global _db_initialized
    if not _db_initialized:
        init_db()
        _db_initialized = True
