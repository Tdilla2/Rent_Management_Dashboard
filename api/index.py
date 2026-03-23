import os
import functools
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session, g
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__, template_folder='../templates', static_folder='../static')
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
            email TEXT DEFAULT '',
            co_leaser TEXT DEFAULT '',
            co_leaser_email TEXT DEFAULT '',
            co_leaser_phone TEXT DEFAULT '',
            is_active BOOLEAN DEFAULT TRUE
        )
    ''')
    # Add new columns to renters table (safe if already exist)
    for col, coldef in [
        ('co_leaser', "TEXT DEFAULT ''"),
        ('co_leaser_email', "TEXT DEFAULT ''"),
        ('co_leaser_phone', "TEXT DEFAULT ''"),
        ('is_active', 'BOOLEAN DEFAULT TRUE'),
    ]:
        try:
            cur.execute(f"ALTER TABLE renters ADD COLUMN IF NOT EXISTS {col} {coldef}")
        except Exception:
            pass

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

    # Users table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            display_name TEXT DEFAULT '',
            role TEXT DEFAULT 'viewer',
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS security_question TEXT DEFAULT ''")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS security_answer_hash TEXT DEFAULT ''")

    # Seed default admin user if no users exist
    cur.execute("SELECT COUNT(*) as count FROM users")
    if cur.fetchone()['count'] == 0:
        admin_hash = generate_password_hash('admin123')
        cur.execute(
            "INSERT INTO users (username, password_hash, display_name, role) VALUES (%s, %s, %s, %s)",
            ('admin', admin_hash, 'Administrator', 'admin')
        )

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


# ── AUTH ──

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        if session.get('user_role') != 'admin':
            flash('Admin access required.', 'danger')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated


@app.before_request
def load_user():
    g.user = None
    if 'user_id' in session:
        g.user = {
            'id': session['user_id'],
            'username': session.get('username'),
            'display_name': session.get('display_name'),
            'role': session.get('user_role')
        }


@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip().lower()
        password = request.form.get('password', '')
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE username=%s AND is_active=TRUE", (username,))
        user = cur.fetchone()
        conn.close()
        if user and check_password_hash(user['password_hash'], password):
            session['user_id'] = user['id']
            session['username'] = user['username']
            session['display_name'] = user['display_name'] or user['username']
            session['user_role'] = user['role']
            session.permanent = True
            flash(f'Welcome, {session["display_name"]}!', 'success')
            return redirect(url_for('dashboard'))
        flash('Invalid username or password.', 'danger')
    conn = get_db()
    settings = get_settings(conn)
    conn.close()
    return render_template('login.html', settings=settings)


@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        step = request.form.get('step', 'lookup')
        conn = get_db()
        cur = conn.cursor()

        if step == 'lookup':
            username = request.form.get('username', '').strip().lower()
            cur.execute("SELECT id, username, security_question, security_answer_hash FROM users WHERE username=%s AND is_active=TRUE", (username,))
            user = cur.fetchone()
            conn.close()
            if not user:
                flash('Username not found.', 'danger')
                return redirect(url_for('forgot_password'))
            if not user['security_question'] or not user['security_answer_hash']:
                flash('No security question set for this account. Please contact an administrator to reset your password.', 'warning')
                return redirect(url_for('forgot_password'))
            settings = get_settings(get_db())
            return render_template('forgot_password.html', settings=settings,
                                   step='answer', username=user['username'],
                                   security_question=user['security_question'])

        elif step == 'answer':
            username = request.form.get('username', '').strip().lower()
            answer = request.form.get('security_answer', '').strip().lower()
            cur.execute("SELECT id, security_answer_hash FROM users WHERE username=%s AND is_active=TRUE", (username,))
            user = cur.fetchone()
            conn.close()
            if not user or not check_password_hash(user['security_answer_hash'], answer):
                flash('Incorrect answer. Please try again or contact an administrator.', 'danger')
                return redirect(url_for('forgot_password'))
            settings = get_settings(get_db())
            return render_template('forgot_password.html', settings=settings,
                                   step='reset', username=username)

        elif step == 'reset':
            username = request.form.get('username', '').strip().lower()
            new_pw = request.form.get('new_password', '')
            confirm = request.form.get('confirm_password', '')
            if len(new_pw) < 6:
                flash('Password must be at least 6 characters.', 'danger')
                settings = get_settings(conn)
                conn.close()
                return render_template('forgot_password.html', settings=settings,
                                       step='reset', username=username)
            if new_pw != confirm:
                flash('Passwords do not match.', 'danger')
                settings = get_settings(conn)
                conn.close()
                return render_template('forgot_password.html', settings=settings,
                                       step='reset', username=username)
            cur.execute("UPDATE users SET password_hash=%s WHERE username=%s",
                        (generate_password_hash(new_pw), username))
            conn.commit()
            conn.close()
            flash('Password has been reset. You can now sign in.', 'success')
            return redirect(url_for('login'))

    conn = get_db()
    settings = get_settings(conn)
    conn.close()
    return render_template('forgot_password.html', settings=settings, step='lookup')


@app.route('/setup-security-question', methods=['GET', 'POST'])
@login_required
def setup_security_question():
    if request.method == 'POST':
        question = request.form.get('security_question', '').strip()
        answer = request.form.get('security_answer', '').strip().lower()
        if not question or not answer:
            flash('Both question and answer are required.', 'danger')
            return redirect(url_for('setup_security_question'))
        conn = get_db()
        cur = conn.cursor()
        cur.execute("UPDATE users SET security_question=%s, security_answer_hash=%s WHERE id=%s",
                    (question, generate_password_hash(answer), session['user_id']))
        conn.commit()
        conn.close()
        flash('Security question saved.', 'success')
        return redirect(url_for('setup_security_question'))
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute("SELECT security_question FROM users WHERE id=%s", (session['user_id'],))
    user = cur.fetchone()
    conn.close()
    return render_template('security_question.html', settings=settings,
                           current_question=user['security_question'] if user else '')


@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))


@app.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password():
    if request.method == 'POST':
        current = request.form.get('current_password', '')
        new_pw = request.form.get('new_password', '')
        confirm = request.form.get('confirm_password', '')
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE id=%s", (session['user_id'],))
        user = cur.fetchone()
        if not check_password_hash(user['password_hash'], current):
            flash('Current password is incorrect.', 'danger')
        elif len(new_pw) < 6:
            flash('New password must be at least 6 characters.', 'danger')
        elif new_pw != confirm:
            flash('New passwords do not match.', 'danger')
        else:
            cur.execute("UPDATE users SET password_hash=%s WHERE id=%s",
                        (generate_password_hash(new_pw), session['user_id']))
            conn.commit()
            flash('Password changed successfully.', 'success')
        conn.close()
        return redirect(url_for('change_password'))
    conn = get_db()
    settings = get_settings(conn)
    conn.close()
    return render_template('change_password.html', settings=settings)


# ── USER MANAGEMENT (admin only) ──

@app.route('/users')
@admin_required
def users_list():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute("SELECT id, username, display_name, role, is_active, created_at FROM users ORDER BY id")
    users = cur.fetchall()
    conn.close()
    return render_template('users.html', users=users, settings=settings)


@app.route('/users/add', methods=['POST'])
@admin_required
def add_user():
    username = request.form.get('username', '').strip().lower()
    password = request.form.get('password', '')
    display_name = request.form.get('display_name', '').strip()
    role = request.form.get('role', 'viewer')
    if not username or not password:
        flash('Username and password are required.', 'danger')
        return redirect(url_for('users_list'))
    if len(password) < 6:
        flash('Password must be at least 6 characters.', 'danger')
        return redirect(url_for('users_list'))
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE username=%s", (username,))
    if cur.fetchone():
        flash('Username already exists.', 'danger')
        conn.close()
        return redirect(url_for('users_list'))
    cur.execute(
        "INSERT INTO users (username, password_hash, display_name, role) VALUES (%s,%s,%s,%s)",
        (username, generate_password_hash(password), display_name or username, role)
    )
    conn.commit()
    conn.close()
    flash(f'User "{username}" created.', 'success')
    return redirect(url_for('users_list'))


@app.route('/users/<int:user_id>/toggle', methods=['POST'])
@admin_required
def toggle_user(user_id):
    if user_id == session['user_id']:
        flash('You cannot deactivate yourself.', 'danger')
        return redirect(url_for('users_list'))
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE users SET is_active = NOT is_active WHERE id=%s", (user_id,))
    conn.commit()
    conn.close()
    flash('User status updated.', 'success')
    return redirect(url_for('users_list'))


@app.route('/users/<int:user_id>/reset-password', methods=['POST'])
@admin_required
def reset_user_password(user_id):
    new_pw = request.form.get('new_password', '')
    if len(new_pw) < 6:
        flash('Password must be at least 6 characters.', 'danger')
        return redirect(url_for('users_list'))
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE users SET password_hash=%s WHERE id=%s",
                (generate_password_hash(new_pw), user_id))
    conn.commit()
    conn.close()
    flash('Password reset successfully.', 'success')
    return redirect(url_for('users_list'))


@app.route('/users/<int:user_id>/delete', methods=['POST'])
@admin_required
def delete_user(user_id):
    if user_id == session['user_id']:
        flash('You cannot delete yourself.', 'danger')
        return redirect(url_for('users_list'))
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id=%s", (user_id,))
    conn.commit()
    conn.close()
    flash('User deleted.', 'success')
    return redirect(url_for('users_list'))


# ── ROUTES ──

@app.route('/')
@login_required
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
@login_required
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
@login_required
def add_renter():
    if request.method == 'POST':
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO renters (name, unit, monthly_rent, phone, email, co_leaser, co_leaser_email, co_leaser_phone) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (request.form['name'], request.form['unit'],
             float(request.form.get('monthly_rent', 0)),
             request.form.get('phone', ''), request.form.get('email', ''),
             request.form.get('co_leaser', ''), request.form.get('co_leaser_email', ''),
             request.form.get('co_leaser_phone', ''))
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
@login_required
def edit_renter(renter_id):
    conn = get_db()
    cur = conn.cursor()
    if request.method == 'POST':
        cur.execute(
            "UPDATE renters SET name=%s, unit=%s, monthly_rent=%s, phone=%s, email=%s, co_leaser=%s, co_leaser_email=%s, co_leaser_phone=%s WHERE id=%s",
            (request.form['name'], request.form['unit'],
             float(request.form.get('monthly_rent', 0)),
             request.form.get('phone', ''), request.form.get('email', ''),
             request.form.get('co_leaser', ''), request.form.get('co_leaser_email', ''),
             request.form.get('co_leaser_phone', ''), renter_id)
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


@app.route('/renters/<int:renter_id>/toggle-active', methods=['POST'])
@login_required
def toggle_renter_active(renter_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE renters SET is_active = NOT is_active WHERE id=%s", (renter_id,))
    conn.commit()
    conn.close()
    flash('Renter status updated.', 'success')
    return redirect(url_for('renters_list'))


@app.route('/renters/<int:renter_id>/delete', methods=['POST'])
@login_required
def delete_renter(renter_id):
    conn = get_db()
    cur = conn.cursor()
    # Delete child records in correct order before deleting renter
    cur.execute("DELETE FROM invoice_items WHERE invoice_id IN (SELECT id FROM invoices WHERE renter_id=%s)", (renter_id,))
    cur.execute("DELETE FROM invoices WHERE renter_id=%s", (renter_id,))
    cur.execute("DELETE FROM receipt_items WHERE receipt_id IN (SELECT id FROM receipts WHERE renter_id=%s)", (renter_id,))
    cur.execute("DELETE FROM receipts WHERE renter_id=%s", (renter_id,))
    cur.execute("DELETE FROM renters WHERE id=%s", (renter_id,))
    conn.commit()
    conn.close()
    flash('Renter deleted.', 'success')
    return redirect(url_for('renters_list'))


@app.route('/payments/<int:renter_id>', methods=['GET','POST'])
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
    if not invoice:
        conn.close()
        flash('Invoice not found.', 'danger')
        return redirect(url_for('invoices_list'))
    cur.execute("SELECT * FROM invoice_items WHERE invoice_id=%s ORDER BY id", (invoice_id,))
    items = cur.fetchall()
    subtotal = sum(float(i['qty'] or 1) * float(i['unit_price'] or 0) for i in items)
    today = date.today()
    days_overdue = 0
    if invoice['due_date']:
        try:
            due = datetime.strptime(invoice['due_date'], '%Y-%m-%d').date()
            days_overdue = (today - due).days
        except (ValueError, TypeError):
            pass

    # Get renter's credit balance
    cur.execute(
        "SELECT COALESCE(SUM(amount), 0) as total FROM credits WHERE renter_id=%s",
        (invoice['renter_id'],)
    )
    renter_credit_balance = float(cur.fetchone()['total'])

    # Get amount already paid toward this invoice's month
    month_name = ''
    period = (invoice['period'] or '').strip().lower()
    for m in MONTHS:
        if m.lower() in period:
            month_name = m
            break
    amount_paid = 0.0
    if month_name:
        month_num = MONTHS.index(month_name) + 1
        pay_year = settings['current_year']
        if invoice['invoice_date']:
            try:
                pay_year = int(invoice['invoice_date'].split('-')[0])
            except (IndexError, ValueError):
                pass
        cur.execute("SELECT amount_paid FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
                    (invoice['renter_id'], pay_year, month_num))
        pay = cur.fetchone()
        if pay:
            amount_paid = float(pay['amount_paid'] or 0)

    remaining_on_invoice = max(0, subtotal - amount_paid)

    conn.close()
    return render_template('invoice_view.html', invoice=invoice, items=items,
                           subtotal=subtotal, settings=settings,
                           days_overdue=days_overdue, today=today,
                           renter_credit_balance=renter_credit_balance,
                           amount_paid=amount_paid,
                           remaining_on_invoice=remaining_on_invoice)


@app.route('/invoices/<int:invoice_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_invoice(invoice_id):
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()

    cur.execute('''
        SELECT invoices.*, renters.name, renters.unit, renters.monthly_rent
        FROM invoices JOIN renters ON invoices.renter_id = renters.id
        WHERE invoices.id=%s
    ''', (invoice_id,))
    invoice = cur.fetchone()
    if not invoice:
        conn.close()
        flash('Invoice not found.', 'danger')
        return redirect(url_for('invoices_list'))

    if request.method == 'POST':
        cur.execute(
            "UPDATE invoices SET invoice_date=%s, due_date=%s, period=%s, notes=%s WHERE id=%s",
            (request.form.get('invoice_date', ''), request.form.get('due_date', ''),
             request.form.get('period', ''), request.form.get('notes', ''), invoice_id)
        )
        # Delete existing items and re-insert
        cur.execute("DELETE FROM invoice_items WHERE invoice_id=%s", (invoice_id,))
        i = 0
        while f'item_desc_{i}' in request.form:
            desc = request.form.get(f'item_desc_{i}', '').strip()
            qty = int(request.form.get(f'item_qty_{i}', 1))
            price = float(request.form.get(f'item_price_{i}', 0))
            if desc and price > 0:
                cur.execute(
                    "INSERT INTO invoice_items (invoice_id, description, qty, unit_price) VALUES (%s,%s,%s,%s)",
                    (invoice_id, desc, qty, price)
                )
            i += 1
        conn.commit()
        conn.close()
        flash('Invoice updated.', 'success')
        return redirect(url_for('view_invoice', invoice_id=invoice_id))

    cur.execute("SELECT * FROM invoice_items WHERE invoice_id=%s ORDER BY id", (invoice_id,))
    items = cur.fetchall()
    conn.close()
    return render_template('invoice_edit.html', invoice=invoice, items=items, settings=settings)


@app.route('/invoices/<int:invoice_id>/apply-credit', methods=['POST'])
@login_required
def apply_credit_to_invoice(invoice_id):
    conn = get_db()
    settings = get_settings(conn)
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

    renter_id = invoice['renter_id']
    apply_amount = float(request.form.get('credit_amount', 0))

    if apply_amount <= 0:
        conn.close()
        flash('Amount must be greater than zero.', 'danger')
        return redirect(url_for('view_invoice', invoice_id=invoice_id))

    # Check renter's available credit balance
    cur.execute("SELECT COALESCE(SUM(amount), 0) as total FROM credits WHERE renter_id=%s", (renter_id,))
    available_credit = float(cur.fetchone()['total'])

    if apply_amount > available_credit:
        conn.close()
        flash(f'Insufficient credit. Available: ${available_credit:,.2f}', 'danger')
        return redirect(url_for('view_invoice', invoice_id=invoice_id))

    # Get invoice total and amount paid
    cur.execute("SELECT COALESCE(SUM(qty * unit_price), 0) as total FROM invoice_items WHERE invoice_id=%s", (invoice_id,))
    invoice_total = float(cur.fetchone()['total'])

    # Determine month/year for payment
    period = (invoice['period'] or '').strip().lower()
    month_name = ''
    for m in MONTHS:
        if m.lower() in period:
            month_name = m
            break

    month_num = MONTHS.index(month_name) + 1 if month_name in MONTHS else None
    pay_year = settings['current_year']
    if invoice['invoice_date']:
        try:
            pay_year = int(invoice['invoice_date'].split('-')[0])
        except (IndexError, ValueError):
            pass

    # Get current amount paid
    amount_paid = 0.0
    if month_num:
        cur.execute("SELECT amount_paid, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
                    (renter_id, pay_year, month_num))
        pay = cur.fetchone()
        if pay:
            amount_paid = float(pay['amount_paid'] or 0)

    remaining = invoice_total - amount_paid
    if apply_amount > remaining:
        apply_amount = round(remaining, 2)

    if apply_amount <= 0:
        conn.close()
        flash('Invoice is already paid in full.', 'info')
        return redirect(url_for('view_invoice', invoice_id=invoice_id))

    # Deduct from credits (insert a negative credit entry)
    cur.execute(
        "INSERT INTO credits (renter_id, credit_date, amount, description, credit_type) VALUES (%s,%s,%s,%s,%s)",
        (renter_id, date.today().isoformat(), -apply_amount,
         f"Applied to Invoice {invoice['invoice_number']}", 'credit')
    )

    # Add to payments
    if month_num:
        cur.execute("SELECT id FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
                    (renter_id, pay_year, month_num))
        existing = cur.fetchone()
        new_paid = amount_paid + apply_amount
        if existing:
            cur.execute("UPDATE payments SET amount_paid=%s WHERE renter_id=%s AND year=%s AND month=%s",
                        (new_paid, renter_id, pay_year, month_num))
        else:
            cur.execute("INSERT INTO payments (renter_id, year, month, amount_paid, fees) VALUES (%s,%s,%s,%s,%s)",
                        (renter_id, pay_year, month_num, apply_amount, 0))

    conn.commit()
    conn.close()
    flash(f'${apply_amount:,.2f} credit applied to {invoice["invoice_number"]}. Remaining credit: ${available_credit - apply_amount:,.2f}', 'success')
    return redirect(url_for('view_invoice', invoice_id=invoice_id))


@app.route('/invoices/<int:invoice_id>/apply-late-fees', methods=['POST'])
@login_required
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
        # Get total EXCLUDING the $75 magistrate fee
        cur.execute(
            "SELECT COALESCE(SUM(qty * unit_price), 0) as total FROM invoice_items WHERE invoice_id=%s AND description NOT LIKE '%%Magistrate%%'",
            (invoice_id,)
        )
        total_ex_magistrate = float(cur.fetchone()['total'])
        day10_pct = round(total_ex_magistrate * 0.10, 2)
        cur.execute(
            "INSERT INTO invoice_items (invoice_id, description, qty, unit_price) VALUES (%s,%s,%s,%s)",
            (invoice_id, 'Late Fee – Day 10 (10%)', 1, day10_pct)
        )
        cur.execute("UPDATE invoices SET late_fee_day10_applied=TRUE WHERE id=%s", (invoice_id,))
        changes.append(f'Day 10: +${day10_pct:.2f} (10% of ${total_ex_magistrate:.2f}, excl. magistrate fee)')

    if changes:
        conn.commit()
        flash('Late fees applied: ' + '; '.join(changes), 'success')
    else:
        flash('No new late fees to apply (check that invoice is overdue and fees not already applied).', 'info')

    conn.close()
    return redirect(url_for('view_invoice', invoice_id=invoice_id))


# ── RECEIPTS ──

@app.route('/receipts')
@login_required
def receipts_list():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute('''
        SELECT receipts.*, renters.name as renter_name, renters.unit,
               renters.monthly_rent,
               COALESCE(SUM(CASE WHEN ri.amount > 0 THEN ri.amount ELSE 0 END), 0) as total,
               COALESCE(SUM(CASE WHEN ri.amount < 0 THEN ABS(ri.amount) ELSE 0 END), 0) as credit_total
        FROM receipts
        JOIN renters ON receipts.renter_id = renters.id
        LEFT JOIN receipt_items ri ON ri.receipt_id = receipts.id
        GROUP BY receipts.id, renters.name, renters.unit, renters.monthly_rent
        ORDER BY receipts.id DESC
    ''')
    receipts_raw = cur.fetchall()

    # Calculate remaining balance or credit for each receipt
    receipts_data = []
    for rec in receipts_raw:
        r = dict(rec)
        renter_id = rec['renter_id']
        month_name = rec['month'] or ''
        monthly_rent = float(rec['monthly_rent'] or 0)
        payment_total = float(rec['total'] or 0)
        credit_amt = float(rec['credit_total'] or 0)

        # Calculate total_due and remaining for this renter/month
        remaining = 0.0
        overpayment = 0.0
        if month_name in MONTHS:
            month_num = MONTHS.index(month_name) + 1
            pay_year = settings['current_year']
            if rec['payment_date']:
                try:
                    pay_year = int(rec['payment_date'].split('-')[0])
                except (IndexError, ValueError):
                    pass
            cur.execute("SELECT amount_paid, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
                        (renter_id, pay_year, month_num))
            pay = cur.fetchone()
            total_paid_month = float(pay['amount_paid']) if pay else 0
            fees = float(pay['fees']) if pay else 0

            # Non-rent charges
            cur.execute('''
                SELECT COALESCE(SUM(ri.amount), 0) as total
                FROM receipt_items ri JOIN receipts r2 ON ri.receipt_id = r2.id
                WHERE r2.renter_id = %s AND r2.month = %s AND r2.receipt_type = 'payment'
                  AND ri.amount > 0 AND LOWER(ri.description) NOT IN ('rent', 'monthly rent')
            ''', (renter_id, month_name))
            non_rent = float(cur.fetchone()['total'])

            total_due = monthly_rent + fees + non_rent
            balance = total_due - total_paid_month
            if balance > 0:
                remaining = balance
            elif balance < 0:
                overpayment = abs(balance)

        # Get renter's total credit balance
        cur.execute("SELECT COALESCE(SUM(amount), 0) as total FROM credits WHERE renter_id=%s", (renter_id,))
        renter_credit = float(cur.fetchone()['total'])

        r['remaining'] = remaining
        r['overpayment'] = overpayment
        r['credit_total'] = credit_amt
        r['renter_credit_balance'] = renter_credit
        receipts_data.append(r)

    conn.close()
    return render_template('receipts_list.html', receipts=receipts_data, settings=settings)


@app.route('/receipts/create', methods=['GET','POST'])
@login_required
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

        # Calculate overpayment / credit
        month_num = MONTHS.index(month) + 1 if month in MONTHS else None
        overpayment = 0.0
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

            # Add non-rent charges (keys, deposits, etc.) to total_due
            non_rent_this = sum(amt for desc, amt in line_items
                                if desc.lower() not in ('rent', 'monthly rent'))
            # Also get non-rent charges from OTHER receipts this month
            cur.execute('''
                SELECT COALESCE(SUM(ri.amount), 0) as total
                FROM receipt_items ri JOIN receipts r ON ri.receipt_id = r.id
                WHERE r.renter_id = %s AND r.month = %s AND r.receipt_type = 'payment'
                  AND ri.amount > 0 AND LOWER(ri.description) NOT IN ('rent', 'monthly rent')
            ''', (renter_id, month))
            non_rent_other = float(cur.fetchone()['total'])
            total_due += non_rent_this + non_rent_other

            remaining = total_due - already_paid
            if total_amount > remaining and remaining >= 0:
                overpayment = round(total_amount - remaining, 2)

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

        # Manual credit from the credit field
        manual_credit = float(request.form.get('credit_amount', 0))
        credit_desc = request.form.get('credit_description', '').strip()
        if manual_credit > 0:
            cur.execute(
                "INSERT INTO receipt_items (receipt_id, description, period, amount) VALUES (%s,%s,%s,%s)",
                (receipt_id, f'Credit: {credit_desc or "Applied Credit"}', month, -manual_credit)
            )
            cur.execute(
                "INSERT INTO credits (renter_id, credit_date, amount, description, credit_type) VALUES (%s,%s,%s,%s,%s)",
                (renter_id, pay_date or date.today().isoformat(), manual_credit,
                 f"{credit_desc or 'Credit'} (Receipt #{rec_num})", 'credit')
            )

        # If overpayment, add a credit line item on the receipt and create credit entry
        if overpayment > 0:
            cur.execute(
                "INSERT INTO receipt_items (receipt_id, description, period, amount) VALUES (%s,%s,%s,%s)",
                (receipt_id, f'Credit (Overpayment)', month, -overpayment)
            )
            cur.execute(
                "INSERT INTO credits (renter_id, credit_date, amount, description, credit_type) VALUES (%s,%s,%s,%s,%s)",
                (renter_id, pay_date or date.today().isoformat(), overpayment,
                 f"Overpayment on Receipt #{rec_num} ({month})", 'credit')
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
@login_required
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
    if not receipt:
        conn.close()
        flash('Receipt not found.', 'danger')
        return redirect(url_for('receipts_list'))
    cur.execute("SELECT * FROM receipt_items WHERE receipt_id=%s ORDER BY id", (receipt_id,))
    items = cur.fetchall()

    # Separate payment items from credits
    payment_total = sum(float(i['amount'] or 0) for i in items if float(i['amount'] or 0) > 0)
    credit_total = sum(abs(float(i['amount'] or 0)) for i in items if float(i['amount'] or 0) < 0)
    net_total = payment_total - credit_total

    monthly_rent = float(receipt['monthly_rent'] or 0)

    month_name = receipt['month'] or ''
    month_num = MONTHS.index(month_name) + 1 if month_name in MONTHS else None
    total_paid_month = 0.0
    fees_month = 0.0
    non_rent_charges = 0.0
    if month_num:
        pay_year = settings['current_year']
        if receipt['payment_date']:
            try:
                pay_year = int(receipt['payment_date'].split('-')[0])
            except (IndexError, ValueError):
                pass

        # Get total paid from payments table (accurate source of truth)
        cur.execute(
            "SELECT amount_paid, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
            (receipt['renter_id'], pay_year, month_num)
        )
        pay = cur.fetchone()
        if pay:
            total_paid_month = float(pay['amount_paid'] or 0)
            fees_month = float(pay['fees'] or 0)

        # Sum non-rent charges from ALL receipts this month (keys, deposits, etc.)
        cur.execute('''
            SELECT COALESCE(SUM(ri.amount), 0) as total
            FROM receipt_items ri
            JOIN receipts r ON ri.receipt_id = r.id
            WHERE r.renter_id = %s AND r.month = %s AND r.receipt_type = 'payment'
              AND ri.amount > 0
              AND LOWER(ri.description) NOT IN ('rent', 'monthly rent')
        ''', (receipt['renter_id'], month_name))
        non_rent_charges = float(cur.fetchone()['total'])

    # Get renter's total credit balance from credits table
    cur.execute(
        "SELECT COALESCE(SUM(amount), 0) as total FROM credits WHERE renter_id=%s",
        (receipt['renter_id'],)
    )
    renter_credit_balance = float(cur.fetchone()['total'])

    # Total due = monthly rent + late fees + non-rent charges (keys, deposits, etc.)
    total_due = monthly_rent + fees_month + non_rent_charges

    conn.close()
    return render_template('receipt_view.html', receipt=receipt, items=items,
                           payment_total=payment_total, credit_total=credit_total,
                           total_paid_month=total_paid_month,
                           total_due=total_due,
                           fees_month=fees_month, monthly_rent=monthly_rent,
                           renter_credit_balance=renter_credit_balance,
                           settings=settings)


@app.route('/receipts/<int:receipt_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_receipt(receipt_id):
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()

    cur.execute('''
        SELECT receipts.*, renters.name, renters.unit, renters.monthly_rent
        FROM receipts JOIN renters ON receipts.renter_id = renters.id
        WHERE receipts.id=%s
    ''', (receipt_id,))
    receipt = cur.fetchone()
    if not receipt:
        conn.close()
        flash('Receipt not found.', 'danger')
        return redirect(url_for('receipts_list'))

    if request.method == 'POST':
        renter_id = receipt['renter_id']

        # Capture old month so we can recalculate it too
        old_month_name = receipt['month'] or ''

        # ── Step 1: Update receipt header ──
        new_pay_date = request.form.get('payment_date', '')
        new_month = request.form.get('month', '')
        new_type = request.form.get('receipt_type', 'payment')

        cur.execute(
            "UPDATE receipts SET payment_date=%s, payment_method=%s, month=%s, invoice_ref=%s, receipt_type=%s WHERE id=%s",
            (new_pay_date, request.form.get('payment_method', ''),
             new_month, request.form.get('invoice_ref', ''), new_type, receipt_id)
        )

        # ── Step 2: Delete only charge items (positive amounts), keep existing credits ──
        cur.execute("DELETE FROM receipt_items WHERE receipt_id=%s AND amount > 0", (receipt_id,))
        i = 0
        while f'item_desc_{i}' in request.form:
            desc = request.form.get(f'item_desc_{i}', '').strip()
            period = request.form.get(f'item_period_{i}', '').strip()
            amt = float(request.form.get(f'item_amt_{i}', 0))
            if desc and amt != 0:
                cur.execute(
                    "INSERT INTO receipt_items (receipt_id, description, period, amount) VALUES (%s,%s,%s,%s)",
                    (receipt_id, desc, period, amt)
                )
            i += 1

        # ── Step 3: Handle credit from the credit field ──
        credit_amt = float(request.form.get('credit_amount', 0))
        credit_desc = request.form.get('credit_description', '').strip()
        if credit_amt > 0:
            cur.execute(
                "INSERT INTO receipt_items (receipt_id, description, period, amount) VALUES (%s,%s,%s,%s)",
                (receipt_id, f'Credit: {credit_desc or "Applied Credit"}', new_month, -credit_amt)
            )
            cur.execute(
                "INSERT INTO credits (renter_id, credit_date, amount, description, credit_type) VALUES (%s,%s,%s,%s,%s)",
                (renter_id, new_pay_date or date.today().isoformat(), credit_amt,
                 f"{credit_desc or 'Credit'} (Receipt #{receipt['receipt_number']})", 'credit')
            )

        # ── Step 3b: Calculate overpayment credit ──
        # Remove any old overpayment credit items from this receipt
        cur.execute("DELETE FROM receipt_items WHERE receipt_id=%s AND description LIKE 'Credit (Overpayment)%%'", (receipt_id,))
        # Remove old overpayment credit entries from credits table for this receipt
        cur.execute("DELETE FROM credits WHERE renter_id=%s AND description LIKE %s",
                    (renter_id, f"Overpayment on Receipt #{receipt['receipt_number']}%"))

        # Sum all positive items on this receipt
        cur.execute("SELECT COALESCE(SUM(amount), 0) as total FROM receipt_items WHERE receipt_id=%s AND amount > 0", (receipt_id,))
        new_total = float(cur.fetchone()['total'])

        # Get the renter's monthly rent
        cur.execute("SELECT monthly_rent FROM renters WHERE id=%s", (renter_id,))
        renter_row = cur.fetchone()
        monthly_rent = float(renter_row['monthly_rent']) if renter_row else 0

        # Get fees from payments table
        m_num_temp = MONTHS.index(new_month) + 1 if new_month in MONTHS else None
        fees_due = 0.0
        if m_num_temp:
            temp_year = settings['current_year']
            if new_pay_date:
                try:
                    temp_year = int(new_pay_date.split('-')[0])
                except (IndexError, ValueError):
                    pass
            cur.execute("SELECT fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s", (renter_id, temp_year, m_num_temp))
            fees_row = cur.fetchone()
            if fees_row:
                fees_due = float(fees_row['fees'] or 0)

        # Sum non-rent charges from ALL receipts this month (including this one)
        cur.execute('''
            SELECT COALESCE(SUM(ri.amount), 0) as total
            FROM receipt_items ri
            JOIN receipts r ON ri.receipt_id = r.id
            WHERE r.renter_id = %s AND r.month = %s AND r.receipt_type = 'payment'
              AND ri.amount > 0
              AND LOWER(ri.description) NOT IN ('rent', 'monthly rent')
        ''', (renter_id, new_month))
        non_rent_charges = float(cur.fetchone()['total'])

        # Total due = monthly rent + fees + non-rent charges (keys, deposits, etc.)
        total_due = monthly_rent + fees_due + non_rent_charges

        # Total paid across ALL receipts this month
        cur.execute('''
            SELECT COALESCE(SUM(ri.amount), 0) as total
            FROM receipt_items ri
            JOIN receipts r ON ri.receipt_id = r.id
            WHERE r.renter_id = %s AND r.month = %s AND r.receipt_type = 'payment'
              AND ri.amount > 0
        ''', (renter_id, new_month))
        total_all_paid = float(cur.fetchone()['total'])

        overpayment_amount = total_all_paid - total_due

        if overpayment_amount > 0 and new_type == 'payment' and new_month in MONTHS:
            overpayment = round(overpayment_amount, 2)
            if overpayment > 0:
                cur.execute(
                    "INSERT INTO receipt_items (receipt_id, description, period, amount) VALUES (%s,%s,%s,%s)",
                    (receipt_id, 'Credit (Overpayment)', new_month, -overpayment)
                )
                cur.execute(
                    "INSERT INTO credits (renter_id, credit_date, amount, description, credit_type) VALUES (%s,%s,%s,%s,%s)",
                    (renter_id, new_pay_date or date.today().isoformat(), overpayment,
                     f"Overpayment on Receipt #{receipt['receipt_number']} ({new_month})", 'credit')
                )

        # ── Step 4: Recalculate payments from ALL receipts for affected months ──
        # Collect all months that need recalculating
        months_to_recalc = set()
        if old_month_name in MONTHS:
            months_to_recalc.add(old_month_name)
        if new_month in MONTHS:
            months_to_recalc.add(new_month)

        for m_name in months_to_recalc:
            m_num = MONTHS.index(m_name) + 1
            # Sum ALL positive receipt_items for this renter/month across ALL payment receipts
            cur.execute('''
                SELECT COALESCE(SUM(ri.amount), 0) as total
                FROM receipt_items ri
                JOIN receipts r ON ri.receipt_id = r.id
                WHERE r.renter_id = %s
                  AND r.month = %s
                  AND r.receipt_type = 'payment'
                  AND ri.amount > 0
            ''', (renter_id, m_name))
            recalc_total = float(cur.fetchone()['total'])

            # Determine year from receipts for this month
            cur.execute('''
                SELECT payment_date FROM receipts
                WHERE renter_id = %s AND month = %s AND receipt_type = 'payment'
                  AND payment_date IS NOT NULL AND payment_date != ''
                ORDER BY id DESC LIMIT 1
            ''', (renter_id, m_name))
            date_row = cur.fetchone()
            pay_year = settings['current_year']
            if date_row and date_row['payment_date']:
                try:
                    pay_year = int(date_row['payment_date'].split('-')[0])
                except (IndexError, ValueError):
                    pass

            # Upsert the payments table with the recalculated total
            cur.execute(
                "SELECT id, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
                (renter_id, pay_year, m_num)
            )
            existing = cur.fetchone()
            if existing:
                cur.execute(
                    "UPDATE payments SET amount_paid=%s WHERE renter_id=%s AND year=%s AND month=%s",
                    (recalc_total, renter_id, pay_year, m_num)
                )
            else:
                cur.execute(
                    "INSERT INTO payments (renter_id, year, month, amount_paid, fees) VALUES (%s,%s,%s,%s,%s)",
                    (renter_id, pay_year, m_num, recalc_total, 0)
                )

        conn.commit()
        conn.close()
        flash('Receipt updated. All balances recalculated.', 'success')
        return redirect(url_for('view_receipt', receipt_id=receipt_id))

    cur.execute("SELECT * FROM receipt_items WHERE receipt_id=%s ORDER BY id", (receipt_id,))
    items = cur.fetchall()
    cur.execute("SELECT * FROM fee_schedule ORDER BY id")
    fee_types = cur.fetchall()
    conn.close()
    return render_template('receipt_edit.html', receipt=receipt, items=items,
                           settings=settings, months=MONTHS, fee_types=fee_types)


@app.route('/receipts/<int:receipt_id>/confirm-deposit', methods=['POST'])
@login_required
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
@login_required
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
@login_required
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
@login_required
def credits_list():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    renter_filter = request.args.get('renter_id', 0, type=int)
    type_filter = request.args.get('credit_type', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')

    query = '''
        SELECT credits.*, renters.name as renter_name, renters.unit
        FROM credits JOIN renters ON credits.renter_id = renters.id
        WHERE 1=1
    '''
    params = []

    if renter_filter:
        query += ' AND credits.renter_id = %s'
        params.append(renter_filter)
    if type_filter:
        query += ' AND credits.credit_type = %s'
        params.append(type_filter)
    if date_from:
        query += ' AND credits.credit_date >= %s'
        params.append(date_from)
    if date_to:
        query += ' AND credits.credit_date <= %s'
        params.append(date_to)

    query += ' ORDER BY credits.credit_date DESC, credits.id DESC'
    cur.execute(query, params)
    credits = cur.fetchall()
    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 ORDER BY name")
    renters = cur.fetchall()
    total_credits = sum(float(c['amount']) for c in credits)
    conn.close()
    return render_template('credits.html', credits=credits, renters=renters,
                           total_credits=total_credits, settings=settings,
                           today=date.today().isoformat(),
                           renter_filter=renter_filter, type_filter=type_filter,
                           date_from=date_from, date_to=date_to)


@app.route('/credits/add', methods=['POST'])
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
def statements_index():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 ORDER BY name")
    renters = cur.fetchall()
    conn.close()
    return render_template('statements_index.html', renters=renters, settings=settings)


@app.route('/statements/<int:renter_id>')
@login_required
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
@login_required
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


# ── REPORTS ──

@app.route('/reports/petty-cash')
@login_required
def petty_cash_report():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    year = request.args.get('year', date.today().year, type=int)
    month = request.args.get('month', date.today().month, type=int)

    start = f"{year}-{month:02d}-01"
    if month == 12:
        end = f"{year + 1}-01-01"
    else:
        end = f"{year}-{month + 1:02d}-01"

    cur.execute(
        "SELECT * FROM petty_cash WHERE transaction_date >= %s AND transaction_date < %s ORDER BY transaction_date, id",
        (start, end)
    )
    transactions = cur.fetchall()

    # Group by category
    categories = {}
    for t in transactions:
        cat = t['category'] or 'miscellaneous'
        if cat not in categories:
            categories[cat] = {'items': [], 'total_in': 0, 'total_out': 0}
        categories[cat]['items'].append(t)
        if t['transaction_type'] == 'in':
            categories[cat]['total_in'] += float(t['amount'])
        else:
            categories[cat]['total_out'] += float(t['amount'])

    total_in = sum(c['total_in'] for c in categories.values())
    total_out = sum(c['total_out'] for c in categories.values())

    conn.close()
    return render_template('petty_cash_report.html', categories=categories,
                           total_in=total_in, total_out=total_out,
                           balance=total_in - total_out,
                           month=month, year=year, settings=settings,
                           month_name=FULL_MONTHS[month - 1])


@app.route('/reports/petty-cash/pdf')
@login_required
def petty_cash_report_pdf():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    year = request.args.get('year', date.today().year, type=int)
    month = request.args.get('month', date.today().month, type=int)

    start = f"{year}-{month:02d}-01"
    if month == 12:
        end = f"{year + 1}-01-01"
    else:
        end = f"{year}-{month + 1:02d}-01"

    cur.execute(
        "SELECT * FROM petty_cash WHERE transaction_date >= %s AND transaction_date < %s ORDER BY transaction_date, id",
        (start, end)
    )
    transactions = cur.fetchall()

    categories = {}
    for t in transactions:
        cat = t['category'] or 'miscellaneous'
        if cat not in categories:
            categories[cat] = {'items': [], 'total_in': 0, 'total_out': 0}
        categories[cat]['items'].append(t)
        if t['transaction_type'] == 'in':
            categories[cat]['total_in'] += float(t['amount'])
        else:
            categories[cat]['total_out'] += float(t['amount'])

    total_in = sum(c['total_in'] for c in categories.values())
    total_out = sum(c['total_out'] for c in categories.values())
    conn.close()

    month_name = FULL_MONTHS[month - 1]
    return render_template('petty_cash_report_pdf.html', categories=categories,
                           total_in=total_in, total_out=total_out,
                           balance=total_in - total_out,
                           month=month, year=year, settings=settings,
                           month_name=month_name, today=date.today().isoformat())


@app.route('/reports/closing-statement')
@login_required
def closing_statement():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    year = request.args.get('year', date.today().year, type=int)
    month = request.args.get('month', date.today().month, type=int)
    period = f"{FULL_MONTHS[month - 1]} {year}"
    date_prefix = f"{year}-{month:02d}"

    # Rent collection per renter
    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 ORDER BY id")
    renters = cur.fetchall()

    renter_summary = []
    total_expected = 0
    total_collected = 0
    total_fees = 0

    for r in renters:
        cur.execute(
            "SELECT amount_paid, fees FROM payments WHERE renter_id=%s AND year=%s AND month=%s",
            (r['id'], year, month)
        )
        pay = cur.fetchone()
        paid = float(pay['amount_paid']) if pay else 0
        fees = float(pay['fees']) if pay else 0
        rent = float(r['monthly_rent'])
        balance = rent + fees - paid
        status = get_payment_status(r['monthly_rent'], paid, fees)
        total_expected += rent
        total_collected += paid
        total_fees += fees
        renter_summary.append({
            'name': r['name'], 'unit': r['unit'], 'rent': rent,
            'paid': paid, 'fees': fees, 'balance': balance, 'status': status,
            'co_leaser': r.get('co_leaser', ''), 'is_active': r.get('is_active', True)
        })

    total_outstanding = total_expected + total_fees - total_collected
    collection_rate = (total_collected / (total_expected + total_fees) * 100) if (total_expected + total_fees) > 0 else 0

    # Invoice totals
    cur.execute('''
        SELECT COUNT(*) as count, COALESCE(SUM(ii.qty * ii.unit_price), 0) as total
        FROM invoices
        LEFT JOIN invoice_items ii ON ii.invoice_id = invoices.id
        WHERE invoices.period = %s
    ''', (period,))
    inv_stats = cur.fetchone()

    # Receipt totals
    cur.execute('''
        SELECT COUNT(DISTINCT receipts.id) as count, COALESCE(SUM(ri.amount), 0) as total
        FROM receipts
        LEFT JOIN receipt_items ri ON ri.receipt_id = receipts.id
        WHERE receipts.payment_date LIKE %s
    ''', (f"{date_prefix}%",))
    rec_stats = cur.fetchone()

    # Deposits confirmed
    cur.execute('''
        SELECT COUNT(DISTINCT receipts.id) as count, COALESCE(SUM(ri.amount), 0) as total
        FROM receipts
        LEFT JOIN receipt_items ri ON ri.receipt_id = receipts.id
        WHERE receipts.deposit_confirmed = TRUE AND receipts.deposit_date LIKE %s
    ''', (f"{date_prefix}%",))
    dep_stats = cur.fetchone()

    # Credits
    cur.execute(
        "SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total FROM credits WHERE credit_date LIKE %s",
        (f"{date_prefix}%",)
    )
    credit_stats = cur.fetchone()

    # Petty cash
    start = f"{year}-{month:02d}-01"
    end = f"{year}-{month + 1:02d}-01" if month < 12 else f"{year + 1}-01-01"
    cur.execute(
        "SELECT transaction_type, COALESCE(SUM(amount), 0) as total FROM petty_cash WHERE transaction_date >= %s AND transaction_date < %s GROUP BY transaction_type",
        (start, end)
    )
    petty = {row['transaction_type']: float(row['total']) for row in cur.fetchall()}

    conn.close()
    return render_template('closing_statement.html',
                           renter_summary=renter_summary,
                           total_expected=total_expected, total_collected=total_collected,
                           total_fees=total_fees, total_outstanding=total_outstanding,
                           collection_rate=collection_rate,
                           inv_stats=inv_stats, rec_stats=rec_stats, dep_stats=dep_stats,
                           credit_stats=credit_stats,
                           petty_in=petty.get('in', 0), petty_out=petty.get('expense', 0),
                           month=month, year=year, period=period, settings=settings,
                           month_name=FULL_MONTHS[month - 1])


@app.route('/reports/account-statement')
@login_required
def account_statement_report():
    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()
    year = request.args.get('year', date.today().year, type=int)
    month = request.args.get('month', 0, type=int)

    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 ORDER BY name")
    renters = cur.fetchall()

    renter_statements = []
    for r in renters:
        date_prefix = f"{year}-{month:02d}" if month else f"{year}"

        # Invoices
        if month:
            cur.execute('''
                SELECT invoices.*, COALESCE(SUM(ii.qty * ii.unit_price), 0) as total
                FROM invoices LEFT JOIN invoice_items ii ON ii.invoice_id = invoices.id
                WHERE invoices.renter_id=%s AND invoices.invoice_date LIKE %s
                GROUP BY invoices.id ORDER BY invoices.invoice_date ASC
            ''', (r['id'], f"{date_prefix}%"))
        else:
            cur.execute('''
                SELECT invoices.*, COALESCE(SUM(ii.qty * ii.unit_price), 0) as total
                FROM invoices LEFT JOIN invoice_items ii ON ii.invoice_id = invoices.id
                WHERE invoices.renter_id=%s AND (invoices.invoice_date LIKE %s OR invoices.period LIKE %s)
                GROUP BY invoices.id ORDER BY invoices.invoice_date ASC
            ''', (r['id'], f"{year}%", f"%{year}%"))
        invoices = cur.fetchall()

        # Receipts
        cur.execute('''
            SELECT receipts.*, COALESCE(SUM(ri.amount), 0) as total
            FROM receipts LEFT JOIN receipt_items ri ON ri.receipt_id = receipts.id
            WHERE receipts.renter_id=%s AND receipts.payment_date LIKE %s
            GROUP BY receipts.id ORDER BY receipts.payment_date ASC
        ''', (r['id'], f"{date_prefix}%"))
        receipts = cur.fetchall()

        # Credits
        cur.execute(
            "SELECT * FROM credits WHERE renter_id=%s AND credit_date LIKE %s ORDER BY credit_date ASC",
            (r['id'], f"{date_prefix}%")
        )
        credits = cur.fetchall()

        total_charges = sum(float(inv['total']) for inv in invoices)
        total_payments = sum(float(rec['total']) for rec in receipts) + sum(float(cr['amount']) for cr in credits)
        balance = total_charges - total_payments

        renter_statements.append({
            'renter': r, 'invoices': len(invoices), 'receipts': len(receipts),
            'credits': len(credits), 'total_charges': total_charges,
            'total_payments': total_payments, 'balance': balance
        })

    conn.close()
    return render_template('account_statement_report.html',
                           renter_statements=renter_statements,
                           month=month, year=year, settings=settings,
                           full_months=FULL_MONTHS)


# ── SETTINGS ──

@app.route('/settings', methods=['GET','POST'])
@admin_required
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
@login_required
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
@login_required
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


# ── CRON JOBS ──

@app.route('/api/cron/generate-invoices', methods=['GET'])
def cron_generate_invoices():
    # Vercel sends the CRON_SECRET in the Authorization header
    secret = os.environ.get('CRON_SECRET', '')
    auth = request.headers.get('Authorization', '')
    if secret and auth != f'Bearer {secret}':
        return jsonify({'error': 'Unauthorized'}), 401

    today = date.today()
    month = today.month
    year = today.year

    conn = get_db()
    settings = get_settings(conn)
    cur = conn.cursor()

    period = f"{FULL_MONTHS[month-1]} {year}"
    invoice_date = f"{year}-{month:02d}-01"
    due_date = f"{year}-{month:02d}-05"

    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 AND is_active = TRUE ORDER BY id")
    renters = cur.fetchall()

    num = get_next_invoice_number(cur)
    created = 0
    skipped = 0

    for renter in renters:
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
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (inv_num, renter['id'], invoice_date, due_date, period,
             f"Payment due by {due_date}. A 10% late fee plus $75 magistrate fee applies on day 6. An additional 10% applies on day 10.",
             True, f"{year}-{month:02d}")
        )
        cur.execute(
            "SELECT id FROM invoices WHERE invoice_number=%s", (inv_num,)
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
    return jsonify({'status': 'ok', 'period': period, 'created': created, 'skipped': skipped})


@app.route('/api/cron/apply-late-fees', methods=['GET'])
def cron_apply_late_fees():
    secret = os.environ.get('CRON_SECRET', '')
    auth = request.headers.get('Authorization', '')
    if secret and auth != f'Bearer {secret}':
        return jsonify({'error': 'Unauthorized'}), 401

    today = date.today()
    month = today.month
    year = today.year
    period = f"{FULL_MONTHS[month-1]} {year}"

    conn = get_db()
    cur = conn.cursor()

    invoice_date = f"{year}-{month:02d}-01"
    due_date = f"{year}-{month:02d}-05"

    # Auto-create invoices for any active renters missing one this month
    cur.execute("SELECT * FROM renters WHERE monthly_rent > 0 AND is_active = TRUE ORDER BY id")
    all_renters = cur.fetchall()
    cur.execute(
        "SELECT renter_id FROM invoices WHERE auto_generated=TRUE AND period=%s",
        (period,)
    )
    existing_renter_ids = {row['renter_id'] for row in cur.fetchall()}

    num = get_next_invoice_number(cur)
    auto_created = 0
    for renter in all_renters:
        if renter['id'] in existing_renter_ids:
            continue
        inv_num = f"INV-{num:04d}"
        cur.execute(
            """INSERT INTO invoices
               (invoice_number, renter_id, invoice_date, due_date, period, notes,
                auto_generated, month_year)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (inv_num, renter['id'], invoice_date, due_date, period,
             f"Payment due by {due_date}. A 10% late fee plus $75 magistrate fee applies on day 6. An additional 10% applies on day 10.",
             True, f"{year}-{month:02d}")
        )
        cur.execute("SELECT id FROM invoices WHERE invoice_number=%s", (inv_num,))
        new_inv_id = cur.fetchone()['id']
        cur.execute(
            "INSERT INTO invoice_items (invoice_id, description, qty, unit_price) VALUES (%s,%s,%s,%s)",
            (new_inv_id, 'Monthly Rent', 1, float(renter['monthly_rent']))
        )
        num += 1
        auto_created += 1

    # Now get all auto-generated invoices for current month that still have fees to apply
    cur.execute('''
        SELECT invoices.*, renters.monthly_rent
        FROM invoices JOIN renters ON invoices.renter_id = renters.id
        WHERE invoices.auto_generated = TRUE
          AND invoices.period = %s
          AND (invoices.late_fee_day6_applied = FALSE OR invoices.late_fee_day10_applied = FALSE)
    ''', (period,))
    invoices = cur.fetchall()

    applied_day6 = 0
    applied_day10 = 0

    for invoice in invoices:
        try:
            due_date_parsed = datetime.strptime(invoice['due_date'], '%Y-%m-%d').date()
        except (ValueError, TypeError):
            continue

        days_overdue = (today - due_date_parsed).days
        invoice_id = invoice['id']

        # Get base items (excluding late fees and magistrate fees)
        cur.execute("SELECT * FROM invoice_items WHERE invoice_id=%s", (invoice_id,))
        items = cur.fetchall()
        late_tags = ['Late Fee', 'Magistrate Fee']
        base_items = [i for i in items if not any(tag in i['description'] for tag in late_tags)]
        base_total = sum(float(i['qty']) * float(i['unit_price']) for i in base_items)

        # Day 6: 10% of base rent + $75 magistrate fee
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
            applied_day6 += 1

        # Day 10: 10% of total EXCLUDING the $75 magistrate fee
        if days_overdue >= 5 and not invoice['late_fee_day10_applied']:
            cur.execute(
                "SELECT COALESCE(SUM(qty * unit_price), 0) as total FROM invoice_items WHERE invoice_id=%s AND description NOT LIKE '%%Magistrate%%'",
                (invoice_id,)
            )
            total_ex_magistrate = float(cur.fetchone()['total'])
            day10_pct = round(total_ex_magistrate * 0.10, 2)
            cur.execute(
                "INSERT INTO invoice_items (invoice_id, description, qty, unit_price) VALUES (%s,%s,%s,%s)",
                (invoice_id, 'Late Fee – Day 10 (10%)', 1, day10_pct)
            )
            cur.execute("UPDATE invoices SET late_fee_day10_applied=TRUE WHERE id=%s", (invoice_id,))
            applied_day10 += 1

    conn.commit()
    conn.close()
    return jsonify({
        'status': 'ok',
        'period': period,
        'day': today.day,
        'invoices_auto_created': auto_created,
        'day6_applied': applied_day6,
        'day10_applied': applied_day10,
        'invoices_checked': len(invoices)
    })


# Initialize database tables on first request
_db_initialized = False

@app.before_request
def ensure_db():
    global _db_initialized
    if not _db_initialized:
        init_db()
        _db_initialized = True
