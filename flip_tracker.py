"""
Flip Tracker - Professional Real Estate Flip Investment Tracker
Replaces spreadsheet-based tracking with a full web application.
"""

from flask import Flask, Blueprint, render_template, request, jsonify
import json
import os
import math
from datetime import datetime, timedelta

flip_bp = Blueprint('flip', __name__)

# ---------------------------------------------------------------------------
# Data persistence (JSON file, with in-memory fallback for Railway)
# ---------------------------------------------------------------------------
DATA_FILE = os.path.join(os.path.dirname(__file__), 'flip_data.json')
_memory_store = None

def _default_data():
    return {
        'properties': [],
        'settings': {
            'default_commission_pct': 4.0,
            'default_closing_cost_pct': 1.5,
            'default_contingency_pct': 15.0,
            'partner_split_pct': 50.0,
        }
    }

def load_data():
    global _memory_store
    if _memory_store is not None:
        return _memory_store
    try:
        with open(DATA_FILE, 'r') as f:
            _memory_store = json.load(f)
            return _memory_store
    except (FileNotFoundError, json.JSONDecodeError):
        _memory_store = _default_data()
        return _memory_store

def save_data(data):
    global _memory_store
    _memory_store = data
    try:
        with open(DATA_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except OSError:
        pass  # Railway read-only FS — memory store is still updated

# ---------------------------------------------------------------------------
# Calculation engine
# ---------------------------------------------------------------------------
def calc_property_metrics(prop):
    """Calculate all derived metrics for a property."""
    purchase_price = prop.get('purchase_price', 0) or 0
    arv = prop.get('arv', 0) or 0
    sale_price = prop.get('sale_price', 0) or 0
    acq_closing_cost = prop.get('acq_closing_cost', 0) or 0
    sale_commission_pct = prop.get('sale_commission_pct', 4.0) or 4.0
    sale_closing_cost_pct = prop.get('sale_closing_cost_pct', 1.5) or 1.5
    contingency_pct = prop.get('contingency_pct', 15.0) or 15.0
    partner_split_pct = prop.get('partner_split_pct', 50.0) or 50.0
    sqft = prop.get('sqft', 0) or 0

    # Dates
    purchase_date = prop.get('purchase_date')
    sale_date = prop.get('sale_date')
    estimated_sale_date = prop.get('estimated_sale_date')
    listing_date = prop.get('listing_date')

    # Expenses
    expenses = prop.get('expenses', [])
    draws = prop.get('draws', [])
    mortgage_payments = prop.get('mortgage_payments', [])
    holding_costs = prop.get('holding_costs', {
        'monthly_mortgage': 0,
        'monthly_insurance': 0,
        'monthly_taxes': 0,
        'monthly_utilities': 0,
        'monthly_hoa': 0,
        'monthly_lawn': 0,
        'monthly_other': 0,
    })

    # ---- Rehab costs ----
    total_rehab = sum(e.get('amount', 0) for e in expenses)
    total_credits = sum(e.get('amount', 0) for e in expenses if e.get('is_credit'))
    net_rehab = total_rehab - (total_credits * 2)  # credits counted in total, subtract double

    # Simpler: labor vs materials vs other
    rehab_by_category = {}
    for e in expenses:
        if e.get('is_credit'):
            continue
        cat = e.get('category', 'Other')
        rehab_by_category[cat] = rehab_by_category.get(cat, 0) + e.get('amount', 0)

    # Budget tracking
    budget = prop.get('rehab_budget', 0) or 0
    budget_variance = ((total_rehab - budget) / budget * 100) if budget > 0 else 0
    budget_remaining = budget - total_rehab

    # Contingency
    contingency_amount = budget * (contingency_pct / 100) if budget > 0 else total_rehab * (contingency_pct / 100)

    # ---- Draws ----
    total_draws = sum(d.get('amount', 0) for d in draws)
    draw_credit = total_draws - total_rehab  # excess draws = less cash in deal

    # ---- Holding costs ----
    total_mortgage_payments = sum(m.get('amount', 0) for m in mortgage_payments)
    monthly_hold = sum([
        holding_costs.get('monthly_mortgage', 0),
        holding_costs.get('monthly_insurance', 0),
        holding_costs.get('monthly_taxes', 0),
        holding_costs.get('monthly_utilities', 0),
        holding_costs.get('monthly_hoa', 0),
        holding_costs.get('monthly_lawn', 0),
        holding_costs.get('monthly_other', 0),
    ])
    daily_hold = monthly_hold / 30 if monthly_hold > 0 else 0

    # Days held
    if purchase_date:
        pd = datetime.strptime(purchase_date, '%Y-%m-%d')
        end = datetime.strptime(sale_date, '%Y-%m-%d') if sale_date else datetime.now()
        days_held = (end - pd).days
        months_held = days_held / 30
    else:
        days_held = 0
        months_held = 0

    total_holding_cost = total_mortgage_payments + (monthly_hold - holding_costs.get('monthly_mortgage', 0)) * months_held
    if total_holding_cost == 0 and monthly_hold > 0:
        total_holding_cost = monthly_hold * months_held

    # ---- Sale costs ----
    effective_sale = sale_price if sale_price > 0 else arv
    sale_commission = effective_sale * (sale_commission_pct / 100)
    sale_closing = effective_sale * (sale_closing_cost_pct / 100)

    # ---- Total investment ----
    # OOP matches the spreadsheet: settlement + EMD + fees + mortgage payments
    purchase_settlement = prop.get('purchase_settlement', 0) or 0
    emd = prop.get('emd', 0) or 0
    appraisal_fee = prop.get('appraisal_fee', 0) or 0
    commitment_fee = prop.get('commitment_fee', 0) or 0
    if purchase_settlement > 0:
        total_cash_oop = purchase_settlement + emd + commitment_fee + appraisal_fee + total_holding_cost
    else:
        total_cash_oop = acq_closing_cost + total_rehab + total_holding_cost
    # Credit for draws = draws received minus rehab spent (excess draws reduce cash needed)
    draw_surplus = max(total_draws - total_rehab, 0)
    cash_in_deal = total_cash_oop - draw_surplus if draw_surplus > 0 else total_cash_oop

    # ---- Profit ----
    total_costs = purchase_price + acq_closing_cost + total_rehab + sale_commission + sale_closing + total_holding_cost
    gross_profit = effective_sale - total_costs
    profit_margin = (gross_profit / effective_sale * 100) if effective_sale > 0 else 0
    partner_share = gross_profit * (partner_split_pct / 100)

    # ---- ROI ----
    roi = (gross_profit / cash_in_deal * 100) if cash_in_deal > 0 else 0
    annualized_roi = (roi / (months_held / 12)) if months_held > 0 else 0
    cash_on_cash = (gross_profit / cash_in_deal * 100) if cash_in_deal > 0 else 0

    # ---- 70% Rule ----
    mao = (arv * 0.70) - total_rehab if arv > 0 else 0
    mao_with_holding = (arv * 0.70) - total_rehab - total_holding_cost if arv > 0 else 0
    passes_70_rule = purchase_price <= mao if mao > 0 else None
    total_cost_to_arv = (total_costs / arv * 100) if arv > 0 else 0

    # ---- Cost per sqft ----
    rehab_per_sqft = (total_rehab / sqft) if sqft > 0 else 0
    total_cost_per_sqft = (total_costs / sqft) if sqft > 0 else 0

    # ---- Days on market ----
    dom = 0
    if listing_date:
        ld = datetime.strptime(listing_date, '%Y-%m-%d')
        end_d = datetime.strptime(sale_date, '%Y-%m-%d') if sale_date else datetime.now()
        dom = (end_d - ld).days

    # ---- Holding cost burn ----
    profit_erosion_per_day = daily_hold
    days_until_zero_profit = int(gross_profit / daily_hold) if daily_hold > 0 else 999

    # ---- Status ----
    status = prop.get('status', 'active')
    if sale_date:
        status = 'sold'
    elif listing_date:
        status = 'listed'

    # ---- Risk flags ----
    flags = []
    if budget > 0 and budget_variance > 10:
        flags.append({'type': 'danger', 'msg': f'Budget overrun: {budget_variance:+.1f}%'})
    elif budget > 0 and budget_variance > 5:
        flags.append({'type': 'warning', 'msg': f'Budget variance: {budget_variance:+.1f}%'})
    if roi > 0 and roi < 15:
        flags.append({'type': 'warning', 'msg': f'ROI below 15%: {roi:.1f}%'})
    if roi > 0 and roi < 10:
        flags.append({'type': 'danger', 'msg': f'ROI critically low: {roi:.1f}%'})
    if gross_profit < 15000 and effective_sale > 0:
        flags.append({'type': 'danger', 'msg': f'Profit below $15K minimum floor'})
    if total_cost_to_arv > 85 and arv > 0:
        flags.append({'type': 'danger', 'msg': f'Total cost at {total_cost_to_arv:.0f}% of ARV (>85%)'})
    if passes_70_rule is False:
        flags.append({'type': 'warning', 'msg': f'Purchase exceeds 70% rule MAO by ${purchase_price - mao:,.0f}'})
    if contingency_pct < 10:
        flags.append({'type': 'warning', 'msg': 'Contingency below 10% — risky'})
    if dom > 60:
        flags.append({'type': 'warning', 'msg': f'{dom} days on market (>60)'})
    if days_held > 180:
        flags.append({'type': 'warning', 'msg': f'{days_held} days held (>180 benchmark)'})

    return {
        # Core
        'purchase_price': purchase_price,
        'arv': arv,
        'sale_price': sale_price,
        'effective_sale': effective_sale,
        'sqft': sqft,
        'status': status,
        # Rehab
        'total_rehab': total_rehab,
        'net_rehab': total_rehab,
        'rehab_by_category': rehab_by_category,
        'budget': budget,
        'budget_variance': budget_variance,
        'budget_remaining': budget_remaining,
        'contingency_pct': contingency_pct,
        'contingency_amount': contingency_amount,
        # Draws
        'total_draws': total_draws,
        'draw_credit': draw_credit,
        # Holding
        'total_mortgage_payments': total_mortgage_payments,
        'monthly_hold': monthly_hold,
        'daily_hold': daily_hold,
        'total_holding_cost': total_holding_cost,
        # Cash
        'acq_closing_cost': acq_closing_cost,
        'total_cash_oop': total_cash_oop,
        'cash_in_deal': cash_in_deal,
        # Sale
        'sale_commission': sale_commission,
        'sale_commission_pct': sale_commission_pct,
        'sale_closing': sale_closing,
        'sale_closing_cost_pct': sale_closing_cost_pct,
        # Profit
        'total_costs': total_costs,
        'gross_profit': gross_profit,
        'profit_margin': profit_margin,
        'partner_split_pct': partner_split_pct,
        'partner_share': partner_share,
        # ROI
        'roi': roi,
        'annualized_roi': annualized_roi,
        'cash_on_cash': cash_on_cash,
        # 70% Rule
        'mao': mao,
        'mao_with_holding': mao_with_holding,
        'passes_70_rule': passes_70_rule,
        'total_cost_to_arv': total_cost_to_arv,
        # Per sqft
        'rehab_per_sqft': rehab_per_sqft,
        'total_cost_per_sqft': total_cost_per_sqft,
        # Timeline
        'days_held': days_held,
        'months_held': months_held,
        'dom': dom,
        # Burn
        'profit_erosion_per_day': profit_erosion_per_day,
        'days_until_zero_profit': days_until_zero_profit,
        # Risk
        'flags': flags,
    }

# ---------------------------------------------------------------------------
# Seed the Willowbrook data
# ---------------------------------------------------------------------------
def seed_willowbrook():
    """Pre-load the 740 Willowbrook Rd data from the spreadsheet."""
    data = load_data()
    # Check if already exists
    for p in data['properties']:
        if 'willowbrook' in p.get('address', '').lower():
            return
    prop = {
        'id': 'willowbrook-740',
        'address': '740 Willowbrook Rd',
        'city': 'Chesapeake',
        'state': 'VA',
        'zip': '23320',
        'sqft': 0,
        'purchase_price': 430000,
        'arv': 645000,
        'sale_price': 0,
        'acq_closing_cost': 17782.57,
        'purchase_settlement': 61532.57,
        'emd': 10000,
        'appraisal_fee': 350,
        'commitment_fee': 999,
        'purchase_date': '2025-12-01',
        'estimated_sale_date': '2026-06-01',
        'sale_date': None,
        'listing_date': None,
        'rehab_budget': 52000,
        'sale_commission_pct': 4.0,
        'sale_closing_cost_pct': 1.5,
        'contingency_pct': 15.0,
        'partner_split_pct': 50.0,
        'status': 'renovation',
        'notes': 'Insurance paid at closing ($3,435.42) — partial reimbursement when we sell.',
        'holding_costs': {
            'monthly_mortgage': 2591.94,
            'monthly_insurance': 0,
            'monthly_taxes': 0,
            'monthly_utilities': 0,
            'monthly_hoa': 0,
            'monthly_lawn': 0,
            'monthly_other': 0,
        },
        'expenses': [
            {'date': '2026-01-15', 'vendor': 'Echols Plumbing', 'description': 'Draw 1 Paypal', 'amount': 3060, 'category': 'Labor - Plumbing', 'is_credit': False},
            {'date': '2026-01-22', 'vendor': 'Echols Plumbing', 'description': 'Draw 2 Paypal', 'amount': 1000, 'category': 'Labor - Plumbing', 'is_credit': False},
            {'date': '2026-02-01', 'vendor': 'Echols Plumbing', 'description': 'Draw 3 Paypal', 'amount': 3000, 'category': 'Labor - Plumbing', 'is_credit': False},
            {'date': '2026-01-20', 'vendor': 'Amazon', 'description': 'Building Materials', 'amount': 3107.57, 'category': 'Building Materials', 'is_credit': False},
            {'date': '2026-01-25', 'vendor': 'Lowes', 'description': 'Building Materials', 'amount': 2976, 'category': 'Building Materials', 'is_credit': False},
            {'date': '2026-02-01', 'vendor': 'Home Depot', 'description': 'Building Materials', 'amount': 3832.96, 'category': 'Building Materials', 'is_credit': False},
            {'date': '2026-01-28', 'vendor': 'Floor Trader', 'description': 'Flooring', 'amount': 2535.29, 'category': 'Flooring', 'is_credit': False},
            {'date': '2026-02-10', 'vendor': 'Echols Plumbing', 'description': 'Draw 4 Paypal', 'amount': 3500, 'category': 'Labor - Plumbing', 'is_credit': False},
            {'date': '2026-02-18', 'vendor': 'Echols Plumbing', 'description': 'Draw 5 Paypal', 'amount': 5000, 'category': 'Labor - Plumbing', 'is_credit': False},
            {'date': '2026-03-01', 'vendor': 'Echols Plumbing', 'description': 'Draw 6 Kitchen', 'amount': 7000, 'category': 'Labor - Kitchen', 'is_credit': False},
            {'date': '2026-03-10', 'vendor': 'Echols Plumbing', 'description': 'Draw 7 Kitchen Final', 'amount': 4180, 'category': 'Labor - Kitchen', 'is_credit': False},
            {'date': '2026-03-12', 'vendor': 'Virtual Tidewater', 'description': 'Marketing Pics', 'amount': 145, 'category': 'Marketing', 'is_credit': False},
            {'date': '2026-03-15', 'vendor': 'Echols Plumbing', 'description': 'Final Payment', 'amount': 5640, 'category': 'Labor - Plumbing', 'is_credit': False},
            {'date': '2026-03-05', 'vendor': 'I2G Source', 'description': 'Termite Repair', 'amount': 550, 'category': 'Repairs - Pest', 'is_credit': False},
            {'date': '2026-03-08', 'vendor': 'TJ Landscaping', 'description': 'Venmo', 'amount': 1000, 'category': 'Landscaping', 'is_credit': False},
        ],
        'draws': [
            {'date': '2026-01-20', 'description': 'Bank Draw 1', 'amount': 48400},
            {'date': '2026-02-15', 'description': 'Bank Draw 2', 'amount': 38650},
            {'date': '2026-03-10', 'description': 'Bank Draw Final', 'amount': 12050},
        ],
        'mortgage_payments': [
            {'date': '2026-01-07', 'amount': 2591.94},
            {'date': '2026-02-05', 'amount': 2591.94},
        ],
    }
    data['properties'].append(prop)
    save_data(data)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@flip_bp.route('/flips')
def flip_dashboard():
    return render_template('flip_tracker.html')

@flip_bp.route('/api/flips', methods=['GET'])
def get_flips():
    data = load_data()
    result = []
    for prop in data['properties']:
        metrics = calc_property_metrics(prop)
        result.append({**prop, 'metrics': metrics})
    return jsonify({'properties': result, 'settings': data.get('settings', {})})

@flip_bp.route('/api/flips', methods=['POST'])
def add_flip():
    data = load_data()
    prop = request.json
    if not prop.get('id'):
        prop['id'] = prop.get('address', 'property').lower().replace(' ', '-') + '-' + str(len(data['properties']))
    # Defaults
    prop.setdefault('expenses', [])
    prop.setdefault('draws', [])
    prop.setdefault('mortgage_payments', [])
    prop.setdefault('holding_costs', {
        'monthly_mortgage': 0, 'monthly_insurance': 0, 'monthly_taxes': 0,
        'monthly_utilities': 0, 'monthly_hoa': 0, 'monthly_lawn': 0, 'monthly_other': 0,
    })
    data['properties'].append(prop)
    save_data(data)
    metrics = calc_property_metrics(prop)
    return jsonify({**prop, 'metrics': metrics})

@flip_bp.route('/api/flips/<prop_id>', methods=['PUT'])
def update_flip(prop_id):
    data = load_data()
    for i, prop in enumerate(data['properties']):
        if prop.get('id') == prop_id:
            updates = request.json
            data['properties'][i].update(updates)
            save_data(data)
            metrics = calc_property_metrics(data['properties'][i])
            return jsonify({**data['properties'][i], 'metrics': metrics})
    return jsonify({'error': 'Not found'}), 404

@flip_bp.route('/api/flips/<prop_id>/expense', methods=['POST'])
def add_expense(prop_id):
    data = load_data()
    for prop in data['properties']:
        if prop.get('id') == prop_id:
            expense = request.json
            prop.setdefault('expenses', []).append(expense)
            save_data(data)
            metrics = calc_property_metrics(prop)
            return jsonify({**prop, 'metrics': metrics})
    return jsonify({'error': 'Not found'}), 404

@flip_bp.route('/api/flips/<prop_id>/draw', methods=['POST'])
def add_draw(prop_id):
    data = load_data()
    for prop in data['properties']:
        if prop.get('id') == prop_id:
            draw = request.json
            prop.setdefault('draws', []).append(draw)
            save_data(data)
            metrics = calc_property_metrics(prop)
            return jsonify({**prop, 'metrics': metrics})
    return jsonify({'error': 'Not found'}), 404

@flip_bp.route('/api/flips/<prop_id>/mortgage', methods=['POST'])
def add_mortgage(prop_id):
    data = load_data()
    for prop in data['properties']:
        if prop.get('id') == prop_id:
            payment = request.json
            prop.setdefault('mortgage_payments', []).append(payment)
            save_data(data)
            metrics = calc_property_metrics(prop)
            return jsonify({**prop, 'metrics': metrics})
    return jsonify({'error': 'Not found'}), 404

@flip_bp.route('/api/flips/<prop_id>', methods=['DELETE'])
def delete_flip(prop_id):
    data = load_data()
    data['properties'] = [p for p in data['properties'] if p.get('id') != prop_id]
    save_data(data)
    return jsonify({'ok': True})

@flip_bp.route('/api/flips/settings', methods=['GET'])
def get_flip_settings():
    data = load_data()
    return jsonify(data.get('settings', {}))

@flip_bp.route('/api/flips/settings', methods=['POST'])
def update_flip_settings():
    data = load_data()
    data['settings'] = request.json
    save_data(data)
    return jsonify(data['settings'])

@flip_bp.route('/api/flips/portfolio', methods=['GET'])
def portfolio_summary():
    """Portfolio-level metrics across all properties."""
    data = load_data()
    props = data['properties']
    if not props:
        return jsonify({})

    total_invested = 0
    total_profit = 0
    total_rehab = 0
    active_count = 0
    sold_count = 0
    all_flags = []

    for prop in props:
        m = calc_property_metrics(prop)
        total_invested += m['cash_in_deal']
        total_profit += m['gross_profit']
        total_rehab += m['total_rehab']
        all_flags.extend([{**f, 'property': prop.get('address', 'Unknown')} for f in m['flags']])
        if m['status'] == 'sold':
            sold_count += 1
        else:
            active_count += 1

    avg_roi = (total_profit / total_invested * 100) if total_invested > 0 else 0

    return jsonify({
        'total_properties': len(props),
        'active': active_count,
        'sold': sold_count,
        'total_invested': total_invested,
        'total_profit': total_profit,
        'total_rehab': total_rehab,
        'avg_roi': avg_roi,
        'all_flags': all_flags,
    })


# ---------------------------------------------------------------------------
# App initialization
# ---------------------------------------------------------------------------
def init_flip_tracker(main_app):
    """Register flip tracker blueprint on the main app."""
    main_app.register_blueprint(flip_bp)
    seed_willowbrook()


if __name__ == '__main__':
    app = Flask(__name__)
    app.register_blueprint(flip_bp)
    seed_willowbrook()
    app.run(debug=True, port=5002)
