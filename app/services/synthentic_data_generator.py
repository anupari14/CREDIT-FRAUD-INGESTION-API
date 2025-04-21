import random, hashlib, secrets
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sys
import os
import requests
import time

def write_dataframe_to_db_via_api(df, api_url, batch_size=10000):
    """
    Writes the rows of a DataFrame to the payment_msg table using the batch add_payment API.

    Args:
        df (pd.DataFrame): The DataFrame containing payment data.
        api_url (str): The base URL of the batch add_payment API endpoint.
        batch_size (int): Number of records to send in each batch.
    """
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size].to_dict(orient='records')  # Convert batch to list of dictionaries
        response = requests.post(api_url, json=batch)
        if response.status_code != 201:
            print(f"Failed to insert batch starting at index {i}")
            print(f"Error: {response.text}")
        else:
            print(f"Successfully inserted batch starting at index {i}")

def generate_base_entities(num_customers=100000, num_merchants=1000, multiple_device_ratio=0.1, scenario_ratio=0.01, twofa_ratio=0.3):
    """Generate base entities: customer profiles with cards & devices, and merchant list with MCCs."""
    faker = Faker()
    Faker.seed(0)  # Ensure reproducible fake data
    customers_info = {}
    next_device_id = 1
    # Select subsets of users for special scenarios:
    scenario_users = set(random.sample(range(1, num_customers+1), int(num_customers * scenario_ratio)))  # users targeted by ATO
    twofa_users = set(random.sample(range(1, num_customers+1), int(num_customers * twofa_ratio)))        # users with 2FA enabled
    # Credit card network mapping for gateway_provider field
    card_network_map = {'visa': 'Visa', 'mastercard': 'Mastercard', 'amex': 'AMEX', 'discover': 'Discover', 'jcb': 'JCB'}
    for user_id in range(1, num_customers+1):
        # Assign 1 (or occasionally 2) trusted devices to each user
        device_ids = [next_device_id]; next_device_id += 1
        if random.random() < multiple_device_ratio:
            device_ids.append(next_device_id); next_device_id += 1
        # If this user is in the scenario list, create an extra device as the attacker's device (not in device_ids)
        attacker_device_id = None
        if user_id in scenario_users:
            attacker_device_id = next_device_id; next_device_id += 1
        # Generate a credit card number and token (hashed) for the user
        card_type = random.choice(list(card_network_map.keys()))
        card_number = faker.credit_card_number(card_type=card_type)
        card_token = hashlib.sha256(card_number.encode('utf-8')).hexdigest()
        card_network = card_network_map[card_type]
        # Assign a home country (ISO country code) for contextualizing location anomalies
        country_code = faker.country_code()
        customers_info[user_id] = {
            'card_token': card_token,
            'devices': device_ids,
            'home_country': country_code,
            'card_network': card_network,
            'twofa_enabled': (user_id in twofa_users)
        }
        if attacker_device_id:
            customers_info[user_id]['attacker_device'] = attacker_device_id
    # Generate merchants with random MCC codes (10% high-risk categories, 90% normal)
    merchants = {}
    high_risk_mcc = [7995, 5967, 7273, 5912, 5122, 4214, 6211, 4829, 6051, 5966]  # high-risk MCCs (gambling, adult, pharma, etc.)
    normal_mcc    = [5411, 5812, 5813, 5300, 5331, 4511, 7011, 5941, 5999, 5200, 5311, 4111, 4812]      # common retail/travel MCCs
    for m_id in range(1, num_merchants+1):
        if random.random() < 0.1:
            mcc = random.choice(high_risk_mcc); category = 'high_risk'
        else:
            mcc = random.choice(normal_mcc); category = 'normal'
        m_country = faker.country_code()  # assign a country code for the merchant
        merchants[m_id] = {'mcc': mcc, 'country': m_country, 'category': category}
    return customers_info, merchants, scenario_users

def generate_auth_log(customers_info, scenario_users, start_date, end_date):
    """Generate authentication log entries, including normal login attempts and account takeover events."""
    faker = Faker()
    records = []
    auth_event_id = 1
    start_ts = int(start_date.timestamp()); end_ts = int(end_date.timestamp())
    compromised_times = {}  # to record the timestamp of successful attacker login for scenario users
    num_users = len(customers_info)
    # Simulate number of login sessions per user (Poisson distribution for variability)
    session_counts = np.random.poisson(12, size=num_users)  # ~12 sessions per user on average over 3 years
    session_counts = [max(1, int(x)) for x in session_counts]
    # Predefined cities for known countries to enrich location field
    known_cities = {
        'US': ['New York','Los Angeles','Chicago','Houston','Miami'],
        'GB': ['London','Manchester','Birmingham','Glasgow'],
        'CA': ['Toronto','Vancouver','Montreal','Calgary'],
        'AU': ['Sydney','Melbourne','Brisbane','Perth'],
        'IN': ['Mumbai','Delhi','Bangalore','Kolkata'],
        'DE': ['Berlin','Munich','Frankfurt','Hamburg'],
        'FR': ['Paris','Lyon','Marseille','Toulouse'],
        'CN': ['Beijing','Shanghai','Guangzhou','Shenzhen'],
        'JP': ['Tokyo','Osaka','Yokohama','Nagoya'],
        'BR': ['Sao Paulo','Rio de Janeiro','Brasilia'],
        'MX': ['Mexico City','Guadalajara','Monterrey'],
        'RU': ['Moscow','Saint Petersburg','Novosibirsk'],
        'ES': ['Madrid','Barcelona','Valencia'],
        'IT': ['Rome','Milan','Naples'],
        'ZA': ['Johannesburg','Cape Town','Durban'],
        'NG': ['Lagos','Abuja','Kano'],
        'KE': ['Nairobi','Mombasa'],
        'AE': ['Dubai','Abu Dhabi'],
        'NL': ['Amsterdam','Rotterdam','The Hague'],
        'SE': ['Stockholm','Gothenburg','Malmo']
    }
    for idx, user_id in enumerate(range(1, num_users+1)):
        info = customers_info[user_id]
        home_country = info['home_country']
        user_devices = info['devices']
        twofa_enabled = info['twofa_enabled']
        # Simulate each login session for this user
        for s in range(session_counts[idx]):
            # Randomly decide if this session has failures before success
            fail_count = 0
            if random.random() < 0.2:  # 20% chance of having login failures in a session
                fail_count = 1
                if random.random() < 0.1:  # 10% chance (of those) to have more than one failure
                    fail_count = random.randint(2, 4)  # could be 2 to 4 failed attempts
            # Choose a random timestamp for the successful login in this session
            success_ts = random.randint(start_ts, end_ts)
            success_time = datetime.fromtimestamp(success_ts)
            device_id = random.choice(user_devices)  # pick one of the user's known devices
            channel = random.choice(['Web', 'Mobile'])
            # Determine location string (use city if country is known for better realism)
            if home_country in known_cities:
                city = random.choice(known_cities[home_country])
                location = f"{city}, {home_country}"
            else:
                location = home_country
            # Occasionally simulate login from a different country (travel or VPN scenario)
            if random.random() < 0.05:
                alt_country = home_country
                while alt_country == home_country:
                    alt_country = faker.country_code()
                if alt_country in known_cities:
                    city = random.choice(known_cities[alt_country]); location = f"{city}, {alt_country}"
                else:
                    location = alt_country
            # If there are failures, log them just before the success_time
            otp_fail = False
            pass_fail_count = fail_count
            if twofa_enabled and fail_count > 1 and random.random() < 0.5:
                # If user has 2FA and multiple failures, treat the last failure as an OTP failure
                otp_fail = True
                pass_fail_count = fail_count - 1
            for attempt in range(1, pass_fail_count + 1):
                fail_time = success_time - timedelta(seconds=5 * (pass_fail_count - attempt + (1 if otp_fail else 0)))
                records.append({
                    'auth_event_id': auth_event_id,
                    'customer_id': user_id,
                    'device_id': device_id,
                    'timestamp': fail_time,
                    'auth_type': 'password',
                    'ip_address': faker.ipv4(),
                    'channel': channel,
                    'location': location,
                    'auth_status': 'failure',
                    'failure_reason': 'Wrong password',
                    'login_attempts': attempt
                })
                auth_event_id += 1
            if twofa_enabled and otp_fail:
                # Log a 2FA (OTP) failure as the last failure before success
                attempt_num = pass_fail_count + 1
                fail_time = success_time - timedelta(seconds=2)
                records.append({
                    'auth_event_id': auth_event_id,
                    'customer_id': user_id,
                    'device_id': device_id,
                    'timestamp': fail_time,
                    'auth_type': '2FA',
                    'ip_address': faker.ipv4(),
                    'channel': channel,
                    'location': location,
                    'auth_status': 'failure',
                    'failure_reason': 'OTP mismatch',
                    'login_attempts': attempt_num
                })
                auth_event_id += 1
            # Log the successful login event
            total_attempts = fail_count + 1
            records.append({
                'auth_event_id': auth_event_id,
                'customer_id': user_id,
                'device_id': device_id,
                'timestamp': success_time,
                'auth_type': '2FA' if twofa_enabled else 'password',
                'ip_address': faker.ipv4(),
                'channel': channel,
                'location': location,
                'auth_status': 'success',
                'failure_reason': '',
                'login_attempts': total_attempts
            })
            auth_event_id += 1
        # If this user is targeted for an account takeover scenario, simulate an attacker login session
        if user_id in scenario_users:
            comp_ts = random.randint(start_ts, end_ts)
            comp_time = datetime.fromtimestamp(comp_ts)
            attacker_device = info['attacker_device']
            # Pick an attacker location different from user's home country
            attacker_country = info['home_country']
            while attacker_country == info['home_country']:
                attacker_country = faker.country_code()
            if attacker_country in known_cities:
                city = random.choice(known_cities[attacker_country]); attacker_location = f"{city}, {attacker_country}"
            else:
                attacker_location = attacker_country
            channel = random.choice(['Web', 'Mobile'])
            # Attacker failures before success
            fail_count = random.randint(1, 4)  # assume attacker will have at least 1 failed attempt
            otp_fail = False
            pass_fail_count = fail_count
            if info['twofa_enabled'] and fail_count > 1 and random.random() < 0.5:
                otp_fail = True
                pass_fail_count = fail_count - 1
            for attempt in range(1, pass_fail_count + 1):
                fail_time = comp_time - timedelta(seconds=5 * (pass_fail_count - attempt + (1 if otp_fail else 0)))
                records.append({
                    'auth_event_id': auth_event_id,
                    'customer_id': user_id,
                    'device_id': attacker_device,
                    'timestamp': fail_time,
                    'auth_type': 'password',
                    'ip_address': faker.ipv4(),
                    'channel': channel,
                    'location': attacker_location,
                    'auth_status': 'failure',
                    'failure_reason': 'Wrong password',
                    'login_attempts': attempt
                })
                auth_event_id += 1
            if info['twofa_enabled'] and otp_fail:
                attempt_num = pass_fail_count + 1
                fail_time = comp_time - timedelta(seconds=2)
                records.append({
                    'auth_event_id': auth_event_id,
                    'customer_id': user_id,
                    'device_id': attacker_device,
                    'timestamp': fail_time,
                    'auth_type': '2FA',
                    'ip_address': faker.ipv4(),
                    'channel': channel,
                    'location': attacker_location,
                    'auth_status': 'failure',
                    'failure_reason': 'OTP mismatch',
                    'login_attempts': attempt_num
                })
                auth_event_id += 1
            # Successful attacker login (account takeover success)
            total_attempts = fail_count + 1
            records.append({
                'auth_event_id': auth_event_id,
                'customer_id': user_id,
                'device_id': attacker_device,
                'timestamp': comp_time,
                'auth_type': '2FA' if info['twofa_enabled'] else 'password',
                'ip_address': faker.ipv4(),
                'channel': channel,
                'location': attacker_location,
                'auth_status': 'success',
                'failure_reason': '',
                'login_attempts': total_attempts
            })
            auth_event_id += 1
            compromised_times[user_id] = comp_time  # record the time of compromise for use in transaction generation
    # Sort all events by time for realism
    records.sort(key=lambda x: x['timestamp'])
    df_auth = pd.DataFrame(records)
    return df_auth, compromised_times

def generate_payment_msgs(customers_info, merchants, scenario_users, compromised_times, start_date, end_date):
    """Generate payment transaction records with embedded fraud signals and anomalies."""
    faker = Faker()
    records = []
    message_id = 1
    start_ts = int(start_date.timestamp()); end_ts = int(end_date.timestamp())
    scenario_tx_ids = []  # list of message_ids for fraudulent transactions (for dispute referencing)
    num_users = len(customers_info)
    # Determine how many transactions each user makes (Poisson distribution around 10)
    trans_counts = np.random.poisson(10, size=num_users)
    trans_counts = [max(1, int(x)) for x in trans_counts]
    # Country-to-currency mapping for assigning transaction currency
    currency_map = {
        'US': 'USD', 'GB': 'GBP', 'CA': 'CAD', 'AU': 'AUD', 'SG': 'SGD', 'JP': 'JPY', 'CN': 'CNY', 'IN': 'INR',
        'DE': 'EUR', 'FR': 'EUR', 'IT': 'EUR', 'ES': 'EUR', 'NL': 'EUR', 'BE': 'EUR', 'IE': 'EUR', 'PT': 'EUR',
        'AT': 'EUR', 'FI': 'EUR', 'GR': 'EUR', 'BR': 'BRL', 'MX': 'MXN', 'RU': 'RUB', 'ZA': 'ZAR', 'AE': 'AED',
        'KE': 'KES', 'NG': 'NGN', 'SE': 'SEK', 'CH': 'CHF', 'HK': 'HKD'
    }
    for idx, user_id in enumerate(range(1, num_users+1)):
        info = customers_info[user_id]
        card_token = info['card_token']; user_country = info['home_country']
        for t in range(trans_counts[idx]):
            # Pick a merchant (favor lower IDs to simulate popular merchants)
            if random.random() < 0.8:
                merchant_id = random.randint(1, min(len(merchants), 200))
            else:
                merchant_id = random.randint(1, len(merchants))
            m_info = merchants[merchant_id]
            mcc = m_info['mcc']; merch_country = m_info['country']
            device_id = random.choice(info['devices'])
            channel = random.choice(['POS', 'Online', 'Mobile'])
            # Amount: 98% transactions are small (< $500), 2% are large (up to $10k)
            amount = round(random.uniform(1, 500), 2) if random.random() < 0.98 else round(random.uniform(500, 10000), 2)
            currency = currency_map.get(merch_country, random.choice(['USD', 'EUR', 'GBP', 'AUD']))
            trans_time = datetime.fromtimestamp(random.randint(start_ts, end_ts))
            ip_address = faker.ipv4()
            # Compute a risk score based on anomalies
            risk_score = 0
            if amount > 2000:
                risk_score += 30  # high amount
            if user_country != merch_country:
                risk_score += 20  # foreign transaction
            if m_info['category'] == 'high_risk':
                risk_score += 20  # high-risk industry merchant
            if device_id not in info['devices'] or device_id != info['devices'][0]:
                risk_score += 5   # using a secondary/new device
            risk_score = min(100, max(0, risk_score + random.randint(-5, 5)))  # add noise and clamp 0-100
            # Decide status: higher risk increases chance of decline
            base_decline_prob = 0.03
            if risk_score > 80:
                base_decline_prob = 0.2  # very risky -> 20% decline
            elif risk_score > 50:
                base_decline_prob = 0.1  # moderately risky -> 10% decline
            if random.random() < base_decline_prob:
                status = 'declined'
                response_code = '07' if risk_score > 80 else random.choice(['05', '51', '54', '65'])
                auth_code = ''  # no auth code on decline
            else:
                status = 'approved'
                response_code = '00'
                auth_code = str(random.randint(0, 999999)).zfill(6)
            iso_message_hex = secrets.token_hex(random.randint(8, 16))  # random hex string to simulate ISO8583 message content
            gateway_provider = info['card_network']
            records.append({
                'message_id': message_id,
                'card_number_token': card_token,
                'merchant_id': merchant_id,
                'device_id': device_id,
                'amount': amount,
                'currency': currency,
                'mcc': mcc,
                'channel': channel,
                'country': merch_country,
                'response_code': response_code,
                'auth_code': auth_code,
                'iso_message_hex': iso_message_hex,
                'gateway_provider': gateway_provider,
                'status': status,
                'risk_score': int(risk_score),
                'ip_address': ip_address,
                'timestamp': trans_time
            })
            message_id += 1
    # Inject additional fraudulent transactions post-ATO (each scenario user gets one fraud txn after compromise)
    for user_id, comp_time in compromised_times.items():
        info = customers_info[user_id]
        # Often choose a high-risk merchant for the fraudulent transaction
        if random.random() < 0.5:
            high_risk_merchants = [mid for mid, m in merchants.items() if m['category'] == 'high_risk']
            merchant_id = random.choice(high_risk_merchants) if high_risk_merchants else random.randint(1, len(merchants))
        else:
            merchant_id = random.randint(1, len(merchants))
        m_info = merchants[merchant_id]
        mcc = m_info['mcc']; merch_country = m_info['country']
        device_id = info['attacker_device']
        channel = random.choice(['Online', 'Mobile'])
        # Fraudulent amount tends to be larger on average
        amount = round(random.uniform(500, 10000), 2) if random.random() < 0.7 else round(random.uniform(1, 500), 2)
        currency = currency_map.get(merch_country, random.choice(['USD', 'EUR', 'GBP', 'AUD']))
        # Schedule this fraud transaction a short time after account takeover
        delay_seconds = random.randint(60, 86400)  # between 1 minute and 1 day later
        fraud_time = comp_time + timedelta(seconds=delay_seconds)
        if fraud_time > end_date:
            fraud_time = end_date
        ip_address = faker.ipv4()
        # High risk score (attacker device, possible foreign use, high amount, high-risk MCC)
        risk_score = 0
        if amount > 2000: risk_score += 30
        if customers_info[user_id]['home_country'] != merch_country: risk_score += 20
        if m_info['category'] == 'high_risk': risk_score += 20
        risk_score += 20  # using unknown device (attacker)
        risk_score = min(100, risk_score + random.randint(0, 5))
        status = 'approved'  # assume the fraudulent charge went through
        response_code = '00'; auth_code = str(random.randint(0, 999999)).zfill(6)
        iso_message_hex = secrets.token_hex(random.randint(8, 16))
        gateway_provider = info['card_network']
        records.append({
            'message_id': message_id,
            'card_number_token': info['card_token'],
            'merchant_id': merchant_id,
            'device_id': device_id,
            'amount': amount,
            'currency': currency,
            'mcc': mcc,
            'channel': channel,
            'country': merch_country,
            'response_code': response_code,
            'auth_code': auth_code,
            'iso_message_hex': iso_message_hex,
            'gateway_provider': gateway_provider,
            'status': status,
            'risk_score': int(risk_score),
            'ip_address': ip_address,
            'timestamp': fraud_time
        })
        scenario_tx_ids.append(message_id)
        message_id += 1
    df_payment = pd.DataFrame(records)
    return df_payment, scenario_tx_ids

def generate_dispute_msgs(df_payment, scenario_tx_ids, customers_info, start_date, end_date):
    """Generate dispute/chargeback records for fraudulent transactions and friendly fraud cases."""
    records = []
    dispute_id = 1
    end_dt = end_date
    token_to_user = {info['card_token']: uid for uid, info in customers_info.items()}  # map card token back to customer
    df_indexed = df_payment.set_index('message_id')
    # Disputes for known fraud transactions (from ATO scenarios)
    for tx_id in scenario_tx_ids:
        if tx_id not in df_indexed.index:
            continue
        trans = df_indexed.loc[tx_id]
        cust_id = token_to_user.get(trans['card_number_token'])
        merchant_id = trans['merchant_id']; amount = trans['amount']; currency = trans['currency']
        tx_time = trans['timestamp']
        # Randomly classify some as triangulation fraud disputes
        reason_code = 'FRAUD' if random.random() < 0.5 else 'TRIANGULATION'
        # Most fraud disputes start as chargebacks; some go to arbitration
        stage = 'Arbitration' if random.random() < 0.3 else 'Chargeback Initiated'
        status = 'Closed'
        evidence = 'Yes' if random.random() < 0.7 else 'No'  # assume merchant often provides evidence in fraud cases
        # File the dispute a few days/weeks after the transaction
        delay_days = random.randint(1, 90)
        dispute_time = tx_time + timedelta(days=delay_days)
        if dispute_time > end_dt:
            dispute_time = end_dt
            status = 'Open'
            stage = 'Pre-Arbitration'
            evidence = 'No'
        resolution_time = ''
        if status == 'Closed':
            res_time = dispute_time + timedelta(days=random.randint(1, 60))
            if res_time > end_dt:
                res_time = end_dt
            resolution_time = res_time
        records.append({
            'dispute_id': dispute_id,
            'transaction_id': tx_id,
            'customer_id': cust_id,
            'merchant_id': merchant_id,
            'amount': amount,
            'currency': currency,
            'timestamp': dispute_time,
            'dispute_reason_code': reason_code,
            'dispute_stage': stage,
            'status': status,
            'evidence_provided': evidence,
            'resolution_timestamp': resolution_time
        })
        dispute_id += 1
    # Disputes for friendly fraud (legitimate charges that were disputed)
    approved_ids = df_payment[df_payment['status'] == 'approved']['message_id'].tolist()
    eligible_ids = [tid for tid in approved_ids if tid not in scenario_tx_ids]
    sample_size = min(len(eligible_ids), int(0.002 * len(df_payment)))  # ~0.2% of transactions turn into friendly fraud disputes
    friendly_ids = random.sample(eligible_ids, sample_size) if sample_size > 0 else []
    for tx_id in friendly_ids:
        trans = df_indexed.loc[tx_id]
        cust_id = token_to_user.get(trans['card_number_token'])
        merchant_id = trans['merchant_id']; amount = trans['amount']; currency = trans['currency']
        tx_time = trans['timestamp']
        reason_code = 'FRIENDLY_FRAUD'
        stage = 'Chargeback Initiated'
        # Most friendly fraud disputes are closed in favor of cardholder (they get refund)
        status = 'Closed' if random.random() < 0.9 else 'Open'
        evidence = 'No'  # merchant typically has no evidence for a false claim by customer
        dispute_time = tx_time + timedelta(days=random.randint(5, 60))
        if dispute_time > end_dt:
            dispute_time = end_dt
            status = 'Open'
            stage = 'Investigation'
        resolution_time = ''
        if status == 'Closed':
            res_time = dispute_time + timedelta(days=random.randint(1, 30))
            if res_time > end_dt:
                res_time = end_dt
            resolution_time = res_time
        records.append({
            'dispute_id': dispute_id,
            'transaction_id': tx_id,
            'customer_id': cust_id,
            'merchant_id': merchant_id,
            'amount': amount,
            'currency': currency,
            'timestamp': dispute_time,
            'dispute_reason_code': reason_code,
            'dispute_stage': stage,
            'status': status,
            'evidence_provided': evidence,
            'resolution_timestamp': resolution_time
        })
        dispute_id += 1
    df_dispute = pd.DataFrame(records)
    df_dispute.sort_values('timestamp', inplace=True)
    return df_dispute

def generate_kyc_msgs(customers_info, start_date, end_date, duplicate_pairs=100, duplicate_triples=10, fail_count=500):
    """Generate KYC (Know-Your-Customer) verification events, including some failures and synthetic identity cases."""
    faker = Faker()
    records = []
    kyc_event_id = 1
    start_ts = int(start_date.timestamp()); end_ts = int(end_date.timestamp())
    user_ids = list(customers_info.keys())
    # Assign a random document type and number for each user
    docs = {}
    for user_id in user_ids:
        home_country = customers_info[user_id]['home_country']
        # Bias towards driver's license for some countries, otherwise passport or national ID
        if home_country in ['US','GB','CA','AU']:
            doc_type = random.choices(['Driver License', 'Passport'], weights=[60,40], k=1)[0]
        else:
            doc_type = random.choices(['Passport', 'National ID', 'Driver License'], weights=[50,30,20], k=1)[0]
        # Generate a fake document number pattern based on type
        if doc_type == 'Passport':
            doc_num = faker.bothify('??#######')   # e.g. AB1234567
        elif doc_type == 'Driver License':
            doc_num = faker.bothify('?########')    # e.g. A12345678
        else:  # National ID
            doc_num = faker.numerify('#########')   # 9-digit ID
        docs[user_id] = {'type': doc_type, 'number': doc_num}
    # Introduce duplicates to simulate synthetic identities (pairs and triples of users sharing the same ID)
    dup_users = set()
    if duplicate_pairs * 2 + duplicate_triples * 3 <= len(user_ids):
        chosen = random.sample(user_ids, duplicate_pairs*2 + duplicate_triples*3)
        random.shuffle(chosen)
        pair_list = chosen[:duplicate_pairs*2]
        triple_list = chosen[duplicate_pairs*2:]
        # Assign pairs of users the same document
        for i in range(0, len(pair_list), 2):
            u1, u2 = pair_list[i], pair_list[i+1]
            dup_users.update([u1, u2])
            docs[u2]['type'] = docs[u1]['type']
            docs[u2]['number'] = docs[u1]['number']
        # Assign triples of users the same document
        for j in range(0, len(triple_list), 3):
            if j+2 < len(triple_list):
                u1, u2, u3 = triple_list[j], triple_list[j+1], triple_list[j+2]
                dup_users.update([u1, u2, u3])
                docs[u2]['type'] = docs[u1]['type']
                docs[u2]['number'] = docs[u1]['number']
                docs[u3]['type'] = docs[u1]['type']
                docs[u3]['number'] = docs[u1]['number']
    # Choose a set of users who will have an initial KYC failure (excluding those already in duplicate sets for simplicity)
    fail_candidates = [uid for uid in user_ids if uid not in dup_users]
    fail_users = set(random.sample(fail_candidates, min(fail_count, len(fail_candidates))))
    # Generate KYC events for each user
    for user_id in user_ids:
        primary_device = customers_info[user_id]['devices'][0]
        doc_type = docs[user_id]['type']
        doc_number = docs[user_id]['number']
        doc_hash = hashlib.sha256(doc_number.encode('utf-8')).hexdigest()
        if user_id in fail_users:
            # Create two events: one failed verification, then a successful retry
            fail_ts = random.randint(start_ts, end_ts)
            fail_time = datetime.fromtimestamp(fail_ts)
            # Retry after 1–14 days
            success_delay = random.randint(1, 14)
            success_time = fail_time + timedelta(days=success_delay)
            if success_time > end_date:
                success_time = end_date
            # Face match scores: low for failure, high for success
            fail_score = random.randint(0, 60)
            success_score = random.randint(80, 100)
            ip1 = faker.ipv4(); ip2 = faker.ipv4()
            geo1 = f"{faker.latitude():.4f},{faker.longitude():.4f}"
            geo2 = f"{faker.latitude():.4f},{faker.longitude():.4f}"
            records.append({
                'kyc_event_id': kyc_event_id,
                'customer_id': user_id,
                'device_id': primary_device,
                'timestamp': fail_time,
                'kyc_type': 'Onboarding',
                'document_type': doc_type,
                'document_number_hash': doc_hash,
                'face_match_score': fail_score,
                'verification_status': 'failed',
                'geo_location': geo1,
                'ip_address': ip1
            })
            kyc_event_id += 1
            records.append({
                'kyc_event_id': kyc_event_id,
                'customer_id': user_id,
                'device_id': primary_device,
                'timestamp': success_time,
                'kyc_type': 'Onboarding Retry',
                'document_type': doc_type,
                'document_number_hash': doc_hash,
                'face_match_score': success_score,
                'verification_status': 'verified',
                'geo_location': geo2,
                'ip_address': ip2
            })
            kyc_event_id += 1
        else:
            # Single successful verification event
            ts = random.randint(start_ts, end_ts)
            event_time = datetime.fromtimestamp(ts)
            score = random.randint(80, 100)
            ip_addr = faker.ipv4()
            geo_loc = f"{faker.latitude():.4f},{faker.longitude():.4f}"
            records.append({
                'kyc_event_id': kyc_event_id,
                'customer_id': user_id,
                'device_id': primary_device,
                'timestamp': event_time,
                'kyc_type': 'Onboarding',
                'document_type': doc_type,
                'document_number_hash': doc_hash,
                'face_match_score': score,
                'verification_status': 'verified',
                'geo_location': geo_loc,
                'ip_address': ip_addr
            })
            kyc_event_id += 1
    df_kyc = pd.DataFrame(records)
    df_kyc.sort_values('timestamp', inplace=True)
    return df_kyc

# Main execution to generate all datasets and save to CSV
if __name__ == "__main__":
    # Define simulation period (3 years)
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2024, 12, 31)

    # Generate base data
    customers_info, merchants, scenario_users = generate_base_entities()

    # Generate detailed logs for each table
    df_auth, compromised_times = generate_auth_log(customers_info, scenario_users, start_date, end_date)
    df_payment, scenario_tx_ids = generate_payment_msgs(customers_info, merchants, scenario_users, compromised_times, start_date, end_date)
    df_dispute = generate_dispute_msgs(df_payment, scenario_tx_ids, customers_info, start_date, end_date)
    df_kyc = generate_kyc_msgs(customers_info, start_date, end_date)

    # Drop the internal timestamp from payments (not part of schema output)
    df_payment.drop(columns=['timestamp'], inplace=True)
    # Save each table to CSV
    # df_payment.to_csv('payment_msgs_raw.csv', index=False)
    write_dataframe_to_db_via_api(df_payment, 'http://ec2-3-25-114-250.ap-southeast-2.compute.amazonaws.com:8000/api/payment/batch', batch_size=100)
    df_auth.to_csv('auth_log_msgs_raw.csv', index=False)
    df_dispute.to_csv('dispute_msgs_raw.csv', index=False)
    df_kyc.to_csv('kyc_msgs_raw.csv', index=False)

    print("Synthetic data generation completed. CSV files saved.")
