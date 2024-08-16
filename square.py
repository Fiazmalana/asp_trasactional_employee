import requests
import json
import psycopg2
from psycopg2 import sql

class PaymentLinksProcessor:
    # Constants
    SQAPI_VERSION = '2024-06-04'
    TOKEN = 'EAAAl1h-N4aXjMG8V8hk7l-24zzWLxBgENnYMLAoQdyt8veqB5L2MZ_mp-vkv_pF'
    REST_BASE_URL = 'https://connect.squareupsandbox.com/v2/'
    
    # Database constants
    DB_DATABASE = "asp_transactional"
    DB_USER = "asp_admin"
    DB_PASSWORD = "mGbn&bD4z#3e6d7D"
    DB_HOST = "34.41.166.241"  

    def __init__(self):
        self.headers = {
            'Authorization': f'Bearer {self.TOKEN}',
            'Square-Version': self.SQAPI_VERSION,
            'Content-Type': 'application/json'
        }
        # Establishing the database connection
        self.conn = psycopg2.connect(
            dbname=self.DB_DATABASE,
            user=self.DB_USER,
            password=self.DB_PASSWORD,
            host=self.DB_HOST
        )
        self.cursor = self.conn.cursor()

    def api_request(self, url, method='GET', data=None):
        try:
            if method == 'GET':
                response = requests.get(url, headers=self.headers)
            elif method == 'POST':
                response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error during API request to {url}: {e}")
            return {}

    def list_payment_links(self):
        url = f'{self.REST_BASE_URL}online-checkout/payment-links?limit=100'
        payment_links = []

        while url:
            result = self.api_request(url)
            payment_links.extend(result.get('payment_links', []))
            url = result.get('cursor') and f'{self.REST_BASE_URL}online-checkout/payment-links?limit=100&cursor={result["cursor"]}'
        
        return payment_links

    def get_order_by_id(self, order_id):
        url = f'{self.REST_BASE_URL}orders/{order_id}'
        return self.api_request(url)

    def extract_amount_details(self, order):
        def get_money_details(amount_money):
            return {
                'Amount': amount_money.get('amount', 0) / 100,
                'Currency': amount_money.get('currency', 'N/A')
            }

        details = {
            'Total_Money_Amount': 0,
            'Total_Money_Currency': 'N/A',
            'Net_Amount_Due_Amount': 0,
            'Net_Amount_Due_Currency': 'N/A',
        }

        if 'total_money' in order:
            total_money = order.get('total_money', {})
            details['Total_Money_Amount'], details['Total_Money_Currency'] = get_money_details(total_money).values()

        if 'net_amount_due_money' in order:
            net_amount_due_money = order.get('net_amount_due_money', {})
            details['Net_Amount_Due_Amount'], details['Net_Amount_Due_Currency'] = get_money_details(net_amount_due_money).values()

        return details

    def record_exists(self, link_id):
        self.cursor.execute(
            sql.SQL("SELECT 1 FROM asp_transactional.reporting_datastore.square_payment_links WHERE link_id = %s"),
            [link_id]
        )
        return self.cursor.fetchone() is not None

    def update_record(self, entry):
        update_query = sql.SQL("""
            UPDATE asp_transactional.reporting_datastore.square_payment_links
            SET created_date = %s, status = %s, description = %s, 
                total_money_amount = %s, total_money_currency = %s,
                net_amount_due_amount = %s, net_amount_due_currency = %s,
                created_at = %s, updated_at = %s, state = %s
            WHERE link_id = %s
        """)
        self.cursor.execute(update_query, (
            entry['Created_Date'],
            entry['Status'],
            entry['Description'],
            entry['Total_Money_Amount'],
            entry['Total_Money_Currency'],
            entry['Net_Amount_Due_Amount'],
            entry['Net_Amount_Due_Currency'],
            entry['Created_At'],
            entry['Updated_At'],
            entry['State'],
            entry['Link_ID']
        ))
        print(f" Updated record with Link_ID: {entry['Link_ID']}")

    def insert_record(self, entry):
        insert_query = sql.SQL("""
            INSERT INTO asp_transactional.reporting_datastore.square_payment_links (
                link_id, created_date, status, description, order_id, 
                total_money_amount, total_money_currency, 
                net_amount_due_amount, net_amount_due_currency, 
                created_at, updated_at, state
            ) VALUES (
                %(Link_ID)s, %(Created_Date)s, %(Status)s, %(Description)s, %(Order_ID)s, 
                %(Total_Money_Amount)s, %(Total_Money_Currency)s, 
                %(Net_Amount_Due_Amount)s, %(Net_Amount_Due_Currency)s, 
                %(Created_At)s, %(Updated_At)s, %(State)s
            )
        """)
        self.cursor.execute(insert_query, entry)
        print(f"Inserted new record with Link_ID: {entry['Link_ID']}")

    def retrieve_and_store_payment_links(self):
        payment_links = self.list_payment_links()
        data_to_write = []

        for link in payment_links:
            link_id = link.get('id', 'N/A')
            created_at = link.get('created_at', 'N/A')
            status = link.get('status', 'N/A')
            description = link.get('description', 'N/A')
            order_id = link.get('order_id', 'N/A')

            if order_id:
                order = self.get_order_by_id(order_id).get('order', {})
                amount_details = self.extract_amount_details(order)
                state = order.get('state', 'N/A')
                updated_at = order.get('updated_at', 'N/A')

                data_entry = {
                    'Link_ID': link_id,
                    'Created_Date': created_at,
                    'Status': status,
                    'Description': description,
                    'Order_ID': order_id,
                    'Total_Money_Amount': amount_details['Total_Money_Amount'],
                    'Total_Money_Currency': amount_details['Total_Money_Currency'],
                    'Net_Amount_Due_Amount': amount_details['Net_Amount_Due_Amount'],
                    'Net_Amount_Due_Currency': amount_details['Net_Amount_Due_Currency'],
                    'Created_At': created_at,
                    'Updated_At': updated_at,
                    'State': state
                }
            else:
                data_entry = {
                    'Link_ID': link_id,
                    'Created_Date': created_at,
                    'Status': 'N/A',
                    'Description': description,
                    'Order_ID': 'N/A',
                    'Total_Money_Amount': 'N/A',
                    'Total_Money_Currency': 'N/A',
                    'Net_Amount_Due_Amount': 'N/A',
                    'Net_Amount_Due_Currency': 'N/A',
                    'Created_At': created_at,
                    'Updated_At': 'N/A',
                    'State': 'N/A'
                }

            if self.record_exists(link_id):
                self.update_record(data_entry)
            else:
                self.insert_record(data_entry)

        self.conn.commit()
        print("All payment links details have been stored into database")

    def close(self):
        self.cursor.close()
        self.conn.close()

if __name__ == '__main__':
    processor = PaymentLinksProcessor()
    try:
        processor.retrieve_and_store_payment_links()
    finally:
        processor.close()
