import psycopg2





def delete_square_links():
        DB_asp = {
        'dbname': 'asp_test',
        'user': 'asp_admin',
        'password': 'mGbn&bD4z#3e6d7D',
        'host': '34.41.166.241',
        'port': '5432'
    }
        




        try:
            conn = psycopg2.connect(**DB_asp)
            select = conn.cursor()
            sql_query = """
            SELECT 
                ChartNumber, 
                ApptID
            FROM 
                public.appointment_payment_links
            WHERE 
                (CURRENT_DATE - ApptDateTime::DATE) >= 5;
            """
            
            select.execute(sql_query)
            results = select.fetchall()

            matches_to_delete = []

            for row in results:
                chart_number, appt_id = row
                description = f"{chart_number}-{appt_id}"

                match_query = """
                SELECT 
                    link_id,
                    description
                FROM 
                    square_payment_links_sbx
                WHERE 
                    description = %s;
                """
                select.execute(match_query, (description,))
                match_results = select.fetchall()

                if match_results:
                                for match in match_results:
                                    link_id, matched_description = match  # Unpack link_id and description
                                    matches_to_delete.append(matched_description)
                                    print(f"Match found: Link ID: {link_id}, Chart Number: {chart_number}, Appointment ID: {appt_id} in description: {matched_description}")

            if matches_to_delete:
                confirm = input(f"if you want to delete ? (yyes/no): ").strip().lower()
                if confirm == 'yes':
                    for match in matches_to_delete:
                        delete_query = """
                        DELETE FROM 
                            square_payment_links_sbx
                        WHERE 
                            description = %s;
                        """
                        select.execute(delete_query, (match,))
                        print(f"Deleted: {match}")
                    conn.commit()
                else:
                    print("No entries were deleted.")
        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            if conn:
                select.close()
                conn.close()
if __name__ == '__main__':
    delete_square_links()
