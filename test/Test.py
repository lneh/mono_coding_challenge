import unittest
import sqlite3
import csv
import os

from src.Main import create_tables, insert_data, query_weddings_06_2024, query_weddings_two_weeks

class MyTestCase(unittest.TestCase):
    @staticmethod
    def write_csv(filename, data, fieldnames):
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow(row)

    def setUp(self):
        self.users_data = [
            {"user_id": "1", "user_name": "Jean Castex"},
            {"user_id": "2", "user_name": "Paul Donald"},
            {"user_id": "3", "user_name": "Edouard Laplace"}
        ]
        self.weddings_data = [
            {"user_id": "1", "wedding_date": "2024-06-10"},
            {"user_id": "2", "wedding_date": "2024-08-14"},
            {"user_id": "3", "wedding_date": "2024-06-30"}
        ]

        self.con = sqlite3.connect(":memory:")  # Use an in-memory database for testing
        self.cur = self.con.cursor()
        create_tables(self.cur)

        self.users_csv = "data/test_users.csv"
        self.weddings_csv = "data/test_weddings.csv"

        self.write_csv(self.users_csv, self.users_data, ['user_id', 'user_name'])
        self.write_csv(self.weddings_csv, self.weddings_data, ['user_id', 'wedding_date'])

        # Insert data into the database
        insert_data(self.cur, self.users_csv, self.weddings_csv)

    def test_create_tables(self):
        # Check if tables are created
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
        self.assertEqual(self.cur.fetchone()[0], 'users')
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weddings'")
        self.assertEqual(self.cur.fetchone()[0], 'weddings')

    def test_insert_data(self):
        self.cur.execute("SELECT * FROM users")
        users = self.cur.fetchall()
        self.assertEqual(len(users), 3)
        self.assertEqual(users[0], ("1", "Jean Castex"))
        self.assertEqual(users[1], ("2", "Paul Donald"))
        self.assertEqual(users[2], ("3", "Edouard Laplace"))

        self.cur.execute("SELECT * FROM weddings")
        weddings = self.cur.fetchall()
        self.assertEqual(len(weddings), 3)
        self.assertEqual(weddings[0], ("1", "2024-06-10"))
        self.assertEqual(weddings[1], ("2", "2024-08-14"))
        self.assertEqual(weddings[2], ("3", "2024-06-30"))

    def test_query_weddings_06_2024(self):
        output_file = 'data/weddings_06_2024.csv'
        query_weddings_06_2024(self.cur)

        self.assertTrue(os.path.exists(output_file))

        with open(output_file) as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]['user_name'], 'Jean Castex')
            self.assertEqual(rows[1]['user_name'], 'Edouard Laplace')

        os.remove(output_file)

    def test_query_weddings_two_weeks(self):
        output_file = 'data/weddings_two_weeks.csv'
        query_weddings_two_weeks(self.cur)

        with open(output_file) as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            # The test will need to be adjusted depending on the current date
            # Here we assume that the current date is before 2024-06-01 for the test
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['user_name'], 'Jean Castex')

        os.remove(output_file)

    def tearDown(self):
        self.con.close()
        os.remove(self.users_csv)
        os.remove(self.weddings_csv)


if __name__ == '__main__':
    unittest.main()
