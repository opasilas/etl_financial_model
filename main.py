import psycopg2
import os
import pandas as pd
import transform

author_mail = transform.data_transformation()


green_color = "\033[92m%s\033[0m"

DATABASE_NAME = os.environ.get('DATABASE_NAME')
DATABASE_USER = os.environ.get('DATABASE_USER')
DATABASE_PASSWORD = os.environ.get('DATABASE_PASSWORD')
HOST_NAME = os.environ.get('HOST_NAME')
PORT = os.environ.get('PORT')

print(green_color % "connecting to postgres database...")
conn = psycopg2.connect(
    dbname=DATABASE_NAME,
    user=DATABASE_USER,
    password=DATABASE_PASSWORD,
    host=HOST_NAME,
    port=PORT,
)

conn.set_session(autocommit=True)
cursor = conn.cursor()



def create_table():
    print(green_color % "connected")

    query = '''
        CREATE TABLE IF NOT EXISTS financial_model (
        id serial PRIMARY KEY,
	    Author_Name VARCHAR ( 100 ) NOT NULL,
	    Ticker_Name VARCHAR ( 100 ) NOT NULL,
	    Category VARCHAR ( 100 ) NOT NULL,
	    Line_Item VARCHAR ( 100 ) NOT NULL,
        Year VARCHAR NOT NULL,
        Period VARCHAR ( 10 ) NOT NULL,
        Estimate_Actual VARCHAR ( 20 ) NOT NULL,
        Value FLOAT NOT NULL
        );
'''

    cursor.execute(query)

    print(green_color % "Table created successfully")

def load_table_with_data(source_path: str, table: str):
    file = pd.read_excel(source_path)

    # conn.set_session(autocommit=True)
    # cursor = conn.cursor()

    for index, row in file.iterrows():

        query = """
            INSERT INTO {} (Author_Name, Ticker_Name, Category, Line_Item, Year,
            Period, Estimate_Actual, Value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """.format(table)

        try:
            cursor.execute(
                query,
                (row["Author_Name"], row["Ticker Name"], row["Category"], 
                row["Line Item"], row["Year "],row["Period"], row["Estimate/Actual"],
                row["Value "] )
            )
        except psycopg2.InterfaceError as e:
            print(e)
    
    sub = 'MODEL UPLOAD'
    bod = 'The model has been successfully uploaded. Thank you'
    
    print(green_color % "Table updated successfully")
    send_email('user_mail','pwd', author_mail, sub, bod)
    print(green_color % "Email has been sent to author")


        # cursor.close()
        # conn.close()

def update_table(table, updated_model_path):

    clear_table(table)
    load_table_with_data(updated_model_path, table)


def clear_table(table: str):
    query = '''
            TRUNCATE {} RESTART IDENTITY
    '''.format(table)

    cursor.execute(query)

    print(green_color % "All data in the table has been cleared\n")

def send_email(user, pwd, recipient, subject, body):
    import smtplib

    FROM = user
    TO = recipient if isinstance(recipient, list) else [recipient]
    SUBJECT = subject
    TEXT = body

    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, ", ".join(TO), SUBJECT, TEXT)

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(user, pwd)
        server.sendmail(FROM, TO, message)
        server.close()
        print ('successfully sent the mail')
    except:
        print ("failed to send mail")



if __name__ == "__main__":
    create_table()
    path = "out.csv"
    load_table_with_data(path, 'financial_model')

    # clear_table('financial_model')
    # send_email('opasilas1@gmail.com','cdkvycjaeiqeweiy', 'silasopawoye@gmail.com', t, b)
    # update_table('financial_model', path)