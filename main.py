import mysql.connector
import datetime
import smtplib
import details
import schedule
import time
import csv
import os

from connect import mydb

mycursor = mydb.cursor(prepared=True)

email_subject = 'Book your upcoming boiler service!'
email_content = 'Book your upcoming boiler service with hothouse properties!'


def send_email(receiver, ):
  print('Start')
  smtp_server = smtplib.SMTP('smtp.office365.com', 587)
  print('SMTP Started')
  smtp_server.ehlo() # Hostname to send for this command defaults to the fully qualified domain name of the local host.
  smtp_server.starttls() #Puts connection to SMTP server in TLS mode
  smtp_server.ehlo()

  smtp_server.login(details.sender, details.password)
  print('Test')
  smtp_server.sendmail(details.sender, receiver, email_content)

day_notice = 14
sent_email_sql = 'INSERT INTO `sent_email` (customer_id) VALUES (%s)'
def main():
  sql = """ 
  SELECT customer.id, CONCAT_WS(" ", customer.title, customer.first_name, customer.last_name) AS name customer.email, CONCAT_WS(", ", address.line_1, address.line_2, address.postcode, address.city) AS address, MAX(service.service_date) AS last_service_date, (MAX(service.service_date) + INTERVAL 365 DAY) AS next_service_date, MAX(sent_email.created) AS last_sent_email 
  FROM service 
  LEFT JOIN sent_email ON service.customer_id = sent_email.customer_id 
  JOIN customer on customer.id = service.customer_id
  JOIN customer_address ON customer_address.customer_id = customer.id
  JOIN address ON address.id = customer_address.address_id
  GROUP by service.customer_id
  HAVING (CURDATE() BETWEEN next_service_date - INTERVAL %i DAY AND next_service_date) AND (last_sent_email IS NULL OR last_sent_email NOT BETWEEN next_service_date - INTERVAL %i DAY AND next_service_date)
  """
  values = (day_notice, day_notice)
  mycursor.execute(sql, values)
  result = mycursor.fetchall()
  current_date = datetime.date.today()
  for row in result:
    customer_id = row[0]
    name = row[1]
    email = row[2]
    address = row[3]
    previous_service = row[4]
    upcoming_service = row[5]
    last_email_sent = row[6]
    send_email(name, email, address, upcoming_service)
    mycursor.execute(sent_email_sql, customer_id)
    with open('email_logs.csv', 'a') as f:
      writer = csv.writer(f)
      time = datetime.utcnow().strftime('%Y-%m-%d, %H:%M:%S')
      writer.writerow([time, customer_id, previous_service, upcoming_service, last_email_sent])
  with open('running_logs.csv') as f:
    writer = csv.writer(f)
    time = datetime.utcnow().strftime('%Y-%m-%d, %H:%M:%S')
    writer.writerow([time, len(result)])


def runner():
  if not os.path.exists('running_logs.csv'):
    with open('running_logs.csv', 'w') as f:
      writer = csv.writer(f)
      writer.writerow(['DateTime (UTC)', 'Emails Sent'])

  if not os.path.exists('email_logs.csv'):
    with open('email_logs.csv', 'w') as f:
      writer = csv.writer(f)
      writer.writerow(['DateTime (UTC)', 'Customer ID', 'Previous Service (UTC)', 'Upcoming Service (UTC)', 'Last Email Sent (UTC)'])
    

  while True:
    schedule.run_pending()
    time.sleep(1)

schedule.every().day.at('09:00').do(main)
runner()