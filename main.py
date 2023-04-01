import mysql.connector
import datetime
import smtplib
import details

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
  SELECT customer.id, CONCAT_WS(" ", customer.title, customer.first_name, customer.last_name) AS name customer.email, CONCAT_WS(", ", address.line_1, address.line_2, address.postcode, address.city) AS address, service.customer_id, MAX(service.service_date) AS last_service_date, (MAX(service.service_date) + INTERVAL 365 DAY) AS next_service_date, MAX(sent_email.created) AS last_sent_email 
  FROM service 
  LEFT JOIN sent_email ON service.customer_id = sent_email.customer_id 
  JOIN customer on customer.id = service.customer_id
  JOIN customer_address ON customer_address.customer_id = customer.id
  JOIN address ON address.id = customer_address.address_id
  GROUP by service.customer_id
  HAVING (CURDATE() BETWEEN next_service_date - INTERVAL 14 DAY AND next_service_date) AND (last_sent_email IS NULL OR last_sent_email NOT BETWEEN next_service_date - INTERVAL 14 DAY AND next_service_date)
  """
  mycursor.execute(sql)
  result = mycursor.fetchall()
  current_date = datetime.date.today()
  for row in result:
    customer_id = row[0]
    name = row[1]
    email = row[2]
    address = row[3]
    send_email()
    mycursor.execute(sent_email_sql, row[0])