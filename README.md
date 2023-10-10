# TRANSACTION PROCESSOR LAMBDA

This microservice is responsible for analyizing the transaction data from S3 files, storing the results in RDS and publishing the results to SQS.

- This microservice is intented to ran as a Lambda function.
- It is triggered by a S3 Event.
- A Makefile is provided to make deploy operations faster (when working locally).

## Flow Diagram

This microservice is part of a microservices architecture.

![Microservices Architecture](https://rick-vega-assets.s3.us-west-2.amazonaws.com/flow-diagram.jpg)

It works like this:

1. The CSV_Creator microservice generates csv on the fly and upload the files to a S3 bucket.
2. The S3 bucket triggers this Transaction_Processor microservice.
3. This microservice reads the CSV files from S3, process the data, store the results in RDS and publish the results to SQS.
4. The SQS queue triggers the Mail_Notificator microservice.
5. The Mail_Notificator microservice reads the SQS queue, prepares the template and send an email to the user.

## A CONCRETE EXAMPLE

1. Make a request to the API Gateway endpoint with a valid payload.

![API Gateway Request](https://rick-vega-assets.s3.us-west-2.amazonaws.com/request-example.jpg)

2. Receive a valid response.

![API Gateway Response](https://rick-vega-assets.s3.us-west-2.amazonaws.com/response-example.jpg)

3. Check database to see all transactions from the CSV file in `transactions` table.

![Database](https://rick-vega-assets.s3.us-west-2.amazonaws.com/transactions-table.jpg)

4. Check database to see the report saved in `reports` table.

![Database](https://rick-vega-assets.s3.us-west-2.amazonaws.com/reports-table.jpg)

5. Go to https://maildrop.cc/inbox/?mailbox=rick-vega-reports-inbox which is an email inbox for testing purposes and check report email.

NOTE: **Please allow some time for the email to arrive, as it is very restrictive with limits**.

![Email](https://rick-vega-assets.s3.us-west-2.amazonaws.com/sample-email.jpg)

## TRY IT YOURSELF

1. Make a request to the API Gateway endpoint with a valid payload.

Make a `POST` request to: `https://qo3djrx06i.execute-api.us-west-2.amazonaws.com/prod/csv`

2. Use this payload:

```json
{
    "amount": 1,
    "rows_min": 10,
    "rows_max": 15,
    "min_transaction_amount": -10000,
    "max_transaction_amount": 20000
  }
```

3. Connect to DATABASE and check the `transactions` and `reports` tables. **Credentials are available via email**.

4. Check the email inbox: [https://maildrop.cc/inbox/?mailbox=rick-vega-reports-inbox](https://maildrop.cc/inbox/?mailbox=rick-vega-reports-inbox). _Please allow some time for the email to arrive._ Maildrop is very restrictive with limits.


## DATABASE SCHEMA

The database schema is very simple for this scenario. Only two tables are needed: `transactions` and `reports`.

Here's the SQL code to create the tables:

```sql
create table if not exists transactions_report.reports
(
	id int auto_increment primary key,
	account_number varchar(64) not null,
	total_balance decimal(8,2) not null,
	average_debit_amount decimal(8,2) not null,
	average_credit_amount decimal(8,2) not null,
	monthly_transactions json not null,
	created_at datetime default CURRENT_TIMESTAMP not null
);

create table if not exists transactions_report.transactions
(
	id int auto_increment primary key,
	account_number varchar(64) not null,
	amount decimal(8,2) not null,
	transaction_id varchar(36) not null,
	date datetime not null,
	constraint transactions_transaction_id_uindex
		unique (transaction_id)
);

create index transactions_account_number_index
	on transactions_report.transactions (account_number);

create index transactions_transaction_id_index
	on transactions_report.transactions (transaction_id);
```

## THE OTHER MICROSERVICES

Please check the documentation for the other microservices:

- [CSV Creator](https://github.com/ricardovegamx/csv_creator)
- [Mail Notificator](https://github.com/ricardovegamx/mail_notificator)

## REQUIREMENTS

- Python 3.11
- AWS CLI
- The following environment variables must be set:
    - `AWS_ACCESS_KEY_ID`
    - `AWS_SECRET_ACCESS_KEY`
    - `AWS_DEFAULT_REGION`
    - `TRANSACTIONS_DB`
    - `EMAIL_NOTIFICATIONS_QUEUE_URL`

`EMAIL_NOTIFICATIONS_QUEUE_URL` must be a sqlalchemy format. For example:

```bash
TRANSACTIONS_DB=mysql+mysqlconnector://username:password@host:port/database
```

## DEPLOYMENT

1. Create a virtual file and install the dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

2. Create the Layer that the lambda will use:

```bash
make create-layer
```

NOTE: Please update your dependencies ARN after creating the layer in the `Makefile` file.

3. Create the lambda function:

```bash
make create-lambda
```

4. Upgrate the lambda function (whenever you make changes to the code):
   
```bash
make update-lambda
```

5. [OPTIONAL] Delete lambda function:
    
```bash 
make delete-lambda
```

## CODE STYLE

Use `make lint` to format the code style to sensible defaults.

## PERMISSIONS

The lambda will need the following permissions: S3 and SQS access.




