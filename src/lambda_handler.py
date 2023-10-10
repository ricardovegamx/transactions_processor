import csv
import json
import logging
import os
import statistics
import sys
from datetime import datetime

import boto3
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


engine = create_engine(os.getenv("TRANSACTIONS_DB"))


def lambda_handler(event, context):
    logger.info(f"event received: {event}")
    bucket, key = get_s3_bucket_key(event)

    return transaction_processor(bucket, key)


def get_s3_bucket_key(event: dict):
    try:
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]
        
        return bucket, key
    except Exception as e:
        logger.fatal("unable to get bucket and key because error: {error}")
        
        return None, None


def get_total_balance(data: list):
    if len(data) < 1:
        return 0.0

    return round(sum([float(record[2]) for record in data]), 2)


def get_avg_amount_by_type(data: list, type: str):
    if type is None or not ["debit", "credit"]:
        return 0.0

    if type == "debit":
        debit_operations = [float(record[2]) for record in data if float(record[2]) < 0]

        if len(debit_operations) > 0:
            return round(statistics.mean(debit_operations), 2)

    if type == "credit":
        credit_operations = [float(record[2]) for record in data if float(record[2]) > 0]

        if len(credit_operations) > 0:
            return round(statistics.mean(credit_operations), 2)

    return 0.0


def get_monthly_transactions(data: list):
    monthly_transactions = {}
    grouped_dates = {}

    for record in data:
        date = datetime.strptime(record[1], "%Y-%m-%d %H:%M:%S")
        year = date.year
        month = date.month

        if year not in grouped_dates:
            grouped_dates[year] = {}

        if month not in grouped_dates[year]:
            grouped_dates[year][month] = []

        if float(record[2]) < 0:
            grouped_dates[year][month].append(record)
        else:
            grouped_dates[year][month].append(record)

    for year in grouped_dates:
        for month in grouped_dates[year]:
            if year not in monthly_transactions:
                monthly_transactions[year] = {}

            if month not in monthly_transactions[year]:
                monthly_transactions[year][month] = {}

            monthly_transactions[year][month] = {
                "month_transactions_count": len(grouped_dates[year][month]),
                "debit_transactions_count": len(
                    [record for record in grouped_dates[year][month] if float(record[2]) < 0]
                ),
                "debit_transaction_month_avg": get_avg_amount_by_type(
                    grouped_dates[year][month], "debit"
                ),
                "credit_transactions_count": len(
                    [record for record in grouped_dates[year][month] if float(record[2]) > 0]
                ),
                "credit_transaction_month_avg": get_avg_amount_by_type(
                    grouped_dates[year][month], "credit"
                ),
            }

    return monthly_transactions


def get_account_report(data: list, account_number: str):
    if account_number is None:
        return {}

    account_report = {
        "account_number": account_number,
        "total_balance": get_total_balance(data),
        "average_debit_amount": get_avg_amount_by_type(data, "debit"),
        "average_credit_amount": get_avg_amount_by_type(data, "credit"),
        "monthly_transactions": get_monthly_transactions(data),
    }

    return account_report


def send_message(sqs, message, tries=1, max_retries=3):
    try:
        response = sqs.send_message(
            QueueUrl=os.getenv("EMAIL_NOTIFICATIONS_QUEUE_URL"), MessageBody=message
        )
        return response["MessageId"]
    except Exception as e:
        if tries <= max_retries:
            logger.info(f"sending message failed at try: {tries} with error {e}")
            tries += 1
            logger.info(f"retry #{tries}…")
            return send_message(sqs, message, tries, max_retries)
        else:
            logger.info(f"message was not send. Max retries of {max_retries} reached.")
            return None


def get_account_number(key: str):
    parts = key.split("_")

    if len(parts) == 3:
        return parts[0]

    return None


def looks_like_headers(first_row):
    return all(isinstance(value, str) for value in first_row)


def persist_to_db(account_number: str, transactions: list, account_report: dict):
    try:
        with engine.begin() as connection:
            logger.info("Inserting transactions data into the database.")

            transactions_query = text(
                "INSERT INTO transactions (account_number, amount, transaction_id, date) "
                "VALUES (:account_number, :amount, :transaction_id, :date)"
            )

            # Create a list of dictionaries representing the data
            transactions_to_insert = [
                {
                    "account_number": account_number,
                    "amount": transaction[2],
                    "transaction_id": transaction[3],
                    "date": transaction[1],
                }
                for transaction in transactions
            ]

            report_query = text(
                "INSERT INTO reports (account_number, total_balance, average_debit_amount, average_credit_amount, monthly_transactions) "
                "VALUES (:account_number, :total_balance, :average_debit_amount, :average_credit_amount, :monthly_transactions)"
            )

            report_to_insert = {
                "account_number": account_report["account_number"],
                "total_balance": account_report["total_balance"],
                "average_debit_amount": account_report["average_debit_amount"],
                "average_credit_amount": account_report["average_credit_amount"],
                "monthly_transactions": json.dumps(account_report["monthly_transactions"]),
            }

            # Use a transaction to ensure atomicity
            try:
                connection.execute(transactions_query, transactions_to_insert)
                connection.execute(report_query, report_to_insert)
                logger.info("transactions and report inserted successfully.")
                return True
            except Exception as e:
                logger.info(e)
                connection.rollback()  # Rollback the transaction
            except IntegrityError as e:
                # Handle any integrity constraint violations here
                logger.info(f"Integrity error: {e}")
                connection.rollback()  # Rollback the transaction
                return False
    except Exception as e:
        logger.info(f"Unable to save the record: {e}")
        return False


def transaction_processor(bucket: str, key: str):
    file = None

    try:
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)
        file = response["Body"].read()
        logger.info("read file from s3 successfully")
    except Exception as e:
        logger.error(f"error: unable to download the file from s3 - {e}")
        sys.exit(-1)

    csv_reader = csv.reader(file.decode("utf-8").splitlines(), delimiter=",")
    first_row = next(csv_reader)

    if looks_like_headers(first_row):
        data_rows = [row for row in csv_reader]
    else:
        file.seek(0)
        data_rows = [first_row] + [row for row in csv_reader]

    account_number = get_account_number(key)

    if not account_number:
        logger.error(f"unable to determine the account number for file: {bucket}/{key}")
        sys.exit(-1)

    account_report = get_account_report(data_rows, account_number)

    persisted_data = persist_to_db(account_number, data_rows, account_report)

    if not persisted_data:
        logger.info("error saving to database")
        sys.exit(-1)

    message_body = json.dumps(account_report)

    sqs = boto3.client("sqs")
    message_id = send_message(sqs, message_body)
    
    if message_id:
        logger.info(f"message {message_id} sent successfully")
