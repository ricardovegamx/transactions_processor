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
        logger.error(f"unable to get bucket and key because error: {e}")

        return None, None


def get_total_balance(data: list):
    if len(data) < 1:
        return 0.0

    return round(sum([float(record[2]) for record in data]), 2)


def get_avg_amount_by_type(data: list, type: str):
    if type is None or not ["debit", "credit"]:
        return 0.0

    if type == "debit":
        # segment to debit operations (those with negative values)
        debit_operations = [float(record[2]) for record in data if float(record[2]) < 0]

        if len(debit_operations) > 0:
            return round(statistics.mean(debit_operations), 2)

    if type == "credit":
        # segment the credit operations (positive values)
        credit_operations = [float(record[2]) for record in data if float(record[2]) > 0]

        if len(credit_operations) > 0:
            return round(statistics.mean(credit_operations), 2)

    return 0.0


def get_monthly_transactions(transactions: list):
    grouped_dates = {}  # transactions segmented by year and (unordered) months
    monthly_transactions = {}  # transactions segmented by year/month and statistics

    # first: segment the transactions by year and month
    for record in transactions:
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

    # calculate the stats (avg, count, etc)
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

    # now start sorting the monthly transactions by getting the main key
    year_key = next(iter(monthly_transactions))

    # sort the keys inside the year dictionary in descending order
    sorted_transactions = {
        year_key: dict(
            sorted(
                monthly_transactions[year_key].items(), key=lambda item: int(item[0]), reverse=True
            )
        )
    }

    return sorted_transactions


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
            logger.error(f"sending message failed at try: {tries} with error {e}")
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
    # if all values are str, in the case of this CSV format, they are headers
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

            # use a transaction to ensure atomicity
            try:
                connection.execute(transactions_query, transactions_to_insert)
                connection.execute(report_query, report_to_insert)
                logger.info("transactions and report inserted successfully.")
                return True
            except IntegrityError as e:
                logger.error(f"integrity error: {e}")
                connection.rollback()  # rollback the transaction
    except Exception as e:
        logger.info(f"Unable to save the record: {e}")
        return False


def transaction_processor(bucket: str, key: str):
    file = None

    # try to get the file with transactions from S3
    try:
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)
        file = response["Body"].read()
        logger.info("read file from s3 successfully")
    except Exception as e:
        logger.error(f"error: unable to download the file from s3 - {e}")
        sys.exit(-1)

    # load the file
    csv_reader = csv.reader(file.decode("utf-8").splitlines(), delimiter=",")

    # validation: check if the file has headers
    first_row = next(csv_reader)

    # define what the transactions will be (with and without headers)
    if looks_like_headers(first_row):
        data_rows = [row for row in csv_reader]
    else:
        file.seek(0)
        data_rows = [first_row] + [row for row in csv_reader]

    account_number = get_account_number(key)

    # catch if the naming convention of the files has been changed
    if not account_number:
        logger.error(f"unable to determine the account number for file: {bucket}/{key}")
        sys.exit(-1)

    # generate the account report (averages, totals, count of transactions…)
    account_report = get_account_report(data_rows, account_number)

    # save to database the info related to the account
    persisted_data = persist_to_db(account_number, data_rows, account_report)

    if not persisted_data:
        logger.error("error saving to database")
        sys.exit(-1)

    message_body = json.dumps(account_report)

    # send msg to queue (it will trigger the mailing service)
    sqs = boto3.client("sqs")
    message_id = send_message(sqs, message_body)

    if message_id:
        logger.info(f"message {message_id} sent successfully")
