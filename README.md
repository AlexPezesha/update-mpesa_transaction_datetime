# go-app-update

This Go application downloads a CSV file from AWS S3, processes each row, and updates transaction datetimes in a PostgreSQL database. It logs successes and errors to separate files and prints progress to the console.

## Features

- Fetches DB and AWS credentials from environment variables
- Downloads a CSV file from S3
- Updates transaction datetimes in PostgreSQL
- Logs successes and errors with row numbers
- Prints progress and remaining rows to the console

## Prerequisites

- Go 1.22 or newer
- PostgreSQL database
- AWS S3 bucket with the CSV file
- Set required environment variables

## Environment Variables

Set these in your shell or `~/.profile`:

```sh
export DBHOSTMASTER=your_db_host
export DBPORT=5432
export DBUSERNAME=your_db_user
export DBPASSWORD=your_db_password
export DB_SCORING=your_db_name

export S3ACCESSKEYID=your_aws_access_key_id
export S3SECRETACCESSKEY=your_aws_secret_access_key
export S3BUCKETNAME=your_s3_bucket_name
export S3REGION=your_s3_bucket_region
```

## Usage

1. Install dependencies:

    ```sh
    go mod tidy
    ```

2. Run the application:

    ```sh
    go run main.go
    ```

3. Check logs:

    - `success_logs.txt` for successful updates
    - `error_logs.txt` for errors

## Configuration

- The table name in PostgreSQL is set to `new_score_transactions` in the code.
- The CSV file should have `transaction_id` and `transaction_datetime` as the first two columns.

## Notes

- Only the first 10 rows are updated (can be changed in code).
- Credentials are printed for debugging; remove these prints in production.
- Make sure your AWS credentials and region match your S3 bucket.

## License

MIT# update-mpesa_transaction_datetime
