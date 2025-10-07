package main

import (
    "bufio"
    "context"
    "database/sql"
    "encoding/csv"
    "fmt"
    "log"
    "os"
    "strconv"
    "strings"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    _ "github.com/lib/pq"
)

func main() {
    ctx := context.Background()

    // Fetch DB credentials from environment
    dbHost := os.Getenv("DBHOSTMASTER")
    dbPortStr := os.Getenv("DBPORT")
    dbUser := os.Getenv("DBUSERNAME")
    dbPassword := os.Getenv("DBPASSWORD")
    dbName := os.Getenv("DB_SCORING")

    dbPort, err := strconv.Atoi(dbPortStr)
    if err != nil {
        log.Fatalf("Invalid DB_PORT: %v", err)
    }

    // Fetch AWS credentials from environment
    awsAccessKeyID := os.Getenv("S3ACCESSKEYID")
    awsSecretAccessKey := os.Getenv("S3SECRETACCESSKEY")
    awsRegion := os.Getenv("S3REGION")
    bucketName := os.Getenv("S3BUCKETNAME")
    objectKey := os.Getenv("S3OBJECTKEY")

    // Connect to PostgreSQL
    db, err := connectPostgres(dbHost, dbPort, dbUser, dbPassword, dbName)
    if err != nil {
        log.Fatalf("DB connection failed: %v", err)
    }
    defer db.Close()

    // Load static AWS credentials
    cfg, err := config.LoadDefaultConfig(ctx,
        config.WithRegion(awsRegion),
        config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
            awsAccessKeyID, awsSecretAccessKey, "",
        )),
    )
    if err != nil {
        log.Fatalf("AWS config load error: %v", err)
    }

    s3Client := s3.NewFromConfig(cfg)

    // Download CSV from S3
    csvReader, err := downloadCSVFromS3(ctx, s3Client, bucketName, objectKey)
    if err != nil {
        log.Fatalf("Error reading CSV from S3: %v", err)
    }

    // Create log files
    successLog, _ := os.Create("success_logs.txt")
    defer successLog.Close()
    errorLog, _ := os.Create("error_logs.txt")
    defer errorLog.Close()

    reader := csv.NewReader(csvReader)
    reader.TrimLeadingSpace = true

    // Read header and count columns
    header, err := reader.Read()
    if err != nil {
        log.Fatalf("Error reading header: %v", err)
    }
    numCols := len(header)

    // Count rows
    numRows := 0
    records := [][]string{}
    for {
        record, err := reader.Read()
        if err != nil {
            break
        }
        records = append(records, record)
        numRows++
    }

    // Process records and log row number and remaining rows
    for i, record := range records {
        rowNum := i + 1
        transactionID := strings.TrimSpace(record[0])
        if transactionID == "" {
            logMsg := fmt.Sprintf("Row %d: Empty transaction_id\n", rowNum)
            fmt.Fprint(errorLog, logMsg)
            continue
        }

        // Parse datetime
        datetimeStr := strings.TrimSpace(record[1])
        datetime, err := time.Parse("2006-01-02 15:04:05", datetimeStr)
        if err != nil {
            logMsg := fmt.Sprintf("Row %d: Invalid datetime for ID %s: '%s'\n", rowNum, transactionID, datetimeStr)
            fmt.Fprint(errorLog, logMsg)
            continue
        }

        // Update PostgreSQL
        err = updateTransactionDatetime(db, transactionID, datetime)
        remaining := numRows - rowNum
        if err != nil {
            fmt.Fprintf(errorLog, "Row %d: DB update failed for transaction_id %s for transaction_datetime %s: %v\n", rowNum, transactionID, datetime.Format("2006-01-02 15:04:05"), err)
        } else {
            fmt.Fprintf(successLog, "Row %d: Updated transaction_id %s for transaction_datetime %s\n", rowNum, transactionID, datetime.Format("2006-01-02 15:04:05"))
        }
    }
}

func connectPostgres(host string, port int, user, password, dbname string) (*sql.DB, error) {
    connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
        host, port, user, password, dbname)
    return sql.Open("postgres", connStr)
}

func downloadCSVFromS3(ctx context.Context, client *s3.Client, bucket, key string) (*bufio.Reader, error) {
    output, err := client.GetObject(ctx, &s3.GetObjectInput{
        Bucket: aws.String(bucket),
        Key:    aws.String(key),
    })
    if err != nil {
        return nil, err
    }
    return bufio.NewReader(output.Body), nil
}

func updateTransactionDatetime(db *sql.DB, transactionID string, newDatetime time.Time) error {
    query := `UPDATE new_score_transactions SET transaction_datetime = $1 WHERE transaction_id = $2`
    res, err := db.Exec(query, newDatetime, transactionID)
    if err != nil {
        return err
    }

    rows, err := res.RowsAffected()
    if err != nil {
        return err
    }
    if rows == 0 {
        return fmt.Errorf("No record found for ID %s", transactionID)
    }

    return nil
}
