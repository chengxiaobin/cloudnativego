package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"sync"

	_ "github.com/go-sql-driver/mysql" // Load the Mysql drivers
)

type MysqlDbParams struct {
	dbName   string
	host     string
	user     string
	password string
}

type MysqlTransactionLogger struct {
	events chan<- Event // Write-only channel for sending events
	errors <-chan error // Read-only channel for receiving errors
	db     *sql.DB      // Our database access interface
	wg     *sync.WaitGroup
}

func (l *MysqlTransactionLogger) WritePut(key, value string) {
	l.wg.Add(1)
	l.events <- Event{EventType: EventPut, Key: key, Value: url.QueryEscape(value)}
}

func (l *MysqlTransactionLogger) WriteDelete(key string) {
	l.wg.Add(1)
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *MysqlTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *MysqlTransactionLogger) LastSequence() uint64 {
	return 0
}

func checkErr(err error) {
    if err != nil {
        panic(err)
    }
}

func (l *MysqlTransactionLogger) Run() {
	events := make(chan Event, 16) // Make an events channel
	l.events = events

	errors := make(chan error, 1) // Make an errors channel
	l.errors = errors

	go func() { // The INSERT query

		query := `INSERT INTO transactions
   (event_type, key1, value)
   VALUES (?, ?, ?)`
		
		for e := range events { // Retrieve the next Event
			_, err := l.db.Exec( // Execute the INSERT query
				query,
				e.EventType, e.Key, e.Value)

			if err != nil {
				errors <- err
			}
		}
		log.Println(query)
	}()
}

func (l *MysqlTransactionLogger) Wait() {
	l.wg.Wait()
}

func (l *MysqlTransactionLogger) Close() error {
	l.wg.Wait()

	if l.events != nil {
		close(l.events) // Terminates Run loop and goroutine
	}

	return l.db.Close()
}

func (l *MysqlTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)    // An unbuffered events channel
	outError := make(chan error, 1) // A buffered errors channel

	query := "SELECT sequence, event_type, key1, value FROM transactions"

	go func() {
		defer close(outEvent) // Close the channels when the
		defer close(outError) // goroutine ends

		rows, err := l.db.Query(query) // Run query; get result set
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}

		defer rows.Close() // This is important!

		var e Event // Create an empty Event

		for rows.Next() { // Iterate over the rows

			err = rows.Scan( // Read the values from the
				&e.Sequence, &e.EventType, // row into the Event.
				&e.Key, &e.Value)

			if err != nil {
				outError <- err
				return
			}

			outEvent <- e // Send e to the channel
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()

	return outEvent, outError
}

/* func (l *MysqlTransactionLogger) verifyTableExists() (bool, error) {
	const table = "transactions"

	var result string

	rows, err := l.db.Query(fmt.Sprintf("SELECT %s", table))
	defer rows.Close()
	if err != nil {
		return false, err
	}

	for rows.Next() && result != table {
		rows.Scan(&result)
	}

	return result == table, rows.Err()
} */

func (l *MysqlTransactionLogger) createTable() error {
	var err error

	createQuery :=`use test;` /* CREATE TABLE IF NOT EXISTS  `transactions` (
		`sequence`      INT(10) NOT NULL AUTO_INCREMENT,
		`event_type`    SMALLINT,
		`key`     TEXT,
		`value`         TEXT,
	  PRIMARY KEY (`sequence`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8; */

	_, err = l.db.Exec(createQuery)
	if err != nil {
		return err
	}

	return nil
}

func NewMysqlTransactionLogger(param MysqlDbParams) (TransactionLogger, error) {//"astaxie:astaxie@/test?charset=utf8"  
	//用户名:密码@tcp(地址:端口)/数据库名
	connStr := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s",
		param.user, param.password, param.host, param.dbName)

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create db value: %w", err)
	}

	err = db.Ping() // Test the databases connection
	if err != nil {
		return nil, fmt.Errorf("failed to opendb connection: %w", err)
	}

	tl := &MysqlTransactionLogger{db: db, wg: &sync.WaitGroup{}}

	/* exists, err := tl.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}
	if !exists { */
	if err = tl.createTable(); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	

	return tl, nil
}
