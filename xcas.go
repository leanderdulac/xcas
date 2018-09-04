package xcas

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

type Config struct {
	Keyspace  string
	Addresses []string
	Username  string
	Password  string
	Timeout   int
	Retries   int
}

func CreateSession(config *Config) (session *gocql.Session, err error) {

	cluster := gocql.NewCluster(config.Addresses...)
	cluster.Consistency = gocql.One
	cluster.Timeout = time.Duration(config.Timeout) * time.Second
	cluster.Keyspace = config.Keyspace
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: config.Retries}
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: config.Username,
		Password: config.Password,
	}

	if session, err = cluster.CreateSession(); err != nil {
		return nil, err
	}

	return session, nil
}

func DefaultIndexName(tableName string, columnName string) string {
	return fmt.Sprintf("%s_%s_idx", tableName, columnName)
}

func HasIndexByName(session *gocql.Session, keyspaceName string, indexName string) (bool, error) {

	stmt := `SELECT COUNT(*) FROM system."IndexInfo" WHERE table_name = ? and index_name = ? LIMIT 1`
	query := session.Query(stmt, keyspaceName, indexName)
	defer query.Release()

	if err := query.Exec(); err != nil {
		return false, err
	}

	var result int
	if err := query.Scan(&result); err != nil {
		return false, err
	}

	return result > 0, nil
}

func HasIndex(session *gocql.Session, keyspaceName string, tableName string, columnName string) (bool, error) {
	return HasIndexByName(session, keyspaceName, DefaultIndexName(tableName, columnName))
}

func CreateIndexIfNotExists(session *gocql.Session, keyspaceName string, tableName string, columnName string) (bool, error) {

	if exists, err := HasIndex(session, keyspaceName, tableName, columnName); err != nil {
		return false, err
	} else if exists {
		return false, nil
	}

	stmt := fmt.Sprintf(`CREATE INDEX ON %s.%s(%s)`, keyspaceName, tableName, columnName)
	query := session.Query(stmt)
	defer query.Release()

	if err := query.Exec(); err != nil {
		return false, err
	}

	return true, nil
}

func CreateIndexes(session *gocql.Session, keyspaceName string, tableName string, columns []string) (bool, error) {

	for _, columnName := range columns {
		if created, err := CreateIndexIfNotExists(session, keyspaceName, tableName, columnName); err != nil {
			return created, err
		}
	}
	return true, nil
}

func HasTable(session *gocql.Session, keyspaceName string, tableName string) (bool, error) {

	stmt := `
	SELECT COUNT(table_name)
	FROM system_schema.tables
	WHERE keyspace_name = ? and table_name = ?`
	query := session.Query(stmt, keyspaceName, tableName)
	defer query.Release()

	if err := query.Exec(); err != nil {
		return false, err
	}

	var count int
	if err := query.Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func TruncateTable(session *gocql.Session, tableName string) error {

	stmt := fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	query := session.Query(stmt)
	defer query.Release()

	if err := query.Exec(); err != nil {
		return err
	}
	return nil
}
