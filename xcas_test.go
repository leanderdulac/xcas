package xcas_test

import (
	"testing"

	"github.com/drrzmr/xcas"
	"github.com/stretchr/testify/require"
)

func TestCassandra(t *testing.T) {

	config := &xcas.Config{
		Addresses: []string{"127.0.0.1"},
		Keyspace:  "xcas",
		Username:  "",
		Password:  "",
		Timeout:   10,
		Retries:   3,
	}

	session, err := xcas.CreateSession(config)
	require.NoError(t, err)
	require.NotNil(t, session)

	t.Run("HasTable", func(t *testing.T) {

		queryDropTableIfExists := session.Query("DROP TABLE IF EXISTS tests")
		defer queryDropTableIfExists.Release()
		err = queryDropTableIfExists.Exec()
		require.NoError(t, err)

		queryCreateTable := session.Query("CREATE TABLE tests (id int PRIMARY KEY, name text)")
		defer queryCreateTable.Release()
		err = queryCreateTable.Exec()
		require.NoError(t, err)

		var has bool

		has, err = xcas.HasTable(session, config.Keyspace, "tests")
		require.NoError(t, err)
		require.True(t, has)

		has, err = xcas.HasTable(session, config.Keyspace, "no_exists_table")
		require.NoError(t, err)
		require.False(t, has)

		has, err = xcas.HasTable(session, "no_exists_keyspace", "no_exists_table")
		require.NoError(t, err)
		require.False(t, has)

		queryDropTable := session.Query("DROP TABLE tests")
		defer queryDropTable.Release()
		err = queryDropTable.Exec()
		require.NoError(t, err)
	})

	t.Run("DefaultIndexName", func(t *testing.T) {

		indexName := xcas.DefaultIndexName("table", "column")
		require.Equal(t, "table_column_idx", indexName)
	})

	t.Run("HasIndexByName", func(t *testing.T) {
		var ok bool
		ok, err = xcas.HasIndexByName(session, config.Keyspace, xcas.DefaultIndexName("test", "test"))
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("HasIndex", func(t *testing.T) {
		var ok bool
		ok, err = xcas.HasIndex(session, config.Keyspace, "test", "test")
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("CreateIndexIfNotExists", func(t *testing.T) {

		queryDropTableIfExists := session.Query("DROP TABLE IF EXISTS tests")
		defer queryDropTableIfExists.Release()
		err = queryDropTableIfExists.Exec()
		require.NoError(t, err)

		queryCreateTable := session.Query("CREATE TABLE tests (id int PRIMARY KEY, name text)")
		defer queryCreateTable.Release()
		err = queryCreateTable.Exec()
		require.NoError(t, err)

		var created bool
		created, err = xcas.CreateIndexIfNotExists(session, config.Keyspace, "tests", "name")
		require.NoError(t, err)
		require.True(t, created)

		queryDropTable := session.Query("DROP TABLE tests")
		defer queryDropTable.Release()
		err = queryDropTable.Exec()
		require.NoError(t, err)
	})

	t.Run("CreateIndexes", func(t *testing.T) {

		queryDropTableIfExists := session.Query("DROP TABLE IF EXISTS tests")
		defer queryDropTableIfExists.Release()
		err = queryDropTableIfExists.Exec()
		require.NoError(t, err)

		queryCreateTable := session.Query("CREATE TABLE tests (id int PRIMARY KEY, name text, age int)")
		defer queryCreateTable.Release()
		err = queryCreateTable.Exec()
		require.NoError(t, err)

		var created bool
		created, err = xcas.CreateIndexes(session, config.Keyspace, "tests", []string{"name", "age"})
		require.NoError(t, err)
		require.True(t, created)

		var ok bool

		ok, err = xcas.HasIndex(session, config.Keyspace, "tests", "name")
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = xcas.HasIndex(session, config.Keyspace, "tests", "age")
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = xcas.HasIndex(session, config.Keyspace, "tests", "age")
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = xcas.HasIndex(session, config.Keyspace, "tests", "non-exists")
		require.NoError(t, err)
		require.False(t, ok)

		queryDropTable := session.Query("DROP TABLE tests")
		defer queryDropTable.Release()
		err = queryDropTable.Exec()
		require.NoError(t, err)
	})

	t.Run("TruncateTable", func(t *testing.T) {

		n := 10
		count := 0

		queryDropTableIfExists := session.Query("DROP TABLE IF EXISTS tests")
		defer queryDropTableIfExists.Release()
		err = queryDropTableIfExists.Exec()
		require.NoError(t, err)

		queryCreateTable := session.Query("CREATE TABLE tests (id int PRIMARY KEY)")
		defer queryCreateTable.Release()
		err := queryCreateTable.Exec()
		require.NoError(t, err)

		for i := 0; i < n; i++ {
			queryInsert := session.Query("INSERT INTO tests(id) VALUES(?)", i)
			defer queryInsert.Release()
			err = queryInsert.Exec()
			require.NoError(t, err)
		}

		queryCountWithN := session.Query("SELECT COUNT(id) FROM tests")
		defer queryCountWithN.Release()
		err = queryCountWithN.Exec()
		require.NoError(t, err)
		err = queryCountWithN.Scan(&count)
		require.NoError(t, err)
		require.Equal(t, n, count)

		err = xcas.TruncateTable(session, "tests")
		require.NoError(t, err)

		queryCountEmpty := session.Query("SELECT COUNT(id) FROM tests")
		defer queryCountEmpty.Release()
		err = queryCountEmpty.Exec()
		require.NoError(t, err)
		err = queryCountEmpty.Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 0, count)

		queryDropTable := session.Query("DROP TABLE tests")
		defer queryDropTable.Release()
		err = queryDropTable.Exec()
		require.NoError(t, err)

	})

	session.Close()
	require.True(t, session.Closed())
}
