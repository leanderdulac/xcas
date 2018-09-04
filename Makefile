KEYSPACE := xcas

cassandra:
	docker-compose up -d cassandra
	docker-compose exec cassandra sh -c 'until cqlsh -e exit >/dev/null 2>&1; do echo "initializing cassandra..."; sleep 1; done'
	docker-compose exec cassandra cqlsh -e "create keyspace if not exists $(KEYSPACE) with replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };"

test:
	@env CASSANDRA_ADDRESS=localhost \
       CASSANDRA_KEYSPACE=$(KEYSPACE) \
       go test -v -cover $(shell glide novendor)
