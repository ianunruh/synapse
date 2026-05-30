module github.com/ianunruh/synapse/examples/postgres

go 1.26

require (
	github.com/ianunruh/synapse v0.0.0-00010101000000-000000000000
	github.com/ianunruh/synapse/checkpointstore/postgres v0.0.0-00010101000000-000000000000
	github.com/ianunruh/synapse/eventstore/postgres v0.0.0-00010101000000-000000000000
	github.com/ianunruh/synapse/snapshotstore/postgres v0.0.0-00010101000000-000000000000
	github.com/jackc/pgx/v5 v5.9.2
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/text v0.34.0 // indirect
)

replace (
	github.com/ianunruh/synapse => ../..
	github.com/ianunruh/synapse/checkpointstore/postgres => ../../checkpointstore/postgres
	github.com/ianunruh/synapse/eventstore/postgres => ../../eventstore/postgres
	github.com/ianunruh/synapse/pgtest => ../../pgtest
	github.com/ianunruh/synapse/snapshotstore/postgres => ../../snapshotstore/postgres
)
