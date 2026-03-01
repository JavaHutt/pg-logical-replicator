package snapshotter

var (
	getSnapshotterQuery        = "SELECT id, started_at, table_name, key, version, is_done FROM snapshotters WHERE id = $1 LIMIT 1"
	createSnapshotterQuery     = "INSERT INTO snapshotters (id, table_name, started_at) values ($1, $2, $3)"
	updateSnapshotterQuery     = "UPDATE snapshotters SET key = $1, version = $2 WHERE id = $3"
	updateSnapshotterDoneQuery = "UPDATE snapshotters SET key = $1, version = $2, is_done = $3 WHERE id = $4"

	selectDataQuery = `SELECT %[1]s, %[2]s, row_to_json(t) FROM %[3]s t
		WHERE created_at <= $1 AND (%[2]s, %[1]s) > ($2, $3)
		ORDER BY (%[4]s) ASC
		LIMIT %[5]d`
)
