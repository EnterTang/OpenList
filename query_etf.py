import sqlite3
import json

db = "/home/entertang/docker_dirs/openlist_etf/data/data.db"
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

job_ids = [
    "8dd20576-4468-439d-ae6e-a5c134a6b285",
    "4e7ea06a-979b-4626-8853-5510b16a8e4c",
    "512dcbd8-dcc9-44dd-b9e9-a0d409da1977",
    "cb709e88-db60-4bdb-8f68-a7c101ed3bc6",
]

print("=== x_cluster_jobs ===")
cur.execute("SELECT id, status, notification_status, task_context_json, updated_at FROM x_cluster_jobs WHERE id IN (%s)" % ",".join("?"*len(job_ids)), job_ids)
for row in cur.fetchall():
    ctx = json.loads(row["task_context_json"])
    media = ctx.get("media", {})
    print(f"job={row['id']} status={row['status']} notify={row['notification_status']} ep={media.get('episode')} updated={row['updated_at']}")

print("\n=== x_etf_archive_records for 九门 ===")
cur.execute("SELECT id, source_name, source_path, local_etf_path, archive_etf_path, status, source_size, source_sha256, season, episode FROM x_etf_archive_records WHERE source_name LIKE ? ORDER BY episode", ("%九门%",))
for row in cur.fetchall():
    print(f"id={row['id']} ep={row['episode']} status={row['status']} size={row['source_size']} sha={row['source_sha256']}")
    print(f"  source={row['source_name']}")
    print(f"  local_etf={row['local_etf_path']}")
    print(f"  archive_etf={row['archive_etf_path']}")

print("\n=== x_cluster_upload_manifests ===")
cur.execute("SELECT id, job_id, name, remote_path, size, sha256, upload_receipt, remote_file_id, remote_parent_id, operation_key FROM x_cluster_upload_manifests WHERE job_id IN (%s)" % ",".join("?"*len(job_ids)), job_ids)
for row in cur.fetchall():
    print(f"job={row['job_id']} name={row['name']} remote={row['remote_path']} size={row['size']} sha={row['sha256']} receipt={row['upload_receipt']} op={row['operation_key']}")
    print(f"  remote_file_id={row['remote_file_id']} remote_parent_id={row['remote_parent_id']}")

print("\n=== x_subscription_items for 九门 140 ===")
cur.execute("SELECT id, subscription_id, source_key, file_name, target_dir, target_name, target_path, status, last_error, cluster_job_id FROM x_subscription_items WHERE subscription_id=? AND (file_name LIKE ? OR target_name LIKE ?) ORDER BY id", (140, "%九门%", "%九门%"))
for row in cur.fetchall():
    print(f"id={row['id']} file={row['file_name']} target={row['target_path']} status={row['status']} job={row['cluster_job_id']} err={row['last_error']}")
