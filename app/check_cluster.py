from cassandra.cluster import Cluster

cluster = Cluster(["cass1", "cass2", "cass3"])
session = cluster.connect()
hosts = [h.address for h in cluster.metadata.all_hosts()]
version = session.execute("SELECT release_version FROM system.local").one().release_version
print(f"Cluster : {cluster.metadata.cluster_name} | Cassandra {version}")
print(f"Noeuds vus par le driver ({len(hosts)}) : {hosts}")
cluster.shutdown()