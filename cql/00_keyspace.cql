-- Keyspace : 3 répliques dans le datacenter dc1 (exigence : RF = 3)
CREATE KEYSPACE IF NOT EXISTS marketplace
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};