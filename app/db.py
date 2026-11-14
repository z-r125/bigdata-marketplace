"""
Accès Cassandra partagé par toutes les requêtes (T1-T15, A16-A25, bench).

- une seule session par processus (la connexion est coûteuse : on la réutilise)
- requêtes préparées en cache (analysées une seule fois par le cluster)
- niveau de cohérence réglable par requête (expérience ONE / QUORUM / ALL)
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import BatchStatement, BatchType

HOSTS = os.getenv("CASS_HOSTS", "cass1,cass2,cass3").split(",")
KEYSPACE = os.getenv("CASS_KEYSPACE", "marketplace")

CL = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
    "ALL": ConsistencyLevel.ALL,
    "SERIAL": ConsistencyLevel.SERIAL,
}
DEFAULT_CL = "LOCAL_QUORUM"

_cluster = None
_session = None
_prepared: dict = {}


def session():
    """Session unique, créée à la première utilisation."""
    global _cluster, _session
    if _session is None:
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc="dc1")),
            consistency_level=CL[DEFAULT_CL],
            request_timeout=30,
        )
        _cluster = Cluster(HOSTS, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
        _session = _cluster.connect(KEYSPACE)
    return _session


def shutdown():
    global _cluster, _session
    if _cluster is not None:
        _cluster.shutdown()
    _cluster = _session = None
    _prepared.clear()


def prepared(cql: str):
    stmt = _prepared.get(cql)
    if stmt is None:
        stmt = _prepared[cql] = session().prepare(cql)
    return stmt


def _cl(cl):
    return CL[cl] if isinstance(cl, str) else cl


def bind(cql: str, params=(), cl=None, fetch_size=None):
    stmt = prepared(cql).bind(params)
    if cl is not None:
        stmt.consistency_level = _cl(cl)
    if fetch_size:
        stmt.fetch_size = fetch_size
    return stmt


def execute(cql: str, params=(), cl=None, fetch_size=None, paging_state=None, timeout=30):
    """Exécute une requête préparée. cl : 'ONE' | 'QUORUM' | 'LOCAL_QUORUM' | 'ALL' | 'SERIAL'."""
    return session().execute(bind(cql, params, cl, fetch_size), paging_state=paging_state, timeout=timeout)


def make_batch(statements, cl=None, logged=True) -> BatchStatement:
    """statements : liste de (cql, params). LOGGED = atomique (tout ou rien)."""
    b = BatchStatement(batch_type=BatchType.LOGGED if logged else BatchType.UNLOGGED)
    if cl is not None:
        b.consistency_level = _cl(cl)
    for cql, params in statements:
        b.add(prepared(cql), params)
    return b


def batch(statements, cl=None, logged=True):
    return session().execute(make_batch(statements, cl, logged))


# ------------------------------------------------------------------ utilitaires
def now_utc() -> datetime:
    """Horodatage UTC tronqué à la milliseconde (précision du type timestamp)."""
    dt = datetime.now(timezone.utc).replace(tzinfo=None)
    return dt.replace(microsecond=dt.microsecond // 1000 * 1000)


def money(x) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"))


# Les UDT sont passés sous forme de tuples, dans l'ordre des champs du CREATE TYPE
def udt_address(a) -> tuple:
    if isinstance(a, dict):
        return (a["label"], a["street"], a["city"], a["postal_code"], a.get("country", "Maroc"))
    return tuple(a)  # UDT relu depuis Cassandra


def udt_order_line(l: dict) -> tuple:
    return (l["product_id"], l["sku"], l["name"], l["category"], int(l["quantity"]), money(l["unit_price"]))