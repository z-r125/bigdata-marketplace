"""
Tableau de bord - Mini-projet Big Data (Marketplace, Cassandra + Spark).
Lit uniquement les CSV produits par le bench, l'export analytique et les jobs Spark :
il fonctionne même si un nœud Cassandra est arrêté pendant la démonstration.
Lancement : make dash  ->  http://localhost:8501
"""
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
BENCH = ROOT / "bench" / "results"
SPARK = ROOT / "spark" / "results"

st.set_page_config(page_title="Marketplace - Cassandra & Spark", layout="wide")


@st.cache_data
def read(path: Path):
    return pd.read_csv(path) if path.exists() else None


def available(df, name):
    if df is None:
        st.info(f"Données absentes : {name}")
        return False
    return True


def last_runs(df):
    """Dernière mesure par (requête, variante)."""
    return df.sort_values("timestamp").groupby(["query", "variant"]).last().reset_index()


# ------------------------------------------------------------------ en-tête + indicateurs
st.title("Marketplace — Cassandra 5.0 & Spark 3.5")
st.caption("Cluster de 3 nœuds (RF = 3) · Spark 1 master + 2 workers · 30 requêtes mesurées")

manifest = json.loads((ROOT / "data" / "manifest.json").read_text()) if (ROOT / "data" / "manifest.json").exists() else {}
a19, a22 = read(BENCH / "a19_ca_categorie.csv"), read(BENCH / "a22_paiement.csv")
k1, k2, k3, k4 = st.columns(4)
k1.metric("Éléments générés", f"{manifest.get('total_elements_enonce', 0):,}".replace(",", " "))
k2.metric("Commandes (12 mois)", f"{int(a22['nb_commandes'].sum()):,}".replace(",", " ") if a22 is not None else "-")
k3.metric("CA hors annulations", f"{a19['ca_mad'].sum() / 1e6:,.1f} M MAD" if a19 is not None else "-")
k4.metric("Date de référence", manifest.get("ref_date", "-"))

tab_sales, tab_clients, tab_perf = st.tabs(["Ventes", "Clients et comportements", "Performances"])

# ------------------------------------------------------------------ onglet 1 : ventes
with tab_sales:
    c1, c2 = st.columns(2)
    a16 = read(BENCH / "a16_ca_par_jour.csv")
    if available(a16, "A16"):
        c1.plotly_chart(px.line(a16, x="jour", y="ca_mad", markers=True,
                                title="A16 — Chiffre d'affaires par jour (30 derniers jours)",
                                labels={"jour": "Jour", "ca_mad": "CA (MAD)"}), use_container_width=True)
    if available(a19, "A19"):
        c2.plotly_chart(px.bar(a19, x="categorie", y="ca_mad", title="A19 — Chiffre d'affaires par catégorie",
                               labels={"categorie": "Catégorie", "ca_mad": "CA (MAD)"}), use_container_width=True)
    c3, c4 = st.columns(2)
    a17 = read(BENCH / "a17_top_produits.csv")
    if available(a17, "A17"):
        c3.plotly_chart(px.bar(a17.sort_values("quantite"), x="quantite", y="nom", orientation="h",
                               title="A17 — Top 10 des produits vendus",
                               labels={"quantite": "Quantité vendue", "nom": ""}), use_container_width=True)
    a25 = read(BENCH / "a25_annulation.csv")
    if available(a25, "A25"):
        c4.plotly_chart(px.bar(a25, x="categorie", y="taux_pct", title="A25 — Taux d'annulation par catégorie",
                               labels={"categorie": "Catégorie", "taux_pct": "Lignes annulées (%)"}),
                        use_container_width=True)
    c5, c6 = st.columns(2)
    a20 = read(BENCH / "a20_panier_ville.csv")
    if available(a20, "A20"):
        c5.plotly_chart(px.bar(a20, x="ville", y="panier_moyen_mad", title="A20 — Panier moyen par ville",
                               labels={"ville": "Ville", "panier_moyen_mad": "Panier moyen (MAD)"}),
                        use_container_width=True)
    if available(a22, "A22"):
        c6.plotly_chart(px.bar(a22, x="mode_paiement", y="nb_commandes", title="A22 — Commandes par mode de paiement",
                               labels={"mode_paiement": "Mode de paiement", "nb_commandes": "Commandes"}),
                        use_container_width=True)

# ------------------------------------------------------------------ onglet 2 : clients (Spark)
with tab_clients:
    c1, c2 = st.columns(2)
    s26 = read(SPARK / "s26_rfm_segments.csv")
    if available(s26, "S26"):
        c1.plotly_chart(px.bar(s26, x="segment", y="nb_clients", color="montant_moy_mad",
                               title="S26 — Segments RFM (couleur = montant moyen)",
                               labels={"segment": "Segment", "nb_clients": "Clients",
                                       "montant_moy_mad": "Montant moyen (MAD)"}), use_container_width=True)
    s28 = read(SPARK / "s28_entonnoir.csv")
    if available(s28, "S28"):
        steps = s28.melt(id_vars="category", value_vars=["vues", "ajouts_panier", "achats"],
                         var_name="etape", value_name="nombre")
        c2.plotly_chart(px.bar(steps, x="category", y="nombre", color="etape", barmode="group",
                               title="S28 — Entonnoir vues → panier → achat par catégorie",
                               labels={"category": "Catégorie", "nombre": "Événements", "etape": "Étape"}),
                        use_container_width=True)
    c3, c4 = st.columns(2)
    s29 = read(SPARK / "s29_cohortes.csv")
    if available(s29, "S29"):
        c3.plotly_chart(px.line(s29, x="cohorte", y="valeur_moy_client_mad", markers=True,
                                title="S29 — Valeur moyenne d'un client par cohorte d'inscription",
                                labels={"cohorte": "Mois d'inscription", "valeur_moy_client_mad": "Valeur (MAD)"}),
                        use_container_width=True)
    s30 = read(SPARK / "s30_croissance.csv")
    if available(s30, "S30"):
        c4.plotly_chart(px.line(s30, x="mois", y="ca_mad", color="categorie",
                                title="S30 — CA mensuel par catégorie",
                                labels={"mois": "Mois", "ca_mad": "CA (MAD)", "categorie": "Catégorie"}),
                        use_container_width=True)
    s27 = read(SPARK / "s27_regles.csv")
    if available(s27, "S27"):
        st.subheader("S27 — Règles d'association (FP-Growth), triées par lift")
        st.dataframe(s27, use_container_width=True, hide_index=True)

# ------------------------------------------------------------------ onglet 3 : performances
with tab_perf:
    allr, timings = read(BENCH / "all.csv"), read(SPARK / "timings.csv")
    if available(allr, "bench/results/all.csv"):
        native = last_runs(allr)
        native = native[native["variant"] == "native"]
        st.plotly_chart(px.bar(native, x="query", y="median_ms", hover_data=["p95_ms", "description"],
                               log_y=True, title="Latence médiane des requêtes natives (échelle log)",
                               labels={"query": "Requête", "median_ms": "Médiane (ms)"}), use_container_width=True)
        if timings is not None:
            both = pd.concat([last_runs(allr), last_runs(timings)])
            cmp = both[both["query"].isin(["A16", "A17", "A19"])]
            c1, c2 = st.columns(2)
            c1.plotly_chart(px.bar(cmp, x="query", y="median_ms", color="variant", barmode="group", log_y=True,
                                   title="Natif vs Spark (médiane, hors démarrage de session)",
                                   labels={"query": "Requête", "median_ms": "Médiane (ms)", "variant": "Exécution"}),
                            use_container_width=True)
            sjobs = last_runs(timings)
            sjobs = sjobs[sjobs["query"].str.startswith("S")]
            c2.plotly_chart(px.bar(sjobs, x="query", y="median_ms", hover_data=["description"],
                                   title="Durée des traitements Spark S26–S30",
                                   labels={"query": "Requête", "median_ms": "Durée (ms)"}), use_container_width=True)
    c3, c4 = st.columns(2)
    cons = read(BENCH / "consistency_summary.csv")
    if available(cons, "consistency_summary.csv"):
        c3.plotly_chart(px.bar(cons, x="cl", y="median_ms", color="query", barmode="group", facet_col="note",
                               category_orders={"cl": ["ONE", "QUORUM", "ALL"],
                                                "note": ["3_noeuds", "1_arrete", "2_arretes"]},
                               title="Cohérence : latence médiane par niveau et par situation",
                               labels={"cl": "Niveau", "median_ms": "Médiane (ms)"}), use_container_width=True)
        c3.dataframe(cons.pivot_table(index="query", columns=["note", "cl"], values="erreurs_%"),
                     use_container_width=True)
    idx = read(BENCH / "index_summary.csv")
    if available(idx, "index_summary.csv"):
        long = idx.melt(id_vars="requete", value_vars=["median_ms_sai", "median_ms_sans_index"],
                        var_name="variante", value_name="median_ms")
        c4.plotly_chart(px.bar(long, x="requete", y="median_ms", color="variante", barmode="group",
                               title="Indexation : index SAI vs scan avec ALLOW FILTERING",
                               labels={"requete": "Requête", "median_ms": "Médiane (ms)"}), use_container_width=True)