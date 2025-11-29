import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import plotly.graph_objects as go
import plotly.express as px

# --------------------------------------------------
# 0. Paramètres généraux
# --------------------------------------------------

# Fichier de métadonnées (même dossier que app.py)
META_PATH = "AGROECO_Metadata_Questions.xlsx"

# Colonnes de contexte attendues dans la base brute Kobo
ID_COLS = ["country", "actor_category", "respondent_index"]

# Ordre « logique » des dimensions PRINCIPALES (sans collaboration)
DIM_MAIN = ["env", "eco", "pol", "terr", "temp"]
# Liste complète (si besoin ailleurs)
DIM_ALL = ["env", "eco", "pol", "terr", "temp", "collab"]


# --------------------------------------------------
# 1. Fonctions utilitaires
# --------------------------------------------------

@st.cache_data
def load_metadata(meta_path: str) -> pd.DataFrame:
    """Charge le fichier de métadonnées des questions AGRO ECO."""
    meta_df = pd.read_excel(meta_path, sheet_name="questions")
    return meta_df


def mean_excluding_zero(x: pd.Series) -> float:
    """Moyenne en excluant les 0 (0 = ne sait pas / non-réponse)."""
    x = x.replace(0, np.nan)
    return x.mean()


def ensure_context_columns(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    S'assure que les colonnes de contexte 'country', 'actor_category'
    et 'respondent_index' existent dans la base brute.

    - country : si absent, on essaie de l'inférer à partir de 'territory'
                (par ex. 'Burkina Faso - Territoire_1' → 'Burkina Faso').
                Sinon, on met 'Pays unique'.
    - respondent_index : si absent, on numérote les répondants de 1 à N.
    - actor_category : si absente, on met 'Catégorie inconnue'.
    """
    df = raw_df.copy()

    # country
    if "country" not in df.columns:
        if "territory" in df.columns:
            countries = (
                df["territory"]
                .astype(str)
                .str.extract(r"^(.*?)(?:\s*-\s*|$)")[0]
                .str.strip()
            )
            df["country"] = countries.replace("", "Pays unique")
        else:
            df["country"] = "Pays unique"

    # respondent_index
    if "respondent_index" not in df.columns:
        df["respondent_index"] = np.arange(1, len(df) + 1)

    # actor_category
    if "actor_category" not in df.columns:
        df["actor_category"] = "Catégorie inconnue"

    return df


def run_analysis(raw_df: pd.DataFrame, meta_df: pd.DataFrame):
    """
    Applique la logique de l’outil AGRO ECO :
    - harmonisation des colonnes de contexte
    - passage en long
    - jointure avec les métadonnées
    - agrégations
    Retourne :
    - tous_les_resultats
    - resume_par_categorie
    - resume_par_pays
    """

    # Harmoniser / créer les colonnes de contexte
    raw_df = ensure_context_columns(raw_df)

    # Vérifier que les colonnes de contexte sont bien là
    for col in ID_COLS:
        if col not in raw_df.columns:
            raise ValueError(f"Colonne de contexte manquante dans la base brute : {col}")

    # Toutes les variables d’indicateurs définies dans les métadonnées
    indicator_vars = meta_df["var_name"].dropna().unique().tolist()

    # Garantir que toutes les colonnes d'indicateurs existent
    for v in indicator_vars:
        if v not in raw_df.columns:
            raw_df[v] = np.nan

    # Conversion en numérique pour toutes les colonnes d’indicateurs
    raw_df[indicator_vars] = raw_df[indicator_vars].apply(
        pd.to_numeric, errors="coerce"
    )

    # Passage en long : une ligne = 1 répondant × 1 indicateur
    long_df = raw_df.melt(
        id_vars=ID_COLS,
        value_vars=indicator_vars,
        var_name="var_name",
        value_name="value"
    )

    # Jointure avec les métadonnées
    meta_subset = meta_df[
        ["var_name", "dimension_code", "dimension_label",
         "question_index", "label", "hint"]
    ]
    long_df = long_df.merge(meta_subset, on="var_name", how="left")

    # --------------------------------------------------
    # Table 1 – Tous les résultats par indicateur
    # --------------------------------------------------
    tous_les_resultats = (
        long_df
        .groupby(
            ["country",
             "actor_category",
             "dimension_label",
             "dimension_code",
             "var_name",
             "question_index",
             "label"],
            dropna=False
        )["value"]
        .apply(mean_excluding_zero)
        .reset_index(name="mean_score")
    )

    tous_les_resultats = tous_les_resultats.sort_values(
        by=["country", "actor_category",
            "dimension_code", "question_index"]
    )

    # --------------------------------------------------
    # Table 2 – Résumé par dimension et catégorie d’acteurs
    # --------------------------------------------------
    resume_par_categorie = (
        tous_les_resultats
        .groupby(
            ["country",
             "actor_category",
             "dimension_label",
             "dimension_code"],
            dropna=False
        )["mean_score"]
        .mean()
        .reset_index(name="dimension_mean")
    )

    resume_par_categorie = resume_par_categorie.sort_values(
        by=["country", "actor_category", "dimension_code"]
    )

    # --------------------------------------------------
    # Table 3 – Résumé par dimension et pays (tous acteurs confondus)
    # --------------------------------------------------
    resume_par_pays = (
        tous_les_resultats
        .groupby(
            ["country",
             "dimension_label",
             "dimension_code"],
            dropna=False
        )["mean_score"]
        .mean()
        .reset_index(name="dimension_mean")
    )

    resume_par_pays = resume_par_pays.sort_values(
        by=["country", "dimension_code"]
    )

    return tous_les_resultats, resume_par_categorie, resume_par_pays


def build_excel_bytes(tous_les_resultats: pd.DataFrame,
                      resume_par_categorie: pd.DataFrame,
                      resume_par_pays: pd.DataFrame) -> bytes:
    """
    Construit un fichier Excel en mémoire avec les trois tables de résultats.
    Retourne les bytes pour téléchargement.
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        tous_les_resultats.to_excel(
            writer, sheet_name="Tous_les_resultats", index=False
        )
        resume_par_categorie.to_excel(
            writer, sheet_name="Resume_par_categorie", index=False
        )
        resume_par_pays.to_excel(
            writer, sheet_name="Resume_par_pays", index=False
        )

    output.seek(0)
    return output.getvalue()


# --------------------------------------------------
# 2. Fonctions pour les graphiques (type Excel + complémentaires)
# --------------------------------------------------

def order_dimensions(df: pd.DataFrame, main_only: bool = True) -> pd.DataFrame:
    """
    Ordonne les dimensions.
    - si main_only=True : uniquement les 5 dimensions principales (sans collaboration)
    - sinon : on garde l'ordre DIM_ALL
    """
    if "dimension_code" not in df.columns:
        return df
    df = df.copy()
    if main_only:
        cats = DIM_MAIN
    else:
        cats = DIM_ALL
    cat = pd.Categorical(df["dimension_code"], categories=cats, ordered=True)
    df["dimension_code"] = cat
    df = df.sort_values("dimension_code")
    return df


def radar_par_pays(resume_par_pays: pd.DataFrame, country: str):
    """
    Radar des scores moyens par dimension pour un pays (profil global de transition).
    Ne prend en compte que les 5 dimensions principales (sans collaboration).
    """
    dfc = resume_par_pays[
        (resume_par_pays["country"] == country)
        & (resume_par_pays["dimension_code"].isin(DIM_MAIN))
    ].copy()
    if dfc.empty:
        return None

    dfc = order_dimensions(dfc, main_only=True)

    labels = dfc["dimension_label"].tolist()
    values = dfc["dimension_mean"].fillna(0).tolist()

    # fermer le polygone
    labels += labels[:1]
    values += values[:1]

    fig = go.Figure(
        data=go.Scatterpolar(
            r=values,
            theta=labels,
            fill="toself",
            name=country
        )
    )
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 5]
            )
        ),
        showlegend=False,
        margin=dict(l=40, r=40, t=40, b=40),
        title=f"Profil global de transition – {country}"
    )
    return fig


def radar_par_categorie(resume_par_categorie: pd.DataFrame, country: str):
    """
    Radar multi-traces par catégorie d'acteurs pour un pays.
    Ne prend en compte que les 5 dimensions principales (sans collaboration).
    """
    dfc = resume_par_categorie[
        (resume_par_categorie["country"] == country)
        & (resume_par_categorie["dimension_code"].isin(DIM_MAIN))
    ].copy()
    if dfc.empty:
        return None

    dfc = order_dimensions(dfc, main_only=True)

    categories = dfc["actor_category"].dropna().unique().tolist()
    dim_labels = (
        dfc[["dimension_code", "dimension_label"]]
        .drop_duplicates()
        .sort_values("dimension_code")
    )["dimension_label"].tolist()

    fig = go.Figure()
    for actor in categories:
        dfa = dfc[dfc["actor_category"] == actor]
        dfa = order_dimensions(dfa, main_only=True)
        vals = dfa["dimension_mean"].fillna(0).tolist()
        t = dim_labels + dim_labels[:1]
        r = vals + vals[:1]
        fig.add_trace(
            go.Scatterpolar(
                r=r,
                theta=t,
                fill="toself",
                name=str(actor)
            )
        )

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 5]
            )
        ),
        showlegend=True,
        margin=dict(l=40, r=40, t=40, b=40),
        title=f"Profils par catégorie d'acteurs – {country}"
    )
    return fig


def bar_dimensions_par_pays(resume_par_pays: pd.DataFrame):
    """
    Barres comparatives par dimension et par pays,
    uniquement pour les 5 dimensions principales (sans collaboration).
    """
    df = resume_par_pays[resume_par_pays["dimension_code"].isin(DIM_MAIN)].copy()
    df = order_dimensions(df, main_only=True)
    fig = px.bar(
        df,
        x="dimension_label",
        y="dimension_mean",
        color="country",
        barmode="group",
        labels={
            "dimension_label": "Dimension",
            "dimension_mean": "Score moyen (0–5)",
            "country": "Pays"
        },
        title="Comparaison des dimensions de la transition par pays"
    )
    fig.update_yaxes(range=[0, 5])
    return fig


def bar_dimensions_par_categorie(resume_par_categorie: pd.DataFrame, country: str):
    """
    Barres comparatives par dimension et catégorie d'acteurs, pour un pays,
    uniquement pour les 5 dimensions principales (sans collaboration).
    """
    dfc = resume_par_categorie[
        (resume_par_categorie["country"] == country)
        & (resume_par_categorie["dimension_code"].isin(DIM_MAIN))
    ].copy()
    if dfc.empty:
        return None
    dfc = order_dimensions(dfc, main_only=True)
    fig = px.bar(
        dfc,
        x="dimension_label",
        y="dimension_mean",
        color="actor_category",
        barmode="group",
        labels={
            "dimension_label": "Dimension",
            "dimension_mean": "Score moyen (0–5)",
            "actor_category": "Catégorie d'acteurs"
        },
        title=f"Dimensions de la transition par catégorie d'acteurs – {country}"
    )
    fig.update_yaxes(range=[0, 5])
    return fig


def scatter_env_eco(resume_par_categorie: pd.DataFrame):
    """
    Graphique complémentaire : relation entre dimension environnementale et économique
    (par pays et catégorie d'acteurs).
    """
    df = resume_par_categorie.copy()
    df = df[df["dimension_code"].isin(["env", "eco"])]

    pivot = df.pivot_table(
        index=["country", "actor_category"],
        columns="dimension_code",
        values="dimension_mean"
    ).reset_index()

    if "env" not in pivot.columns or "eco" not in pivot.columns:
        return None

    fig = px.scatter(
        pivot,
        x="env",
        y="eco",
        color="country",
        symbol="actor_category",
        labels={
            "env": "Score environnemental moyen",
            "eco": "Score économique moyen",
            "country": "Pays",
            "actor_category": "Catégorie d'acteurs"
        },
        title="Relations entre performances environnementales et économiques"
    )
    fig.update_xaxes(range=[0, 5])
    fig.update_yaxes(range=[0, 5])
    return fig


def tableau_synthese_dimensions(resume_par_categorie: pd.DataFrame, country: str = None):
    """
    Construit un tableau de type « Résumé des résultats » :
    lignes = dimensions principales (sans collaboration)
    colonnes = catégories d'acteurs
    - si country est None : tous pays confondus
    - sinon : filtré sur un pays
    """
    df = resume_par_categorie.copy()
    if country is not None:
        df = df[df["country"] == country]

    df = df[df["dimension_code"].isin(DIM_MAIN)].copy()

    if df.empty:
        return None

    df = (
        df
        .groupby(["dimension_label", "dimension_code", "actor_category"], dropna=False)["dimension_mean"]
        .mean()
        .reset_index()
    )

    table = df.pivot_table(
        index=["dimension_label", "dimension_code"],
        columns="actor_category",
        values="dimension_mean"
    )

    ordered_index = []
    for code in DIM_MAIN:
        mask = (table.index.get_level_values("dimension_code") == code)
        if mask.any():
            for idx in table.index[mask]:
                ordered_index.append(idx)

    if ordered_index:
        table = table.loc[ordered_index]

    return table


# --------------------------------------------------
# 3. Interface Streamlit
# --------------------------------------------------

st.set_page_config(
    page_title="AGRO ECO – Analyse automatique",
    layout="wide"
)

st.title("AGRO ECO – Analyse automatique des données Kobo")

st.markdown(
    """
    Cet outil permet d’analyser automatiquement une **base brute Kobo** 
    issue du questionnaire AGRO ECO, et de produire des résultats 
    identiques (moyennes par indicateur et par dimension) à la version Excel de l’outil.
    
    **Étapes :**
    1. Téléverser la base brute (Excel) téléchargée depuis KoboCollect.  
    2. Vérifier l’aperçu.  
    3. Lancer l’analyse.  
    4. Visualiser les tableaux et les graphiques.  
    5. Télécharger le fichier de résultats (Excel).
    """
)

# Charger les métadonnées (une seule fois)
try:
    meta_df = load_metadata(META_PATH)
except Exception as e:
    st.error(f"Erreur lors du chargement des métadonnées ({META_PATH}) : {e}")
    st.stop()

uploaded_file = st.file_uploader(
    "Téléverser la base brute Kobo (Excel)",
    type=["xlsx", "xls"]
)

if uploaded_file is not None:
    try:
        raw_df = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Erreur lors de la lecture du fichier Excel : {e}")
        st.stop()

    st.subheader("Aperçu de la base brute")
    st.dataframe(raw_df.head())

    if st.button("Lancer l'analyse AGRO ECO"):
        try:
            tous_les_resultats, resume_par_categorie, resume_par_pays = run_analysis(
                raw_df, meta_df
            )
        except Exception as e:
            st.error(f"Erreur lors de l'analyse : {e}")
            st.stop()

        st.success("Analyse terminée.")

        # --------------------------
        # TABLEAUX BRUTS
        # --------------------------
        st.subheader("Résumé par dimension et par pays")
        st.dataframe(resume_par_pays)

        st.subheader("Résumé par dimension, pays et catégorie d'acteurs")
        st.dataframe(resume_par_categorie)

        st.subheader("Tous les résultats (par indicateur, pays, catégorie)")
        st.dataframe(tous_les_resultats)

        # --------------------------
        # TABLEAUX TYPE « RÉSUMÉ DES RÉSULTATS »
        # --------------------------
        st.markdown("## Tableaux de synthèse – type « Résumé des résultats »")

        table_global = tableau_synthese_dimensions(resume_par_categorie, country=None)
        if table_global is not None:
            st.markdown("### Tableau global (tous pays confondus)")
            st.dataframe(table_global)

        countries = resume_par_pays["country"].dropna().unique().tolist()
        for country in countries:
            table_country = tableau_synthese_dimensions(resume_par_categorie, country=country)
            if table_country is not None:
                st.markdown(f"### {country}")
                st.dataframe(table_country)

        # --------------------------
        # GRAPHIQUES – type Excel
        # --------------------------
        st.markdown("## Graphiques – Profils globaux de transition (radars)")

        for country in countries:
            fig_radar = radar_par_pays(resume_par_pays, country)
            if fig_radar is not None:
                st.plotly_chart(fig_radar, use_container_width=True)

        st.markdown("## Graphiques – Profils par catégorie d'acteurs (radars)")

        for country in countries:
            fig_rcat = radar_par_categorie(resume_par_categorie, country)
            if fig_rcat is not None:
                st.plotly_chart(fig_rcat, use_container_width=True)

        # --------------------------
        # GRAPHIQUES – complémentaires
        # --------------------------
        st.markdown("## Graphiques complémentaires")

        fig_bar_pays = bar_dimensions_par_pays(resume_par_pays)
        st.plotly_chart(fig_bar_pays, use_container_width=True)

        for country in countries:
            fig_bar_cat = bar_dimensions_par_categorie(resume_par_categorie, country)
            if fig_bar_cat is not None:
                st.plotly_chart(fig_bar_cat, use_container_width=True)

        fig_scatter = scatter_env_eco(resume_par_categorie)
        if fig_scatter is not None:
            st.plotly_chart(fig_scatter, use_container_width=True)

        # Export Excel
        excel_bytes = build_excel_bytes(
            tous_les_resultats, resume_par_categorie, resume_par_pays
        )

        st.download_button(
            label="📥 Télécharger le fichier de résultats (Excel)",
            data=excel_bytes,
            file_name="AGROECO_Results_from_Kobo.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
else:
    st.info("Veuillez téléverser un fichier Excel brut exporté de KoboCollect.")


