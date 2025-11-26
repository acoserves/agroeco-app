import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO

# --------------------------------------------------
# 0. Paramètres généraux
# --------------------------------------------------

META_PATH = "AGROECO_Metadata_Questions.xlsx"  # fichier de métadonnées à placer à côté de app.py

# Colonnes de contexte attendues dans la base brute Kobo
ID_COLS = ["country", "actor_category", "respondent_index"]


# --------------------------------------------------
# 1. Fonctions utilitaires
# --------------------------------------------------

@st.cache_data
def load_metadata(meta_path: str) -> pd.DataFrame:
    """Charge le fichier de métadonnées des questions."""
    meta_df = pd.read_excel(meta_path, sheet_name="questions")
    return meta_df


def mean_excluding_zero(x: pd.Series) -> float:
    """Moyenne en excluant les 0 (0 = ne sait pas / non-réponse)."""
    x = x.replace(0, np.nan)
    return x.mean()


def run_analysis(raw_df: pd.DataFrame, meta_df: pd.DataFrame):
    """
    Applique la logique AGRO ECO :
    - passage en long
    - jointure avec les métadonnées
    - agrégations
    Retourne :
    - tous_les_resultats
    - resume_par_categorie
    - resume_par_pays
    """

    # Vérifier que les colonnes de contexte sont là
    for col in ID_COLS:
        if col not in raw_df.columns:
            raise ValueError(f"Colonne de contexte manquante dans la base brute : {col}")

    # Liste des variables d’indicateurs issues des métadonnées
    indicator_vars = meta_df["var_name"].dropna().unique().tolist()

    # Garder seulement celles qui existent effectivement dans la base
    indicator_vars = [v for v in indicator_vars if v in raw_df.columns]

    if len(indicator_vars) == 0:
        raise ValueError("Aucune variable d’indicateur trouvée dans la base brute.")

    # Conversion en numérique
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
# 2. Interface Streamlit
# --------------------------------------------------

st.set_page_config(
    page_title="AGRO ECO / QTAAE – Analyse automatique",
    layout="wide"
)

st.title("AGRO ECO / QTAAE – Analyse automatique des données Kobo")

st.markdown(
    """
    Cet outil permet d’analyser automatiquement une **base brute Kobo** 
    issue du questionnaire AGRO ECO / QTAAE, et de produire des résultats 
    identiques (moyennes par indicateur et par dimension) à la version Excel de l’outil.
    
    **Étapes :**
    1. Téléverser la base brute (Excel) téléchargée depuis KoboCollect.  
    2. Vérifier l’aperçu.  
    3. Lancer l’analyse.  
    4. Visualiser les tableaux et quelques graphiques.  
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

    # Bouton pour lancer l'analyse
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
        # TABLEAUX
        # --------------------------
        st.subheader("Résumé par dimension et par pays")
        st.dataframe(resume_par_pays)

        st.subheader("Résumé par dimension, pays et catégorie d'acteurs")
        st.dataframe(resume_par_categorie)

        st.subheader("Tous les résultats (par indicateur, pays, catégorie)")
        st.dataframe(tous_les_resultats)

        # --------------------------
        # GRAPHIQUES – PAR PAYS
        # --------------------------
        st.markdown("## Graphiques – Dimensions par pays")

        # Tableau croisé: lignes = dimensions, colonnes = pays
        pivot_pays = resume_par_pays.pivot(
            index="dimension_label",
            columns="country",
            values="dimension_mean"
        )
        st.bar_chart(pivot_pays)

        # --------------------------
        # GRAPHIQUES – PAR CATÉGORIE ET PAR PAYS
        # --------------------------
        st.markdown("## Graphiques – Dimensions par catégorie d'acteurs et par pays")

        countries = resume_par_pays["country"].dropna().unique().tolist()
        for country in countries:
            st.markdown(f"### {country}")
            dfc = resume_par_categorie[resume_par_categorie["country"] == country]
            if not dfc.empty:
                pivot_cat = dfc.pivot(
                    index="actor_category",
                    columns="dimension_label",
                    values="dimension_mean"
                ).sort_index()
                st.bar_chart(pivot_cat)

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
