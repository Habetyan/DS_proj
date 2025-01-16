import pandas as pd
import plotly.express as px
import streamlit as st
import numpy as np
from sklearn.preprocessing import StandardScaler
from umap import UMAP
import warnings
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

warnings.filterwarnings("ignore")

# Load the data
@st.cache_data
def load_data():
    df = pd.read_csv("output.csv")
    df1 = pd.read_csv("last_output.csv")
    return df,df1


df,df1 = load_data()

# --- Sidebar for Team Selection ---
st.sidebar.header("Team Statistics")
selected_team = st.sidebar.selectbox("Select a Team", df["team"].sort_values())


# --- Filter data for selected team ---
team_data = df[df["team"] == selected_team].iloc[0]  # Get the row as a Series
team_data_changed = df1[df1["team"] == selected_team].iloc[0]  # Get the row as a Series

# --- Main content ---
st.title("Football Team Performance Analysis")

# --- Team Statistics Panel ---
st.sidebar.subheader(f"Statistics for {selected_team}")

# Basic Stats
st.sidebar.markdown(f"**Favorite Tactics:** {team_data['favorite_tactics']}")
st.sidebar.markdown(f"**Market Value:** {team_data_changed['Market_Value']}")
st.sidebar.markdown(f"**Similar Teams:** {str(team_data_changed['cluster_members']).strip("[]").replace("'","")}")
st.sidebar.markdown(f"**Squad Size:** {team_data['squad_size']}")
st.sidebar.markdown(f"**Points (Last 5):** {team_data['points_last_5']}")
st.sidebar.markdown(f"**Points (Last 10):** {team_data['points_last_10']}")
st.sidebar.markdown(f"**Goals:** {team_data['goals']}")
st.sidebar.markdown(f"**Possession:** {team_data['possession']}%")
st.sidebar.markdown(f"**Pass Accuracy:** {team_data['pass%']}%")
st.sidebar.markdown(f"**Aerials Won:** {team_data['aerialswon']}")
st.sidebar.markdown(f"**Rating:** {team_data['rating']}")

# --- Comparison with Other Teams ---
st.sidebar.subheader("Comparison (Points Last 5)")

# 1. Sort by the metric
df_sorted = df.sort_values("points_last_5", ascending=False).reset_index(drop=True)

# 2. Find the index of the selected team
selected_team_index = df_sorted[df_sorted["team"] == selected_team].index[0]

# 3. Slice to get the 2 higher and 2 lower teams
higher_teams = df_sorted.iloc[max(0, selected_team_index - 2):selected_team_index]
lower_teams = df_sorted.iloc[selected_team_index + 1:min(selected_team_index + 3, len(df_sorted))]

# Concatenate and display
comparison_df = pd.concat([higher_teams, lower_teams])

if not comparison_df.empty:
    st.sidebar.dataframe(comparison_df[["team", "points_last_5"]].sort_values("points_last_5", ascending=False))
else:
    st.sidebar.write("No teams to compare.")

# --- Add more comparisons based on other statistics ---
# Example: Goals per shot
st.sidebar.subheader("Comparison (Normal Goals per Shot)")
df["normal_goals_per_shot"] = df["normal_goals"] / df["normal_shots"]

# 1. Sort by the metric (Normal Goals per Shot)
df_sorted = df.sort_values("normal_goals_per_shot", ascending=False).reset_index(drop=True)

# 2. Find the index of the selected team
selected_team_index = df_sorted[df_sorted["team"] == selected_team].index[0]

# 3. Slice to get the 2 higher and 2 lower teams
higher_teams = df_sorted.iloc[max(0, selected_team_index - 2):selected_team_index]
lower_teams = df_sorted.iloc[selected_team_index + 1:min(selected_team_index + 3, len(df_sorted))]

comparison_df = pd.concat([higher_teams, lower_teams])

if not comparison_df.empty:
    st.sidebar.dataframe(
        comparison_df[["team", "normal_goals_per_shot"]].sort_values("normal_goals_per_shot", ascending=False))
else:
    st.sidebar.write("No teams to compare.")

# --- New Analysis and Visualizations ---

# 1. Possession vs. Points
fig = px.scatter(df, x="possession", y="points_last_10", color="team",
                 hover_data=["team", "possession", "points_last_10"],
                 title="Possession vs. Points (Last 10)")
st.plotly_chart(fig)

# 2. Pass Accuracy vs. Goals
fig = px.scatter(df, x="pass%", y="goals", color="team",
                 hover_data=["team", "pass%", "goals"],
                 title="Pass Accuracy vs. Goals")
st.plotly_chart(fig)

# 3. Aerials Won vs. Rating
fig = px.scatter(df, x="aerialswon", y="rating", color="team",
                 hover_data=["team", "aerialswon", "rating"],
                 title="Aerials Won vs. Team Rating")
st.plotly_chart(fig)

# 4. Correlation Heatmap
st.subheader("Correlation Heatmap")
corr_df = df[["points_last_5", "points_last_10", "goals", "shots pg", "possession", "pass%", "aerialswon", "rating"]]
corr_matrix = corr_df.corr()
fig = px.imshow(corr_matrix, text_auto=True, aspect="auto", color_continuous_scale="RdYlGn")
st.plotly_chart(fig)

# --- Rest of your analysis and visualization code ---
# (Shooting Efficiency, Expected Goals, etc. - from previous examples)
# ...
# --- Sidebar ---
analysis_type = st.sidebar.selectbox("Select Analysis Type", [
    "Shooting Efficiency",
    "Expected Goals (xG)",
    "Shot Type Distribution",
    "Tactical Analysis",
    "Squad and Form",
    "UMAP Visualization"
])

if analysis_type == "Shooting Efficiency":
    st.header("Shooting Efficiency")
    shot_type = st.sidebar.selectbox("Select Shot Type", ["normal", "standard", "slow", "fast"])

    # Scatter plot with Plotly
    fig = px.scatter(df, x=f"{shot_type}_shots", y=f"{shot_type}_goals", color="team",
                     hover_data=["team", f"{shot_type}_shots", f"{shot_type}_goals"],
                     title=f"{shot_type.capitalize()} Goals vs. {shot_type.capitalize()} Shots")
    st.plotly_chart(fig)

    # Goals per shot calculation and bar chart
    df[f"{shot_type}_goals_per_shot"] = df[f"{shot_type}_goals"] / df[f"{shot_type}_shots"]
    fig = px.bar(df, x="team", y=f"{shot_type}_goals_per_shot",
                 title=f"{shot_type.capitalize()} Goals per Shot")
    fig.update_xaxes(categoryorder='total descending')  # Sort bars
    st.plotly_chart(fig)

elif analysis_type == "Expected Goals (xG)":
    # ... (Similar modifications for other sections using Plotly)
    st.header("Expected Goals (xG) Analysis")

    # Select shot type
    shot_type = st.sidebar.selectbox("Select Shot Type", ["normal", "standard", "slow", "fast"])

    # Scatter plot: Goals vs. xG
    fig = px.scatter(df, x=f"{shot_type}_xg", y=f"{shot_type}_goals", color="team",
                     hover_data=["team", f"{shot_type}_xg", f"{shot_type}_goals"],
                     title=f"{shot_type.capitalize()} Goals vs. {shot_type.capitalize()} xG")
    st.plotly_chart(fig)

    # Goals - xG calculation and bar chart
    df[f"{shot_type}_goals_minus_xg"] = df[f"{shot_type}_goals"] - df[f"{shot_type}_xg"]

    fig = px.bar(df, x="team", y=f"{shot_type}_goals_minus_xg",
                 title=f"{shot_type.capitalize()} Goals - xG")
    fig.update_xaxes(categoryorder='total descending')
    st.plotly_chart(fig)

elif analysis_type == "Shot Type Distribution":
    st.header("Shot Type Distribution")

    # Stacked bar chart
    shot_types = ["normal", "standard", "slow", "fast"]
    shot_data = df[["team"] + [f"{t}_shots" for t in shot_types]]
    shot_data = shot_data.set_index("team")

    fig = px.bar(shot_data, barmode="stack", title="Shot Type Distribution")
    st.plotly_chart(fig)

elif analysis_type == "Tactical Analysis":
    st.header("Tactical Analysis")

    # Favorite tactics frequency
    fig = px.bar(df["favorite_tactics"].value_counts(), title="Favorite Tactics Frequency")
    st.plotly_chart(fig)

    # Box plot: Points vs. Tactics
    fig = px.box(df, x="favorite_tactics", y="points_last_10", title="Points vs. Tactics")
    st.plotly_chart(fig)

elif analysis_type == "Squad and Form":
    st.header("Squad and Form")

    # Scatter plot: Squad Size vs. Points
    fig = px.scatter(df, x="squad_size", y="points_last_10", color="team",
                     hover_data=["team", "squad_size", "points_last_10"],
                     title="Squad Size vs. Points")
    st.plotly_chart(fig)

    # Bar chart: Points comparison
    fig = px.bar(df, x="team", y=["points_last_5", "points_last_10"],
                 title="Points Comparison (Last 5 and Last 10 Games)")
    fig.update_xaxes(categoryorder='total descending', type='category')
    st.plotly_chart(fig)

elif analysis_type == "UMAP Visualization":
    # df = df.iloc[:, :-4]
    st.header("UMAP Visualization of EPL Teams with K-Means Clustering")

    numeric_features = df.select_dtypes(include='number')
    numeric_features.fillna(numeric_features.mean(), inplace=True)

    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(numeric_features)

    # K-Means Clustering
    num_clusters = 6
    kmeans = KMeans(n_clusters=num_clusters, random_state=42)
    df['Cluster'] = kmeans.fit_predict(scaled_features)
    silhouette_avg = silhouette_score(scaled_features, df['Cluster'])
    st.write(f"Silhouette Score: {silhouette_avg:.2f}")

    # --- 3D UMAP Visualization ---
    umap_3d = UMAP(n_components=3, random_state=42)
    umap_embedding_3d = umap_3d.fit_transform(scaled_features)

    df['UMAP_1_3d'] = umap_embedding_3d[:, 0]
    df['UMAP_2_3d'] = umap_embedding_3d[:, 1]
    df['UMAP_3_3d'] = umap_embedding_3d[:, 2]

    fig_3d = px.scatter_3d(
        df,
        x='UMAP_1_3d',
        y='UMAP_2_3d',
        z='UMAP_3_3d',
        color=df['Cluster'].astype(str),
        text='team',
        title='Clusters on Original Data Visualized in 3D with UMAP',
        labels={'UMAP_1_3d': 'UMAP Dimension 1', 'UMAP_2_3d': 'UMAP Dimension 2', 'UMAP_3_3d': 'UMAP Dimension 3'}
    )

    fig_3d.update_traces(marker=dict(size=6, opacity=0.8))
    fig_3d.update_layout(legend_title="Cluster")

    st.plotly_chart(fig_3d)

    # --- 2D UMAP Visualization ---
    # elif analysis_type == "UMAP Visualization":
    # ... (rest of the code is the same) ...

    # --- 2D UMAP Visualization ---
    umap_2d = UMAP(n_components=2, random_state=42)
    umap_embedding_2d = umap_2d.fit_transform(scaled_features)

    df['UMAP_1'] = umap_embedding_2d[:, 0]
    df['UMAP_2'] = umap_embedding_2d[:, 1]

    fig_2d = px.scatter(
        df,
        x='UMAP_1',
        y='UMAP_2',
        color=df['Cluster'].astype(str),
        text='team',
        title='Clusters on Original Data Visualized in 2D with UMAP',
        labels={'UMAP_1': 'UMAP Dimension 1', 'UMAP_2': 'UMAP Dimension 2', 'color': 'Cluster'}
    )

    fig_2d.update_traces(marker=dict(size=6, opacity=0.8),
                         textposition='top center')  # Position text above the point

    fig_2d.update_layout(legend_title="Cluster")

    st.plotly_chart(fig_2d)