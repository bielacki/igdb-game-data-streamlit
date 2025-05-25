import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import polars as pl
import math
from datetime import date
import json

# --- CONFIG ---
PROJECT_ID = "course-data-engineering"
DATASET = "igdb_dwh"
TABLE = "games_dashboard"
ROWS_PER_PAGE = 20

# --- AUTHENTICATION ---
credentials_dict = st.secrets["streamlit-sa"]
credentials = service_account.Credentials.from_service_account_info(credentials_dict)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# --- CACHE FULL DATASET ONCE A DAY ---
@st.cache_data(show_spinner="Loading and caching full dataset...", ttl=86400)
def load_full_dataset():
    query = f"""
    select id, name, cover_url, rating, rating_count, igdb_url, first_release_date, hypes, dlt_load_timestamp
    from `{PROJECT_ID}.{DATASET}.{TABLE}`
    """
    return pl.DataFrame(client.query(query).to_dataframe())

def get_max_hypes(df: pl.DataFrame) -> int:
    return df.filter(pl.col("first_release_date") > date.today()).get_column("hypes").max()

def format_last_update(df: pl.DataFrame) -> str:
    last_update = df.get_column("dlt_load_timestamp").max()
    return last_update.strftime("%Y-%m-%d %H:%M UTC")

def get_games_number(df: pl.DataFrame) -> int:
    return df.get_column("id").count()

def get_ratings_count(df: pl.DataFrame) -> int:
    return df.get_column("rating_count").sum()

# --- FILTER AND PAGINATE ---
def filter_data(df: pl.DataFrame, search: str, rating_range: tuple, min_count: int,
                sort_by: str, order: str) -> pl.DataFrame:
    filtered = df.filter(
        (pl.col("name").str.to_lowercase().str.contains(search.lower())) &
        (pl.col("rating") >= rating_range[0]) &
        (pl.col("rating") <= rating_range[1]) &
        (pl.col("rating_count") >= min_count)
    )
    if order == "ASC":
        filtered = filtered.sort(sort_by)
    else:
        filtered = filtered.sort(sort_by, descending=True)
    return filtered

def filter_upcoming_data(df: pl.DataFrame, search: str, min_hypes: int, max_hypes: int, 
                        sort_by: str = "Release Date", order: str = "Ascending") -> pl.DataFrame:
    filtered = df.filter(
        (pl.col("first_release_date") > date.today()) &
        (pl.col("name").str.to_lowercase().str.contains(search.lower())) &
        (pl.col("hypes") >= min_hypes) &
        (pl.col("hypes") <= max_hypes)
    )
    
    # Sort based on selected option with order
    descending = order == "Descending"
    if sort_by == "Release Date":
        return filtered.sort("first_release_date", descending=descending)
    else:  # sort by hypes
        return filtered.sort("hypes", descending=descending)

def paginate(df: pl.DataFrame, page: int, rows_per_page: int = ROWS_PER_PAGE) -> pl.DataFrame:
    start = (page - 1) * rows_per_page
    return df.slice(start, rows_per_page)

# --- STYLE ---
st.markdown("""
    <style>
    .game-card {
        border-radius: 12px;
        padding: 11px;
        text-align: left;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        transition: transform 0.1s ease-in-out;
    }
    .game-card:hover {
        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
        transform: scale(1.03);
        transition: transform 0.2s;
    }
    .game-title {
        font-size: 0.85rem;
        font-weight: 600;
        margin-top: 8px;
        color: #9147ff;
        display: -webkit-box;
        -webkit-line-clamp: 2;
        -webkit-box-orient: vertical;
        overflow: hidden;
        min-height: 3.4em;
    }
    .game-meta {
        font-size: 0.75rem;
        color: #2b2b2b;
    }
    </style>
""", unsafe_allow_html=True)

# --- LOAD DATA ---
full_df = load_full_dataset()

last_update = format_last_update(full_df)
total_games_count = get_games_number(full_df)
ratings_count = get_ratings_count(full_df)

# --- SIDEBAR HEADER ---
st.logo("igdb_logo.svg")
st.sidebar.markdown("""
    <div style='margin-bottom: 10px;'>
        <div style='font-size: 1.5em; font-weight: 600; color: #9147ff; margin-bottom: 5px;'>
            IGDB Game stats
        </div>
        <div style='font-size: 0.8em; color: #666;'>   
            This application was developed as part of the <a href="https://github.com/DataTalksClub/data-engineering-zoomcamp/" target="_blank">Data Engineering Zoomcamp</a>.
                    <br>
                    <br>
            Powered by data from the <a href="https://api-docs.igdb.com/#getting-started" target="_blank">IGDB API</a>.
                    <br>
                    <br>
            Code is available on <a href="https://github.com/bielacki/igdb-game-data" target="_blank">GitHub</a>.
        </div>
    </div>
""", unsafe_allow_html=True)

st.sidebar.markdown(f"""
    <div style='font-size: 0.8em; color: #666; margin-top: 10px;padding-bottom: 10px; border-bottom: 1px solid #eee;'>
        <div>📅 Last updated: <span style='font-weight: 600'>{last_update}</span></div>
        <div>🎮 Games in the database: <span style='font-weight: 600'>{total_games_count}</span></div>
        <div>💬 Number of user ratings: <span style='font-weight: 600'>{ratings_count}</span></div>
    </div>
""", unsafe_allow_html=True)

# --- SIDEBAR TAB SELECTION ---
tab_choice = st.sidebar.radio("Select View", ["🚀 Upcoming Releases", "🔥 Popular Games"], index=0)

if tab_choice == "🔥 Popular Games":
    # --- SIDEBAR FILTERS FOR POPULAR ---
    search = st.sidebar.text_input("🔍 Search Title", key="search_popular")
    rating_range = st.sidebar.slider(
        "⭐ Rating Range", 
        0, 
        100, 
        (0, 100),  # default values (min, max)
        step=1, 
        key="rating_range_popular"
    )
    min_count = st.sidebar.number_input("💬 Minimum Ratings Count", min_value=0, value=0, step=10, key="min_count_popular")
    sort_by = st.sidebar.selectbox("📊 Sort By", ["rating_count", "rating"], index=0, key="sort_by_popular")
    order = "DESC" if st.sidebar.radio("⬇️ Order", ["Descending", "Ascending"], key="order_popular") == "Descending" else "ASC"

elif tab_choice == "🚀 Upcoming Releases":
    # --- SIDEBAR FILTERS FOR UPCOMING ---
    search_upcoming = st.sidebar.text_input("🔍 Search Title", key="search_upcoming")
    max_hypes = get_max_hypes(full_df)
    hypes_range = st.sidebar.slider(
        "🔥 Hypes Range", 
        0, 
        int(max_hypes), 
        (0, int(max_hypes)), 
        step=1,
        key="hypes_range"
    )
    sort_by_upcoming = st.sidebar.selectbox(
        "📊 Sort By", 
        ["Hypes", "Release Date"], 
        index=0, 
        key="sort_by_upcoming"
    )
    order_upcoming = st.sidebar.radio("⬇️ Order", ["Descending", "Ascending"], key="order_upcoming")

# --- POPULAR GAMES VIEW ---
if tab_choice == "🔥 Popular Games":
    filtered_df = filter_data(full_df, search, rating_range, min_count, sort_by, order)
    total_games = filtered_df.shape[0]
    total_pages = max(1, math.ceil(total_games / ROWS_PER_PAGE))

    st.markdown("### 🎨 Popular Games")

    st.markdown("Top IGDB games by users rating and popularity.")

    if "page_num" not in st.session_state:
        st.session_state.page_num = 1

    # Pagination controls
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("⬅️ Previous", key="prev_popular", use_container_width=True) and st.session_state.page_num > 1:
            st.session_state.page_num -= 1
    with col3:
        if st.button("Next ➡️", key="next_popular", use_container_width=True) and st.session_state.page_num < total_pages:
            st.session_state.page_num += 1
    with col2:
        st.markdown(f"<div style='text-align:center;'>Page {st.session_state.page_num} of {total_pages}</div>", unsafe_allow_html=True)

    page_df = paginate(filtered_df, st.session_state.page_num)

    col_count = 4
    rows = page_df.shape[0] // col_count + int(page_df.shape[0] % col_count != 0)

    for i in range(rows):
        st.markdown('<div class="game-row">', unsafe_allow_html=True)
        cols = st.columns(col_count)
        for j in range(col_count):
            idx = i * col_count + j
            if idx < page_df.shape[0]:
                game = page_df.row(idx)
                with cols[j]:
                    st.markdown(f"""
                        <a href="{game[5]}" target="_blank" style="text-decoration: none;">
                            <div class="game-card">
                                <img src="{game[2]}" width="100%" style="border-radius: 8px; display: block; margin: 0 auto;">
                                <div class="game-title">{game[1]}</div>
                                <div class="game-meta">⭐ {game[3]:.1f} &nbsp;&nbsp; 💬 {game[4]}</div>
                            </div>
                        </a>
                    """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

# --- UPCOMING RELEASES VIEW ---
elif tab_choice == "🚀 Upcoming Releases":
    filtered_df = filter_upcoming_data(
        full_df, 
        search_upcoming, 
        hypes_range[0], 
        hypes_range[1],
        sort_by_upcoming,
        order_upcoming
    )
    total_games = filtered_df.shape[0]
    total_pages = max(1, math.ceil(total_games / ROWS_PER_PAGE))

    st.markdown("### 🚀 Upcoming Releases")

    st.markdown("Most anticipated games based on \"Hypes\" — the number of follows a game receives before release.")

    # Pagination state
    if "upcoming_page_num" not in st.session_state:
        st.session_state.upcoming_page_num = 1

    # Pagination controls
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("⬅️ Previous", key="prev_upcoming", use_container_width=True) and st.session_state.upcoming_page_num > 1:
            st.session_state.upcoming_page_num -= 1
    with col3:
        if st.button("Next ➡️", key="next_upcoming", use_container_width=True) and st.session_state.upcoming_page_num < total_pages:
            st.session_state.upcoming_page_num += 1
    with col2:
        st.markdown(f"<div style='text-align:center;'>Page {st.session_state.upcoming_page_num} of {total_pages}</div>", unsafe_allow_html=True)

    # Get paginated data
    page_df = paginate(filtered_df, st.session_state.upcoming_page_num)

    # Display grid
    col_count = 4
    rows = page_df.shape[0] // col_count + int(page_df.shape[0] % col_count != 0)

    for i in range(rows):
        st.markdown('<div class="game-row">', unsafe_allow_html=True)
        cols = st.columns(col_count)
        for j in range(col_count):
            idx = i * col_count + j
            if idx < page_df.shape[0]:
                game = page_df.row(idx)
                release_date = game[6].strftime("%b %d, %Y")
                with cols[j]:
                    st.markdown(f"""
                        <a href="{game[5]}" target="_blank" style="text-decoration: none;">
                            <div class="game-card">
                                <img src="{game[2]}" width="100%" style="border-radius: 8px; display: block; margin: 0 auto;">
                                <div class="game-title">{game[1]}</div>
                                <div class="game-meta">🚀 {release_date} &nbsp;&nbsp; 🔥 {game[7]}</div>
                            </div>
                        </a>
                    """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)