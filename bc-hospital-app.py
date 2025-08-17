import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium

# Page configuration
st.set_page_config(
    page_title="BC Hospital Dashboard",
    page_icon="🏥",
    layout="wide"
)

@st.cache_data
def load_data():
    """Load hospital data from CSV file"""
    try:
        df = pd.read_csv('bc_hospitals_from_wikipedia.csv')
        return df
    except FileNotFoundError:
        st.error("Hospital data file 'bc_hospitals_from_wikipedia.csv' not found. Please run the scraper first.")
        return None

def main():
    st.title("🏥 BC Hospital Dashboard")
    st.markdown("Explore hospital locations and information across British Columbia")
    
    # Load data
    df = load_data()
    if df is None:
        return
    
    # Clean data - remove rows without coordinates
    df = df.dropna(subset=['Latitude', 'Longitude'])
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Get unique health authorities
    health_authorities = sorted(df['Health Authority'].unique())
    selected_authority = st.sidebar.selectbox(
        "Select Health Authority",
        options=['All'] + health_authorities,
        index=0
    )
    
    # Filter data based on selection
    if selected_authority == 'All':
        filtered_df = df.copy()
    else:
        filtered_df = df[df['Health Authority'] == selected_authority].copy()
    
    # Summary metrics
    st.header("Summary")
    col1, col2 = st.columns(2)
    
    with col1:
        total_hospitals = len(filtered_df)
        st.metric("Total Hospitals", total_hospitals)
    
    with col2:
        # Calculate total beds (only count hospitals with bed data)
        beds_data = filtered_df[filtered_df['Beds'].notna()]
        total_beds = int(beds_data['Beds'].sum()) if not beds_data.empty else 0
        st.metric("Total Beds", f"{total_beds:,}")
    
    st.markdown("---")
    
    # Main content area
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("Hospital Locations")
        
        if not filtered_df.empty:
            # Calculate center point for map
            center_lat = filtered_df['Latitude'].mean()
            center_lon = filtered_df['Longitude'].mean()
            
            # Create map
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=8,
                tiles="OpenStreetMap"
            )
            
            # Add markers for each hospital
            for idx, hospital in filtered_df.iterrows():
                # Create popup text
                popup_text = f"""
                <b>{hospital['Facility Name']}</b><br>
                Location: {hospital['Location City']}<br>
                Health Authority: {hospital['Health Authority']}<br>
                Beds: {int(hospital['Beds']) if pd.notna(hospital['Beds']) else 'N/A'}
                """
                
                # Choose marker color based on bed count
                if pd.notna(hospital['Beds']):
                    beds = hospital['Beds']
                    if beds >= 200:
                        color = 'red'  # Large hospitals
                    elif beds >= 100:
                        color = 'orange'  # Medium hospitals
                    else:
                        color = 'green'  # Small hospitals
                else:
                    color = 'gray'  # Unknown bed count
                
                folium.Marker(
                    location=[hospital['Latitude'], hospital['Longitude']],
                    popup=folium.Popup(popup_text, max_width=300),
                    tooltip=hospital['Facility Name'],
                    icon=folium.Icon(color=color, icon='plus', prefix='fa')
                ).add_to(m)
            
            # Display map
            st_folium(m, width=500, height=400)
            
            # Legend
            st.markdown("""
            **Map Legend:**
            - 🔴 Large hospitals (200+ beds)
            - 🟠 Medium hospitals (100-199 beds)  
            - 🟢 Small hospitals (<100 beds)
            - ⚫ Bed count unknown
            """)
        else:
            st.info("No hospitals found for the selected health authority.")
    
    with col2:
        st.subheader("Hospital Details")
        
        if not filtered_df.empty:
            # Prepare data for display table
            display_df = filtered_df[['Facility Name', 'Location City', 'Beds']].copy()
            
            # Format beds column
            display_df['Beds'] = display_df['Beds'].apply(
                lambda x: f"{int(x)}" if pd.notna(x) else "N/A"
            )
            
            # Sort alphabetically by facility name
            display_df = display_df.sort_values('Facility Name')
            
            # Reset index for cleaner display
            display_df = display_df.reset_index(drop=True)
            
            # Display table
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Facility Name": "Hospital Name",
                    "Location City": "City",
                    "Beds": "Bed Count"
                }
            )
            
            # Additional stats
            st.markdown("### Statistics")
            hospitals_with_beds = filtered_df[filtered_df['Beds'].notna()]
            
            if not hospitals_with_beds.empty:
                avg_beds = hospitals_with_beds['Beds'].mean()
                max_beds = hospitals_with_beds['Beds'].max()
                min_beds = hospitals_with_beds['Beds'].min()
                
                stat_col1, stat_col2, stat_col3 = st.columns(3)
                with stat_col1:
                    st.metric("Avg Beds", f"{avg_beds:.0f}")
                with stat_col2:
                    st.metric("Largest", f"{int(max_beds)}")
                with stat_col3:
                    st.metric("Smallest", f"{int(min_beds)}")
        else:
            st.info("No hospital details to display.")
    
    # Footer
    st.markdown("---")
    st.markdown("*Data sourced from Wikipedia*")

if __name__ == "__main__":
    main()