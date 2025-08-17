import pandas as pd

# Read the generated CSV file
df = pd.read_csv('bc_hospitals_from_wikipedia.csv')

# Show basic info about the data
print(f"Total hospitals collected: {len(df)}")
print(f"Columns: {list(df.columns)}")
print("\nFirst few rows:")
print(df.head())

print("\nHospitals with bed count data:")
beds_data = df[df['Beds'].notna()]
print(f"Found bed data for {len(beds_data)} hospitals")
if len(beds_data) > 0:
    print(beds_data[['Facility Name', 'Beds', 'Health Authority']].head())