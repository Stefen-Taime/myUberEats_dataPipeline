import pandas as pd
import hashlib

# Function to generate a unique ID based on the given value
def generate_id(value):
    return hashlib.md5(value.encode()).hexdigest()

# Function to clean and extract date information
def clean_and_extract_date_info(df):
    # Convert 'DateTime' column to datetime type assuming it's in the format 'dd/mm/yyyy hh:mm'
    df['DateTime'] = pd.to_datetime(df['DateTime'], format='%d/%m/%Y %H:%M', errors='coerce', dayfirst=True)
    df.dropna(subset=['DateTime'], inplace=True)  # Drop rows where 'DateTime' is NaN
    
    # Extract components from 'DateTime'
    df['Jour'] = df['DateTime'].dt.day
    df['Mois'] = df['DateTime'].dt.month
    df['Annee'] = df['DateTime'].dt.year
    df['Heure'] = df['DateTime'].dt.hour
    df['Minute'] = df['DateTime'].dt.minute
    
    # Create a unique ID for each date-time combination
    df['DateHeure'] = df.apply(
        lambda row: generate_id(f"{row['Jour']}/{row['Mois']}/{row['Annee']} {row['Heure']}:{row['Minute']}"), axis=1
    )
    
    # Select relevant columns for Date/Time dimension
    date_df = df[['DateHeure', 'Jour', 'Mois', 'Annee', 'Heure', 'Minute']].drop_duplicates()

    return date_df


