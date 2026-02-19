import pandas as pd
import numpy as np
from loguru import logger

FILE = "book_series_analysis.csv"

def main():
    logger.info(f"Loading {FILE}...")
    df = pd.read_csv(FILE)
    
    # Comprehensive manual data based on user's table
    manual_data = [
        {
            "Book Series Name": "Treasure State Wildcats", "Author Name": "Devney Perry", "Books in Series": 4, 
            "First Book Rating": 3.94, "First Book Rating Count": 57958, "Commissioning_Rank": "P0", "Self Pub Flag": "Indie",
            "Books_Featured_Rank_Validation": "Coach, Blitz, Rally, Merit", "Total Pages": None
        },
        {
            "Book Series Name": "Waylon University", "Author Name": "Ilsa Madden-Mills", "Books in Series": 2, 
            "First Book Rating": 4.03, "First Book Rating Count": 18836, "Commissioning_Rank": "P0", "Self Pub Flag": "Indie",
            "Books_Featured_Rank_Validation": "I Bet You, I Hate You", "Total Pages": None
        },
        {
            "Book Series Name": "Fallen Crest High", "Author Name": "Tijan", "Books in Series": 15, 
            "First Book Rating": 4.29, "First Book Rating Count": 12234, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Books_Featured_Rank_Validation": "Fallen Crest High (Goodreads Choice Finalist)", "Total Pages": 300
        },
        {
            "Book Series Name": "The Curvy Girl Club", "Author Name": "Kelsie Stelting", "Books in Series": 14, 
            "First Book Rating": 3.96, "First Book Rating Count": 16756, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": None
        },
        {
            "Book Series Name": "Rules of the Game", "Author Name": "Avery Keelan", "Books in Series": 9, 
            "First Book Rating": 3.88, "First Book Rating Count": 27418, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Books_Featured_Rank_Validation": "Offside Hearts, Offside, Breakaway Hearts...", "Total Pages": 820
        },
        {
            "Book Series Name": "The Cocky Kingmans", "Author Name": "Amy Award", "Books in Series": 8, 
            "First Book Rating": 3.72, "First Book Rating Count": 55199, "Commissioning_Rank": "P1", "Self Pub Flag": "Self-Pub",
            "Total Pages": None
        },
        {
            "Book Series Name": "Real", "Author Name": "Katy Evans", "Books in Series": 7, 
            "First Book Rating": 4.11, "First Book Rating Count": 114622, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": 310
        },
        {
            "Book Series Name": "Red Zone Rivals", "Author Name": "Elsie Silver", "Books in Series": 6, 
            "First Book Rating": 3.7, "First Book Rating Count": 180633, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Books_Featured_Rank_Validation": "Off to the Races (#56 - Sports)", "Total Pages": 727
        },
        {
            "Book Series Name": "Breakers Hockey", "Author Name": "Stephanie Garber", "Books in Series": 5, 
            "First Book Rating": 4.06, "First Book Rating Count": 666432, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": 428
        },
        {
            "Book Series Name": "Fulton U", "Author Name": "Maya Hughes", "Books in Series": 5, 
            "First Book Rating": 3.83, "First Book Rating Count": 29199, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": 399
        },
        {
            "Book Series Name": "Dare Series", "Author Name": "Shantel Tessier", "Books in Series": 4, 
            "First Book Rating": 4.16, "First Book Rating Count": 22011, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": 431
        },
        {
            "Book Series Name": "Denver Dragons Series", "Author Name": "Madi Danielle", "Books in Series": 4, 
            "First Book Rating": 3.69, "First Book Rating Count": 9073, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": 339
        },
        {
            "Book Series Name": "Fairfield U", "Author Name": "G.N. Wright", "Books in Series": 4, 
            "First Book Rating": 4, "First Book Rating Count": 18177, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Books_Featured_Rank_Validation": "The Puck Player (#77 - Hockey)", "Total Pages": 391
        },
        {
            "Book Series Name": "Lakeside University", "Author Name": "Avery Keelan", "Books in Series": 4, 
            "First Book Rating": 3.79, "First Book Rating Count": 32110, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": 378
        },
        {
            "Book Series Name": "Landry Family", "Author Name": "Adriana Locke", "Books in Series": 4, 
            "First Book Rating": 4.07, "First Book Rating Count": 5909, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": 535
        },
        {
            "Book Series Name": "Washington Wolves", "Author Name": "Karla Sorensen", "Books in Series": 4, 
            "First Book Rating": 3.85, "First Book Rating Count": 24718, "Commissioning_Rank": "P1", "Self Pub Flag": "Self Pub",
            "Total Pages": 85
        },
        {
            "Book Series Name": "Indy Speed Hockey", "Author Name": "Siena Trap", "Books in Series": 3, 
            "First Book Rating": 3.96, "First Book Rating Count": 2829, "Commissioning_Rank": "P1", "Self Pub Flag": "Self Pub",
            "Books_Featured_Rank_Validation": "Surprise for the Sniper (#18 - Hockey Top 100); Frozen Heart Face-Off (#27)", "Total Pages": 450
        },
        {
            "Book Series Name": "Puffin Books", "Author Name": "Richard Adams", "Books in Series": 3, 
            "First Book Rating": 4.09, "First Book Rating Count": 506135, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": 443
        },
        {
            "Book Series Name": "Seattle Phantom Football", "Author Name": "Maggie Rawdon", "Books in Series": 3, 
            "First Book Rating": 4.1, "First Book Rating Count": 145, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": 990
        },
        {
            "Book Series Name": "The Atlanta Vipers Series", "Author Name": "Eliza Peake", "Books in Series": 3, 
            "First Book Rating": 3.92, "First Book Rating Count": 250, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": None
        },
        {
            "Book Series Name": "The Ottawa Otters", "Author Name": "K.M. Gillis", "Books in Series": 3, 
            "First Book Rating": 4.03, "First Book Rating Count": 9476, "Commissioning_Rank": "P1", "Self Pub Flag": "Self Pub",
            "Books_Featured_Rank_Validation": "O Goalie Night (#34 - Hockey); Check The Halls (#29 - Hockey)", "Total Pages": None
        },
        {
            "Book Series Name": "The Remington Royals", "Author Name": "Siena Trap", "Books in Series": 3, 
            "First Book Rating": 3.92, "First Book Rating Count": 1337, "Commissioning_Rank": "P1", "Self Pub Flag": "Self Pub",
            "Books_Featured_Rank_Validation": "Scoring the Princess (#62 - Hockey)", "Total Pages": None
        },
        {
            "Book Series Name": "Breaking the Ice", "Author Name": "Kandi Steiner", "Books in Series": 2, 
            "First Book Rating": 3.83, "First Book Rating Count": 22432, "Commissioning_Rank": "P1", "Self Pub Flag": "Indie",
            "Total Pages": 401
        },
        {
            "Book Series Name": "The Players", "Author Name": "Monica Murphy", "Books in Series": 2, 
            "First Book Rating": 3.74, "First Book Rating Count": 31926, "Commissioning_Rank": "P1", "Self Pub Flag": "Self Pub",
            "Books_Featured_Rank_Validation": "Playing Hard to Get (#28 - Sports)", "Total Pages": None
        },
        {
            "Book Series Name": "A Scoring Chance Series", "Author Name": "Marie M.", "Books in Series": 1, 
            "First Book Rating": 4.11, "First Book Rating Count": 4093, "Commissioning_Rank": "P1", "Self Pub Flag": "Self Pub",
            "Total Pages": 394
        }
    ]
    
    # Injection Logic
    logger.info("Injecting validated manual series data (Rankings & Pages)...")
    for item in manual_data:
        mask = df['Book Series Name'] == item['Book Series Name']
        if mask.any():
            idx = df[mask].index[0]
            for key, val in item.items():
                df.at[idx, key] = val
            logger.info(f"Synchronized Rank Details: {item['Book Series Name']}")
        else:
            # Add new if not present
            new_row = {col: np.nan for col in df.columns}
            new_row.update(item)
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            logger.info(f"Added New: {item['Book Series Name']}")

    # Save
    df.to_csv(FILE, index=False)
    logger.success(f"Final Data Confirmation Complete for {FILE}")

if __name__ == "__main__":
    main()
