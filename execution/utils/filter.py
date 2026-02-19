
import re
from loguru import logger

# Specialized Keywords by Sport (including common variations)
HOCKEY_KEYWORDS = [
    'hockey', 'puck', 'ice', 'skate', 'rink', 'nhl', 'ahl', 'slapshot', 
    'hat trick', 'face-off', 'penalty box', 'goalie', 'skating', 'stick',
    'center', 'enforcer', 'defense', 'shorthanded', 'breakaway', 'netminder',
    'icing', 'body check', 'power play', 'zamboni', 'pucking', 'pucked', 'pucker',
    'net', 'goal', 'comets', 'reapers', 'blades', 'varsity', 'campus', 'puck-shy'
]

FOOTBALL_KEYWORDS = [
    'football', 'nfl', 'quarterback', 'qb', 'wide receiver', 'touchdown', 'gridiron',
    'halftime', 'super bowl', 'linebacker', 'tight end', 'offensive line', 'interception',
    'lineback', 'touchdowns'
]

BASEBALL_KEYWORDS = [
    'baseball', 'mlb', 'pitcher', 'batter', 'home run', 'dugout', 'diamond',
    'world series', 'strikeout', 'shortstop', 'catcher', 'fastball', 'curveball',
    'homerun'
]

BASKETBALL_KEYWORDS = [
    'basketball', 'nba', 'dunk', 'hoops', 'point guard', 'slam dunk', 'court',
    'layup', 'three-pointer', 'rebound', 'ncaa', 'march madness'
]

OTHER_SPORTS_KEYWORDS = [
    'soccer', 'racing', 'f1', 'nascar', 'tennis', 'boxer', 'boxing', 'mma', 'ufc',
    'wrestling', 'swimming', 'olympic', 'gold medal', 'track and field', 'rugby',
    'golf', 'caddy', 'formula 1', 'sharks', 'titans', 'raiders', 'wildcat', 'wolves',
    'storm', 'rays', 'rebels', 'wild', 'knights', 'avalanche', 'vipers', 'mustangs',
    'bruins', 'oilers', 'leafs', 'panthers', 'warriors', 'jets', 'kings', 'ducks',
    'courage', 'nights', 'wolves', 'tigers', 'lions', 'bears', 'bulldogs'
]

ROMANCE_DRAMA_KEYWORDS = [
    'romance', 'love', 'kiss', 'passion', 'relationship', 'dating', 'crush',
    'heart', 'sexy', 'spicy', 'steamy', 'drama', 'rivalry', 'rivals', 
    'teammate', 'jock', 'forbidden', 'contemporary', 'billionaire', 'grumpy',
    'sunshine', 'slow burn', 'enemies to lovers', 'friends to lovers', 
    'fake dating', 'single dad', 'forced proximity'
]

GENERAL_KEYWORDS = [
    'sports', 'player', 'coach', 'team', 'athlete', 'score', 'mvp', 
    'rookie', 'captain', 'league', 'training', 'season', 'game',
    'championship', 'cup', 'athlete', 'pro', 'draft', 'match',
    'locker room', 'stadium', 'arena', 'varsity', 'collegiate',
    'athletic', 'tournament', 'playoff'
]

# Negative Keywords to exclude non-fiction, memoirs, guides, etc.
NON_FICTION_KEYWORDS = [
    'non-fiction', 'nonfiction', 'biography', 'memoir', 'autobiography', 
    'true story', 'how to', 'guide', 'manual', 'handbook', 'technique',
    'tutorial', 'history of', 'documentary', 'encyclopedia', 'collection of essays',
    'journalism', 'report', 'statistics', 'stats', 'almanac', 'coaching guide',
    'training manual', 'fitness guide', 'drills', 'workout', 'rules of'
]

ALL_SPORTS = HOCKEY_KEYWORDS + FOOTBALL_KEYWORDS + BASEBALL_KEYWORDS + BASKETBALL_KEYWORDS + OTHER_SPORTS_KEYWORDS + GENERAL_KEYWORDS

def is_sports_hockey_related(text: str, metadata: dict = None) -> bool:
    """
    Checks if a book or series is related to sports romance/drama.
    Strictly filters out non-fiction and non-sports.
    """
    if not text and not metadata:
        return False
    
    # Combine all searchable text
    search_space = str(text).lower()
    if metadata:
        for val in metadata.values():
            if val:
                search_space += " " + str(val).lower()
    
    # 1. Check for Non-Fiction (Immediate Disqualification)
    for kw in NON_FICTION_KEYWORDS:
        if re.search(r'\b' + re.escape(kw) + r'\b', search_space):
            return False

    # 2. Must contain at least one Sport keyword
    has_sport = False
    for kw in ALL_SPORTS:
        if ' ' in kw:
            if kw in search_space:
                has_sport = True
                break
        else:
            # Use a more lenient check (not just word boundary) for sport roots
            # but still avoid some false positives
            if re.search(re.escape(kw), search_space):
                has_sport = True
                break
    
    if not has_sport:
        return False
        
    # 3. Strength Check: Does it look like fiction/romance?
    # For titles, we are lenient. For descriptions, we expect drama/romance vibes.
    has_drama = False
    for kw in ROMANCE_DRAMA_KEYWORDS:
        if ' ' in kw:
            if kw in search_space:
                has_drama = True
                break
        else:
            if re.search(re.escape(kw), search_space):
                has_drama = True
                break
                
    # If we have a very long description (indicating metadata is present), 
    # we should check for drama keywords to be sure it's not a dry stats book.
    # Exception: if the title itself is very clearly sports-romance (jock, coach, puck), we allow it.
    if len(search_space) > 150:
        if not has_drama:
            # Check for generic romance indicators if explicit keywords are missing
            if 'romance' in search_space or 'novel' in search_space or 'story' in search_space:
                return True
            return False

    return True

def filter_dataframe_by_relevance(df):
    """
    Filters a dataframe to keep only sports romance/drama rows.
    """
    initial_len = len(df)
    
    def check_row(row):
        metadata = {
            'title': row.get('Book Name', ''),
            'series': row.get('Series Name', ''),
            'desc': row.get('Description', ''),
            'trope': row.get('Primary Trope', ''),
            'subgenre': row.get('Primary Subgenre', '')
        }
        return is_sports_hockey_related("", metadata)

    mask = df.apply(check_row, axis=1)
    filtered_df = df[mask].copy()
    
    removed = initial_len - len(filtered_df)
    if removed > 0:
        logger.info(f"Filtered out {removed} non-fiction or non-sports/drama books.")
    
    return filtered_df
