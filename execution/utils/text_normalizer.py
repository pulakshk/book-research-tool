#!/usr/bin/env python3
"""
Text Normalization and Fuzzy Matching Utilities
Provides robust text normalization for book titles and authors to improve matching accuracy
"""
import re
import unicodedata
from typing import Dict, Tuple, Optional
from difflib import SequenceMatcher


class TextNormalizer:
    """Handles text normalization with multiple levels of strictness"""
    
    # Special character mappings for normalization
    CHAR_MAPPINGS = {
        '–': '-',  # em-dash to hyphen
        '—': '-',  # em-dash to hyphen
        ''': "'",  # smart quote to straight quote
        ''': "'",
        '"': '"',  # smart double quotes
        '"': '"',
        '…': '...',  # ellipsis
        '&': 'and',
    }
    
    # Series indicator patterns
    SERIES_PATTERNS = [
        r'\(([^)]+(?:Series|Saga|Trilogy|Duology))\s*(?:#|Book)?\s*(\d+)?\)',  # (Series Name #1)
        r'\(([^)]+)\s+(?:#|Book)\s*(\d+(?:\.\d+)?)\)',  # (Series #1) or (Series Book 1)
        r':\s*(?:Book|#)\s*(\d+(?:\.\d+)?)',  # : Book 1
        r'-\s*(?:Book|#)\s*(\d+(?:\.\d+)?)',  # - Book 1
        r'\(#(\d+(?:\.\d+)?)\)',  # (#1)
    ]
    
    # Subtitle patterns
    SUBTITLE_PATTERNS = [
        r':\s*(.+)$',  # Everything after colon
        r'–\s*(.+)$',  # Everything after em-dash
        r'—\s*(.+)$',  # Everything after long dash
    ]
    
    @classmethod
    def normalize_unicode(cls, text: str) -> str:
        """Normalize unicode characters to consistent form"""
        if not text:
            return ""
        # NFD normalization + filter out combining marks
        normalized = unicodedata.normalize('NFD', text)
        # Remove combining characters but keep base characters
        return ''.join(c for c in normalized if not unicodedata.combining(c))
    
    @classmethod
    def apply_char_mappings(cls, text: str) -> str:
        """Apply special character mappings"""
        for original, replacement in cls.CHAR_MAPPINGS.items():
            text = text.replace(original, replacement)
        return text
    
    @classmethod
    def extract_series_info(cls, text: str) -> Dict[str, Optional[str]]:
        """
        Extract series name and book number from title
        Returns: {'series_name': str, 'book_number': str, 'cleaned_title': str}
        """
        if not text:
            return {'series_name': None, 'book_number': None, 'cleaned_title': text}
        
        series_name = None
        book_number = None
        cleaned = text
        
        for pattern in cls.SERIES_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) >= 2 and groups[0] and groups[1]:
                    # Pattern with both series name and number
                    series_name = groups[0].strip()
                    book_number = groups[1].strip()
                elif len(groups) >= 1 and groups[0]:
                    # Pattern with only number or only series
                    if groups[0].replace('.', '').isdigit():
                        book_number = groups[0].strip()
                    else:
                        series_name = groups[0].strip()
                        if len(groups) > 1 and groups[1]:
                            book_number = groups[1].strip()
                
                # Remove the matched series info from title
                cleaned = text[:match.start()].strip() + ' ' + text[match.end():].strip()
                cleaned = re.sub(r'\s+', ' ', cleaned).strip()
                break
        
        return {
            'series_name': series_name,
            'book_number': book_number,
            'cleaned_title': cleaned
        }
    
    @classmethod
    def extract_subtitle(cls, text: str) -> Tuple[str, Optional[str]]:
        """
        Extract subtitle from title
        Returns: (main_title, subtitle)
        """
        if not text:
            return text, None
        
        for pattern in cls.SUBTITLE_PATTERNS:
            match = re.search(pattern, text)
            if match:
                main_title = text[:match.start()].strip()
                subtitle = match.group(1).strip() if len(match.groups()) > 0 else None
                return main_title, subtitle
        
        return text, None
    
    @classmethod
    def normalize_title(cls, title: str, level: str = 'standard') -> Dict[str, str]:
        """
        Normalize a book title with specified level of aggressiveness
        
        Levels:
        - 'strict': Exact match - minimal normalization (unicode + trim)
        - 'standard': Balanced - remove special chars, normalize spaces
        - 'fuzzy': Aggressive - alphanumeric only, lowercase
        
        Returns dict with:
        - original: Original title
        - strict: Minimally normalized
        - standard: Balanced normalization
        - fuzzy: Aggressive normalization for fuzzy matching
        - core_title: Title without subtitle
        - series_name: Extracted series name (if any)
        - book_number: Extracted book number (if any)
        """
        if not title or str(title).lower() in ['nan', 'none', '']:
            return {
                'original': title,
                'strict': '',
                'standard': '',
                'fuzzy': '',
                'core_title': '',
                'series_name': None,
                'book_number': None
            }
        
        original = str(title).strip()
        
        # Step 1: Unicode normalization
        text = cls.normalize_unicode(original)
        
        # Step 2: Apply character mappings
        text = cls.apply_char_mappings(text)
        
        # Step 3: Extract series info
        series_info = cls.extract_series_info(text)
        text_no_series = series_info['cleaned_title']
        
        # Step 4: Extract subtitle
        core_title, subtitle = cls.extract_subtitle(text_no_series)
        
        # Strict normalization: just unicode + trim
        strict = cls.normalize_unicode(original).strip()
        
        # Standard normalization: remove excess whitespace, normalize punctuation
        standard = text_no_series
        standard = re.sub(r'[<>{}[\]\\|]', '', standard)  # Remove problematic chars
        standard = re.sub(r'\s+', ' ', standard).strip()
        
        # Fuzzy normalization: alphanumeric only, lowercase
        fuzzy = re.sub(r'[^a-z0-9\s]', '', standard.lower())
        fuzzy = re.sub(r'\s+', '', fuzzy)  # Remove all spaces for fuzzy matching
        
        return {
            'original': original,
            'strict': strict,
            'standard': standard,
            'fuzzy': fuzzy,
            'core_title': core_title.strip(),
            'series_name': series_info['series_name'],
            'book_number': series_info['book_number']
        }
    
    @classmethod
    def normalize_author(cls, author: str, level: str = 'standard') -> str:
        """
        Normalize author name
        
        Handles:
        - Unicode normalization
        - Trim whitespace
        - Remove common suffixes (Jr., Sr., III, etc.)
        - Fuzzy: lowercase, alphanumeric only
        """
        if not author or str(author).lower() in ['nan', 'none', '']:
            return ''
        
        text = str(author).strip()
        
        # Unicode normalization
        text = cls.normalize_unicode(text)
        
        # Remove common suffixes
        text = re.sub(r',?\s+(Jr\.?|Sr\.?|III?|IV|Ph\.?D\.?|M\.?D\.?)$', '', text, flags=re.IGNORECASE)
        
        # Remove "by " prefix if present
        text = re.sub(r'^by\s+', '', text, flags=re.IGNORECASE)
        
        if level == 'fuzzy':
            # Lowercase, alphanumeric only
            text = re.sub(r'[^a-z0-9\s]', '', text.lower())
            text = re.sub(r'\s+', '', text)
        else:
            # Standard: just clean up whitespace
            text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    @staticmethod
    def levenshtein_distance(s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings"""
        if len(s1) < len(s2):
            return TextNormalizer.levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                # j+1 instead of j since previous_row and current_row are one character longer than s2
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    @classmethod
    def calculate_similarity(cls, str1: str, str2: str) -> float:
        """
        Calculate similarity score between two strings (0.0 to 1.0)
        Uses both SequenceMatcher and Levenshtein for robustness
        """
        if not str1 or not str2:
            return 0.0
        
        # Use difflib's SequenceMatcher for quick ratio
        ratio1 = SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
        
        # Use Levenshtein for edit distance ratio
        max_len = max(len(str1), len(str2))
        if max_len == 0:
            return 1.0
        lev_dist = cls.levenshtein_distance(str1.lower(), str2.lower())
        ratio2 = 1.0 - (lev_dist / max_len)
        
        # Return average of both methods
        return (ratio1 + ratio2) / 2.0
    
    @classmethod
    def fuzzy_match_titles(cls, title1: str, title2: str, threshold: float = 0.85) -> bool:
        """
        Check if two titles match using fuzzy matching
        
        Args:
            title1: First title
            title2: Second title  
            threshold: Similarity threshold (0.0 to 1.0)
        
        Returns:
            True if titles are similar enough to be considered a match
        """
        norm1 = cls.normalize_title(title1)
        norm2 = cls.normalize_title(title2)
        
        # Try different levels of matching
        # 1. Exact match on fuzzy normalized
        if norm1['fuzzy'] and norm2['fuzzy'] and norm1['fuzzy'] == norm2['fuzzy']:
            return True
        
        # 2. High similarity on standard normalized
        if norm1['standard'] and norm2['standard']:
            similarity = cls.calculate_similarity(norm1['standard'], norm2['standard'])
            if similarity >= threshold:
                return True
        
        # 3. Core title match (without subtitle)
        if norm1['core_title'] and norm2['core_title']:
            similarity = cls.calculate_similarity(norm1['core_title'], norm2['core_title'])
            if similarity >= threshold:
                return True
        
        return False
    
    @classmethod
    def fuzzy_match_authors(cls, author1: str, author2: str, threshold: float = 0.90) -> bool:
        """
        Check if two author names match using fuzzy matching
        Higher threshold than titles since author names are typically shorter
        """
        norm1 = cls.normalize_author(author1, level='fuzzy')
        norm2 = cls.normalize_author(author2, level='fuzzy')
        
        if not norm1 or not norm2:
            return False
        
        # Exact match on normalized
        if norm1 == norm2:
            return True
        
        # Fuzzy match
        similarity = cls.calculate_similarity(norm1, norm2)
        return similarity >= threshold


# Convenience functions for backward compatibility and ease of use
def normalize_title(title: str, level: str = 'standard') -> Dict[str, str]:
    """Normalize a book title - convenience wrapper"""
    return TextNormalizer.normalize_title(title, level)


def normalize_author(author: str, level: str = 'standard') -> str:
    """Normalize an author name - convenience wrapper"""
    return TextNormalizer.normalize_author(author, level)


def fuzzy_match_titles(title1: str, title2: str, threshold: float = 0.85) -> bool:
    """Check if two titles match - convenience wrapper"""
    return TextNormalizer.fuzzy_match_titles(title1, title2, threshold)


def fuzzy_match_authors(author1: str, author2: str, threshold: float = 0.90) -> bool:
    """Check if two authors match - convenience wrapper"""
    return TextNormalizer.fuzzy_match_authors(author1, author2, threshold)


def calculate_similarity(str1: str, str2: str) -> float:
    """Calculate similarity score - convenience wrapper"""
    return TextNormalizer.calculate_similarity(str1, str2)


if __name__ == "__main__":
    # Test cases
    print("Testing TextNormalizer...")
    print("=" * 80)
    
    # Test title normalization
    test_titles = [
        "Pucking Around (Jacksonville Rays, #1)",
        "Pucking Around",
        "The Deal: A Hockey Romance",
        "The Deal",
        "Book Title – Subtitle",
        "Book Title - Subtitle"
    ]
    
    print("\nTitle Normalization Tests:")
    for title in test_titles:
        result = normalize_title(title)
        print(f"\nOriginal: {result['original']}")
        print(f"  Standard: {result['standard']}")
        print(f"  Fuzzy: {result['fuzzy']}")
        print(f"  Core: {result['core_title']}")
        print(f"  Series: {result['series_name']} #{result['book_number']}")
    
    # Test fuzzy matching
    print("\n" + "=" * 80)
    print("Fuzzy Matching Tests:")
    
    pairs = [
        ("Pucking Around (Jacksonville Rays, #1)", "Pucking Around"),
        ("The Deal: A Hockey Romance", "The Deal"),
        ("Book Title – Subtitle", "Book Title - Subtitle"),
        ("Similar Title", "Completely Different")
    ]
    
    for t1, t2 in pairs:
        match = fuzzy_match_titles(t1, t2)
        similarity = TextNormalizer.calculate_similarity(
            normalize_title(t1)['standard'],
            normalize_title(t2)['standard']
        )
        print(f"\n'{t1}' vs '{t2}'")
        print(f"  Match: {match}, Similarity: {similarity:.2f}")
