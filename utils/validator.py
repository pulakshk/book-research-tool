#!/usr/bin/env python3
"""
Data Validation Utilities
Pre-flight validation checks for book datasets
"""
import pandas as pd
import numpy as np
import re
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

# Add parent to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config


class ValidationSeverity(Enum):
    """Severity levels for validation errors"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class ValidationError:
    """Represents a single validation error"""
    severity: ValidationSeverity
    field: str
    row_index: Optional[int]
    message: str
    value: Optional[str] = None
    suggested_fix: Optional[str] = None


@dataclass
class ValidationReport:
    """Complete validation report for a dataset"""
    total_rows: int
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    info: List[ValidationError] = field(default_factory=list)
    
    @property
    def error_count(self) -> int:
        return len([e for e in self.errors if e.severity == ValidationSeverity.ERROR or e.severity == ValidationSeverity.CRITICAL])
    
    @property
    def warning_count(self) -> int:
        return len([e for e in self.warnings if e.severity == ValidationSeverity.WARNING])
    
    @property
    def is_valid(self) -> bool:
        """Dataset is valid if there are no CRITICAL or ERROR level issues"""
        return self.error_count == 0
    
    def add_error(self, error: ValidationError):
        """Add an error to the appropriate list"""
        if error.severity == ValidationSeverity.INFO:
            self.info.append(error)
        elif error.severity == ValidationSeverity.WARNING:
            self.warnings.append(error)
        else:
            self.errors.append(error)
    
    def print_summary(self):
        """Print a summary of the validation report"""
        print(f"\n{'='*80}")
        print("VALIDATION REPORT")
        print(f"{'='*80}")
        print(f"Total Rows: {self.total_rows}")
        print(f"Errors: {self.error_count}")
        print(f"Warnings: {self.warning_count}")
        print(f"Info: {len(self.info)}")
        print(f"Status: {'✓ VALID' if self.is_valid else '✗ INVALID'}")
        print(f"{'='*80}\n")
        
        if self.errors:
            print("ERRORS:")
            for i, error in enumerate(self.errors[:10], 1):  # Show first 10
                print(f"{i}. [{error.severity.value}] {error.field}: {error.message}")
                if error.suggested_fix:
                    print(f"   Suggested fix: {error.suggested_fix}")
            if len(self.errors) > 10:
                print(f"   ... and {len(self.errors) - 10} more errors")
            print()
        
        if self.warnings:
            print("WARNINGS:")
            for i, warning in enumerate(self.warnings[:10], 1):  # Show first 10
                print(f"{i}. [{warning.severity.value}] {warning.field}: {warning.message}")
            if len(self.warnings) > 10:
                print(f"   ... and {len(self.warnings) - 10} more warnings")
            print()


class DataValidator:
    """Validates book datasets against defined rules"""
    
    @staticmethod
    def validate_required_fields(df: pd.DataFrame, report: ValidationReport):
        """Check that required fields are present and not null"""
        for field in config.REQUIRED_FIELDS:
            if field not in df.columns:
                report.add_error(ValidationError(
                    severity=ValidationSeverity.CRITICAL,
                    field=field,
                    row_index=None,
                    message=f"Required field '{field}' is missing from dataset",
                    suggested_fix=f"Add column '{field}' to dataset"
                ))
                continue
            
            # Check for null values
            null_mask = df[field].isna() | (df[field].astype(str).str.strip() == '')
            null_count = null_mask.sum()
            
            if null_count > 0:
                null_indices = df[null_mask].index.tolist()[:5]  # Show first 5
                report.add_error(ValidationError(
                    severity=ValidationSeverity.ERROR,
                    field=field,
                    row_index=None,
                    message=f"Required field '{field}' has {null_count} null/empty values (rows: {null_indices}...)",
                    suggested_fix="Fill in missing values or remove invalid rows"
                ))
    
    @staticmethod
    def validate_recommended_fields(df: pd.DataFrame, report: ValidationReport):
        """Check recommended fields and warn if missing"""
        for field in config.RECOMMENDED_FIELDS:
            if field not in df.columns:
                report.add_error(ValidationError(
                    severity=ValidationSeverity.WARNING,
                    field=field,
                    row_index=None,
                    message=f"Recommended field '{field}' is missing",
                    suggested_fix=f"Consider adding column '{field}'"
                ))
                continue
            
            # Check for high percentage of nulls
            null_count = df[field].isna().sum()
            null_pct = (null_count / len(df)) * 100
            
            if null_pct > 50:
                report.add_error(ValidationError(
                    severity=ValidationSeverity.WARNING,
                    field=field,
                    row_index=None,
                    message=f"Recommended field '{field}' is {null_pct:.1f}% empty ({null_count}/{len(df)} rows)",
                    suggested_fix="Run enrichment pipeline to populate this field"
                ))
    
    @staticmethod
    def validate_numeric_fields(df: pd.DataFrame, report: ValidationReport):
        """Validate that numeric fields contain valid numbers"""
        for field in config.NUMERIC_FIELDS:
            if field not in df.columns:
                continue
            
            # Try to convert to numeric
            try:
                numeric_series = pd.to_numeric(df[field], errors='coerce')
                
                # Check for conversion errors (non-numeric values)
                invalid_mask = df[field].notna() & numeric_series.isna()
                invalid_count = invalid_mask.sum()
                
                if invalid_count > 0:
                    invalid_indices = df[invalid_mask].index.tolist()[:5]
                    invalid_values = df[invalid_mask][field].tolist()[:5]
                    report.add_error(ValidationError(
                        severity=ValidationSeverity.ERROR,
                        field=field,
                        row_index=None,
                        message=f"Field '{field}' has {invalid_count} non-numeric values (e.g., {invalid_values})",
                        suggested_fix="Convert to numeric or remove invalid values"
                    ))
                
                # Check for negative numbers where inappropriate
                if field in ['Pages', 'Book Number', 'Total Books in Series']:
                    negative_mask = numeric_series < 0
                    negative_count = negative_mask.sum()
                    
                    if negative_count > 0:
                        report.add_error(ValidationError(
                            severity=ValidationSeverity.ERROR,
                            field=field,
                            row_index=None,
                            message=f"Field '{field}' has {negative_count} negative values",
                            suggested_fix="Negative values don't make sense for this field"
                        ))
            except Exception as e:
                report.add_error(ValidationError(
                    severity=ValidationSeverity.WARNING,
                    field=field,
                    row_index=None,
                    message=f"Could not validate numeric field '{field}': {e}"
                ))
    
    @staticmethod
    def validate_rating_ranges(df: pd.DataFrame, report: ValidationReport):
        """Validate that ratings are within valid ranges"""
        for field, (min_val, max_val) in config.RATING_RANGES.items():
            if field not in df.columns:
                continue
            
            numeric_series = pd.to_numeric(df[field], errors='coerce')
            
            # Check for out-of-range values
            out_of_range_mask = (numeric_series < min_val) | (numeric_series > max_val)
            out_of_range_count = out_of_range_mask.sum()
            
            if out_of_range_count > 0:
                out_of_range_indices = df[out_of_range_mask].index.tolist()[:5]
                out_of_range_values = numeric_series[out_of_range_mask].tolist()[:5]
                report.add_error(ValidationError(
                    severity=ValidationSeverity.ERROR,
                    field=field,
                    row_index=None,
                    message=f"Field '{field}' has {out_of_range_count} values outside valid range [{min_val}, {max_val}] (e.g., {out_of_range_values})",
                    suggested_fix=f"Ratings should be between {min_val} and {max_val}"
                ))
    
    @staticmethod
    def validate_urls(df: pd.DataFrame, report: ValidationReport):
        """Validate URL formats"""
        for field in config.URL_FIELDS:
            if field not in df.columns:
                continue
            
            pattern = config.URL_PATTERNS.get(field)
            if not pattern:
                continue
            
            # Check URLs
            non_null_mask = df[field].notna() & (df[field].astype(str).str.strip() != '')
            non_null_urls = df[non_null_mask][field]
            
            invalid_count = 0
            invalid_examples = []
            
            for idx, url in non_null_urls.items():
                if not re.match(pattern, str(url)):
                    invalid_count += 1
                    if len(invalid_examples) < 5:
                        invalid_examples.append(str(url))
            
            if invalid_count > 0:
                report.add_error(ValidationError(
                    severity=ValidationSeverity.WARNING,
                    field=field,
                    row_index=None,
                    message=f"Field '{field}' has {invalid_count} URLs with unexpected format (e.g., {invalid_examples[:2]})",
                    suggested_fix="Verify these are valid URLs"
                ))
    
    @staticmethod
    def validate_duplicates(df: pd.DataFrame, report: ValidationReport):
        """Detect potential duplicate entries"""
        # Import here to avoid circular dependency
        from utils.text_normalizer import TextNormalizer
        
        normalizer = TextNormalizer()
        
        # Create normalized columns
        df_temp = df.copy()
        df_temp['_norm_title'] = df_temp['Book Name'].apply(
            lambda x: normalizer.normalize_title(str(x))['fuzzy']
        )
        df_temp['_norm_author'] =df_temp['Author Name'].apply(
            lambda x: normalizer.normalize_author(str(x), level='fuzzy')
        )
        
        # Find duplicates (same title + same author)
        duplicates = df_temp[df_temp.duplicated(subset=['_norm_title', '_norm_author'], keep=False)]
        
        if len(duplicates) > 0:
            duplicate_count = len(duplicates)
            report.add_error(ValidationError(
                severity=ValidationSeverity.WARNING,
                field='Book Name, Author Name',
                row_index=None,
                message=f"Found {duplicate_count} potential duplicate book entries (same title + author)",
                suggested_fix="Run deduplication with BookMatcher.deduplicate_books()"
            ))
            
            # Show some examples
            for i, (idx, row) in enumerate(duplicates.head(3).iterrows()):
                report.add_error(ValidationError(
                    severity=ValidationSeverity.INFO,
                    field='Book Name',
                    row_index=idx,
                    message=f"Duplicate: '{row['Book Name']}' by {row['Author Name']}"
                ))
    
    @staticmethod
    def validate_schema(df: pd.DataFrame, report: ValidationReport):
        """Validate that dataset matches expected schema"""
        expected_cols = set(config.MASTER_SCHEMA_COLUMNS)
        actual_cols = set(df.columns)
        
        # Missing columns
        missing_cols = expected_cols - actual_cols
        if missing_cols:
            report.add_error(ValidationError(
                severity=ValidationSeverity.WARNING,
                field='Schema',
                row_index=None,
                message=f"Missing {len(missing_cols)} expected columns: {list(missing_cols)[:5]}",
                suggested_fix="Run DatasetManager.align_columns() to fix schema"
            ))
        
        # Extra columns
        extra_cols = actual_cols - expected_cols
        if extra_cols:
            report.add_error(ValidationError(
                severity=ValidationSeverity.INFO,
                field='Schema',
                row_index=None,
                message=f"Found {len(extra_cols)} unexpected columns: {list(extra_cols)[:5]}",
                suggested_fix="These columns will be ignored"
            ))
    
    @classmethod
    def validate_dataset(cls, df: pd.DataFrame) -> ValidationReport:
        """
        Run all validation checks on a dataset
        
        Args:
            df: DataFrame to validate
        
        Returns:
            ValidationReport with all validation results
        """
        report = ValidationReport(total_rows=len(df))
        
        # Run all validation checks
        cls.validate_schema(df, report)
        cls.validate_required_fields(df, report)
        cls.validate_recommended_fields(df, report)
        cls.validate_numeric_fields(df, report)
        cls.validate_rating_ranges(df, report)
        cls.validate_urls(df, report)
        cls.validate_duplicates(df, report)
        
        return report


def validate_file(file_path: str) -> ValidationReport:
    """Validate a CSV file"""
    try:
        df = pd.read_csv(file_path)
        return DataValidator.validate_dataset(df)
    except Exception as e:
        report = ValidationReport(total_rows=0)
        report.add_error(ValidationError(
            severity=ValidationSeverity.CRITICAL,
            field='File',
            row_index=None,
            message=f"Could not load file: {e}"
        ))
        return report


if __name__ == "__main__":
    # Test validation
    import sys
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "data/unified_book_data_enriched_aligned.csv"
    
    print(f"Validating: {file_path}")
    report = validate_file(file_path)
    report.print_summary()
