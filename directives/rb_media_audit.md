# Directive: RB Media Shortlist Audit

## Goal
Enrich the RB Media Shortlist with accurate author names and audited book counts (Primary and Total) from Goodreads.

## Procedure
1.  **Extract Goodreads Series ID**: Parse the `Series URL` to get the series ID (e.g., `43943-outlander`).
2.  **Fetch Goodreads Data**:
    *   Navigate to `https://www.goodreads.com/series/[ID]`.
    *   **Author Name**: Identify the primary author of the series.
    *   **Primary Works**: Count the number of "Primary Works" listed on the Goodreads series page.
    *   **Total Works**: Count the total number of works (including novellas, collections, etc.) listed.
3.  **Update CSV**:
    *   Populate `Author Name` in the 3rd column.
    *   Audit/Update `Primary Works` and `Total Works` in their respective columns.
4.  **Consistency Check**: Ensure the series name matches the one on Goodreads or is a reasonable variation.

## Precision Requirements
*   "Primary Works" specifically refers to the main entries in the series (usually numbered 1, 2, 3...).
*   If multiple authors are listed, prioritize the first/main author.
