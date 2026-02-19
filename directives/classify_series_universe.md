# Directive: Classify Series Universe Type

## Goal
Determine if a book series follows a "Same Characters" or "Different Characters" model within the same universe.

## Classification Options
1.  **same universe-same couple**: The primary couple (protagonists) from the first book remains the main focus across the subsequent books (e.g., a "serial" where one couple's story spans multiple volumes).
2.  **same universe different couple**: Each book focuses on a different main couple (e.g., siblings in a family, teammates on a sports team), but they all live in the same interconnected world (common in Romance series).

## Instructions for AI
Analyze the `Book Series Name`, `Author Name`, `Books_In_Series_List`, and any available `Subjective Analysis` or descriptions.

### Prompt Template
```text
You are a literary analysis agent. Your task is to classify a book series into one of two categories based on couple continuity.

Series Name: {series_name}
Author: {author}
Books in Series: {books_list}
Description/Analysis: {analysis}

Categories:
- "same universe-same couple": The main couple from the first book stays the same throughout the series.
- "same universe different couple": Each book focuses on a DIFFERENT main couple (very common in Romance series like Bridgerton or sports team series).

Respond with ONLY a JSON object:
{
  "universe_type": "same universe-same couple" | "same universe different couple",
  "reasoning": "Brief explanation focused on whether the couple changes or stays the same"
}
```
