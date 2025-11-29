# Datonics Segments Processing

## Overview

This directory contains the dbt models for processing Datonics segment data and mapping it to AKKIO_IDs. Due to the massive scale of the datonics_ids table (609B rows), we use a batched approach with hierarchical leaf-filtering to reduce redundancy.

## The Problem

Datonics segments are hierarchically nested (e.g., `Automotive > Luxury > Owners`). The raw data contains:
- 1,191 distinct segments across 29 top-level categories
- 609 billion rows in datonics_ids
- Significant redundancy: if an ID has `Automotive > Luxury > Owners`, it also has rows for `Automotive > Luxury` and `Automotive`

## The Solution

### 1. Leaf Filtering
For each ID, we only keep the **deepest segment in each hierarchy branch**. This eliminates redundant parent segments while preserving all information.

**Example:**
- Before: ID has 3 rows: `Automotive`, `Automotive > Luxury`, `Automotive > Luxury > Owners`
- After: ID has 1 row: `Automotive > Luxury > Owners` (with L1='Automotive', L2='Luxury', L3='Owners')

**Result:** ~18-25% row reduction depending on category

### 2. Hierarchy Pivoting
Instead of storing the full segment_name string, we split it into L1-L5 columns:
- L1: Top-level category (e.g., 'Automotive')
- L2: Second level (e.g., 'Luxury')
- L3: Third level (e.g., 'Owners')
- L4: Fourth level (max depth observed: 5)
- L5: Fifth level

This allows querying like:
```sql
-- Find all IDs in Automotive
WHERE SEGMENT_L1 = 'Automotive'

-- Find all IDs in Luxury (any category)
WHERE SEGMENT_L2 = 'Luxury'

-- Find IDs in Automotive Luxury but not Travel Luxury
WHERE SEGMENT_L1 = 'Automotive' AND SEGMENT_L2 = 'Luxury'
  AND AKKIO_ID NOT IN (SELECT AKKIO_ID WHERE SEGMENT_L1 = 'Travel' AND SEGMENT_L2 = 'Luxury')
```

### 3. Category Batching
Processing is split by top-level category (29 categories) for:
- Parallelization: Multiple categories can run simultaneously
- Incremental updates: Only re-process changed categories
- Debuggability: Easier to monitor and troubleshoot individual categories

### 4. ID Type Splitting
Within each category, we process by id_type (`ip`, `aaid`, `idfa`, `ctv`) to:
- Reduce join cardinality
- Avoid cross-product issues
- Optimize query performance (~2.5 min per id_type for Automotive)

## Architecture

```
int_datonics_segments_metadata
  └─ Pre-computed segment metadata and parent-child relationships (1,191 segments)

int_datonics_categories
  └─ Simple view listing all 29 distinct categories (optional, for reference)

datonics_all_segments
  └─ Single-pass SQL query processing ALL categories at once
     Split by id_type (ip, aaid, idfa, ctv) for performance
```

## Adding a New Category

**No code changes required!** New categories are automatically detected from the source data.

When a new top-level category appears in `datonics_segments.segment_name`, it will be:
1. Picked up by `int_datonics_segments_metadata` (refreshes from source)
2. Automatically processed by `datonics_all_segments` (processes all segments regardless of category)

To verify coverage, query `int_datonics_categories` to see the current list.

## Performance Estimates

**Single-pass approach** (current implementation):
- All 29 categories processed in one query
- Split by id_type for efficiency (4 parallel CTEs)
- Estimated runtime: Unknown - needs testing
- Expected to be comparable or better than sequential category processing due to database optimization

**Previous per-category approach** (for reference):
- Medium category (Automotive, 50 segments): ~13 minutes
- All 29 categories sequentially: 3-5 hours estimated

The single-pass approach should allow the database to optimize across all categories and potentially improve performance.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| AKKIO_ID | STRING | Akkio unique identifier |
| SEGMENT_ID | STRING | Datonics segment ID |
| SEGMENT_L1 | STRING | Top-level category |
| SEGMENT_L2 | STRING | Second-level segment |
| SEGMENT_L3 | STRING | Third-level segment |
| SEGMENT_L4 | STRING | Fourth-level segment |
| SEGMENT_L5 | STRING | Fifth-level segment |
| SEGMENT_DESCRIPTION | STRING | Segment description |
| SEGMENT_SOURCE | STRING | Always 'datonics' |

## Notes

- Leaf filtering is done **per ID**, not globally - we preserve IDs that only have parent segments
- Unrelated hierarchies (e.g., `Automotive > Luxury` and `Travel > Luxury`) are both kept
- The L1-L5 structure handles max observed depth of 5 levels
- All processing uses DISTINCT to handle any duplicate rows from the source data
