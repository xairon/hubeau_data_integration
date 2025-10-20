# Legacy Hub'Eau Slicing Strategies

This folder contains the OLD implementation using departmental and station-based slicing.

## Why Removed?

Hub'Eau API has a bug: when you OMIT the `size` parameter, you can paginate indefinitely
using just `page=N`, bypassing the official 20k depth limit.

### Discovery

Testing revealed:
```bash
# Official behavior (documented):
curl "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations?format=json&size=5000&page=1"
# Returns: 5000 records max, depth limit 20k

# Actual behavior (bug):
curl "https://hubeau.eaufrance.fr/api/v1/niveaux_nappes/stations?format=json&page=5"
# Returns: 3205 records (last page), which should be impossible
```

## Old Strategies (Removed 2025-01-16)

### 1. Departmental Slicing
- Split requests by 101 French departments (01-95, 2A, 2B, 971-978)
- Required complex chunking logic (1-5 depts per request)
- Caused HTTP 500 errors on overloaded queries
- **Removed**: ~300 lines of chunking logic

### 2. Station Code Chunking
- Split station codes into chunks (25-50 codes per request)
- Avoided URL length limits (>2083 chars)
- Required parallel processing with semaphores
- **Removed**: ~200 lines of chunking logic

### 3. Temporal Chunking
- Split large date ranges into months/weeks
- Special handling for temperature (station-by-station)
- Special handling for ecoulement (campagne-based)
- **Removed**: ~300 lines of special-case methods

### 4. Depth Limit Handling
- Tracked 20k record limits per API
- Issued truncation warnings
- Complex fallback strategies
- **Removed**: All depth limit checks

## Code Complexity Removed

- **Modern Client** (`hubeau_client.py`): ~800 lines → ~200 lines
- **Legacy DLT** (`hubeau_source.py`): 5 slicing functions → 2 slicing functions
- **Config Files**: 23 files with dept lists → simple global pagination
- **Total Reduction**: ~1,050 lines (-70%)

## Files Backed Up

- `hubeau_client_old.py` - Modern client with all slicing logic
- `hubeau_source_old.py` - DLT client with 5 slicing functions
- `hubeau_configs_old.py` - Endpoint configs with page_size/depth_limit

## If Rollback Needed

If Hub'Eau fixes the pagination bug:

1. Restore backed up files from this folder
2. Revert config files to use `slicing_mode: dept`
3. Re-add `page_size` parameters to endpoint configs
4. Update Dagster assets to use old slicing strategies

## Performance Impact

### Before (Complex Slicing)
- Piézométrie stations: 101 dept requests × 5-10 pages = 500-1000 API calls
- Quality rivers: 101 dept requests × parallel processing = complex orchestration
- Frequent HTTP 500 errors from overloaded queries

### After (Simple Pagination)
- All APIs: Single paginated request stream
- Piézométrie stations: ~10-20 pages total = 10-20 API calls
- No more HTTP 500 errors from spatial overload

## Warning

⚠️ **This refactoring relies on an undocumented API bug**

If Hub'Eau fixes their pagination logic:
- Pagination will stop at page 20k/page_size ≈ page 4-20 (depending on default size)
- Data will be incomplete
- Monitor for sudden drops in record counts

**Monitoring**: Watch for these signs of bug fix:
- Sudden decrease in records fetched
- Pagination stopping early (< expected pages)
- New HTTP 400 errors mentioning pagination limits

Date Archived: 2025-01-16
