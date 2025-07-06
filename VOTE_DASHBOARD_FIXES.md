# Vote Dashboard Fixes

## Issues Fixed:

### Update: Fixed "nested window functions" error
- The original recent_votes_with_stakes view used ROW_NUMBER() window function in a CTE
- PostgreSQL doesn't allow window functions in WHERE clauses of CTEs
- Fixed by using DISTINCT ON instead of ROW_NUMBER()
- Also created a simpler alternative view (recent_votes_simple) as a fallback

## Original Issues Fixed:

1. **Reserved Keyword "window"**: 
   - PostgreSQL treats "window" as a reserved keyword
   - Fixed by quoting "window" in all SQL queries and schema definitions
   - Updated in both dashboard queries and materialized view definition

2. **Duplicate Panel IDs**: 
   - Panel ID 106 was used twice (Account Change Statistics and Vote Participation)
   - Updated Vote Participation panels to use IDs 110-113

3. **Time Series Query**:
   - Added time filter to limit data to last hour for better performance
   - Reordered columns for proper Grafana time series format (time, value, metric)

4. **Materialized View Refresh**:
   - Added refresh function for the materialized view
   - Added optional trigger function for automatic refresh
   - Can be scheduled with pg_cron or called manually

## Dashboard Panels:

1. **Current Voting Stake %** (ID: 111)
   - Shows the latest 5-minute voting stake percentage
   - Color thresholds: Red < 33%, Yellow 33-66%, Green > 66%

2. **Vote Participation Over Time** (ID: 112)
   - Time series showing 5-minute, 30-minute, and 1-hour windows
   - Y-axis limited to 0-100%
   - Shows mean and last values in legend

3. **Recent Votes with Validator Stakes** (ID: 113)
   - Table showing recent votes with validator information
   - Includes stake amount, percentage, and validator status
   - Limited to 100 most recent votes

## Testing:
Use the test_vote_queries.sql file to validate all queries work correctly before deployment.

## If Issues Persist:
If the recent_votes_with_stakes view still has issues, update the dashboard to use recent_votes_simple:

```sql
-- Replace this in the dashboard JSON:
FROM recent_votes_with_stakes

-- With this:
FROM recent_votes_simple
```

The simpler view avoids complex CTEs and window functions entirely.