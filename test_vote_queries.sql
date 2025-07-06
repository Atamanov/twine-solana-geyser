-- Test queries for vote participation dashboard
-- Run these queries to validate the dashboard will work correctly

-- Test 1: Current voting stake percentage (stat panel)
SELECT voting_stake_percentage
FROM vote_participation_stats
WHERE "window" = '5 minutes'
ORDER BY last_updated DESC
LIMIT 1;

-- Test 2: Vote participation over time (time series)
SELECT 
  last_updated AS time,
  voting_stake_percentage AS value,
  "window" AS metric
FROM vote_participation_stats
WHERE last_updated > NOW() - INTERVAL '1 hour'
ORDER BY last_updated, "window";

-- Test 3: Recent votes with stakes (table)
SELECT 
  slot,
  voter_pubkey,
  vote_signature,
  created_at,
  validator_stake,
  stake_percentage,
  validator_status
FROM recent_votes_with_stakes
LIMIT 100;

-- Test 4: Check if materialized view exists and has data
SELECT COUNT(*) as row_count FROM vote_participation_stats;

-- Test 5: Check if recent_votes_with_stakes view works
SELECT COUNT(*) as vote_count FROM recent_votes_with_stakes;

-- Test 6: Alternative simpler view (use this in dashboard if main view has issues)
SELECT * FROM recent_votes_simple LIMIT 10;

-- Test 7: Debug - check if we have any epoch validator sets
SELECT epoch, COUNT(*) as validator_count, SUM(total_stake) as total_stake
FROM epoch_validator_sets
GROUP BY epoch
ORDER BY epoch DESC;