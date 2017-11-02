--pg_xlog_location_diff(location text, location text) 	numeric 	
--Calculate the difference between two transaction log locations

select pg_xlog_location_diff(pg_last_xlog_replay_location(), '$1') --'$1' result from master
