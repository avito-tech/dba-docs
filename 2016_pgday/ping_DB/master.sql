
CREATE TABLE force_wal
(
  id serial NOT NULL,
  pg_current_xlog_insert_location text,
  dtime timestamp with time zone
)
WITH (
  OIDS=FALSE
);

-- pg_current_xlog_insert_location() 	
-- Get current transaction log insert location

CREATE OR REPLACE FUNCTION force_wal(
    IN i_pg_current_xlog_insert_location text,
    OUT o_pg_current_xlog_insert_location text)
  RETURNS text AS
$BODY$
begin

    o_pg_current_xlog_insert_location := i_pg_current_xlog_insert_location;

    update 
        force_wal 
    set 
        pg_current_xlog_insert_location = i_pg_current_xlog_insert_location,
        dtime = now();

    if not FOUND then
        insert into force_wal 
            (pg_current_xlog_insert_location, dtime)
        values
            (i_pg_current_xlog_insert_location, now());
    end if;
    
    return;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
