
---Table and trigger function on provider's side
create table items 
(
	item_id int, 
	user_id int, 	
	category_id,
    last_update_txtime timestamp without time zone
)


--- deferred trigger
CREATE OR REPLACE FUNCTION items_delta_trg()
  RETURNS trigger AS
$BODY$
declare
    data hstore;
begin
  if TG_OP = 'INSERT' then
        Data := (select hstore(i) || hstore('old_category_id',null::text) from items i where i.item_id = NEW.item_id);
  elsif TG_OP = 'UPDATE' then
    if OLD.last_update_txtime is distinct from NEW.last_update_txtime then
        data := (select hstore(i)|| hstore('old_category_id',OLD.category_id::text) from items i where i.item_id = NEW.item_id);
    end if;
  end if;

  if data is not null then  
        perform xrpc._call(xrpc.x_qname('q_items_dt'), 'consumer_db', 'accept_item', data);
  end if;
  return NULL; -- deferred trigger
end
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

CREATE CONSTRAINT TRIGGER items_delta
  AFTER INSERT OR UPDATE 
  ON items
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW
  EXECUTE PROCEDURE items_delta_trg();



---Provider init function

CREATE OR REPLACE FUNCTION init_user_cnt(IN i_user_id integer, OUT error_code)
  RETURNS integer AS
$BODY$
begin
    error_code := 0;
    
    perform user_id from users where user_id = i_user_id for update;

    perform xrpc._call(xrpc.x_qname('q_items_dt'), 'consumer_db', 'delete_user_cnt', hstore('user_id', i_user_id::text)) 
    
    perform 
        xrpc._call(xrpc.x_qname('q_items_dt'), 'consumer_db', 'init_cnt', hstore(i)) 
    from     
        (
        select 
            user_id,category_id,count(*) 
        from 
            items 
        where 
            user_id = i_user_id
        group by user_id, category_id
        ) i;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;