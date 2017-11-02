---Table on subscriber's side
CREATE TABLE user_items_cnt
(
  user_id integer,
  category_id integer,
  cnt integer DEFAULT 0,
  date timestamp without time zone DEFAULT now()
)



CREATE TABLE tmp_user_cnt
(
  user_id integer
)



---Accept function
CREATE OR REPLACE FUNCTION accept_item(i_args hstore)
  RETURNS text AS
$BODY$
Begin
	if ((i_args->'category_id') is distinct from (i_args->'category_id_old')) then
		insert into user_items_cnt(user_id, category_id,cnt,date)
		values((i_args->'user_id')::Int, (i_args->'category_id')::Int,
			 1, now());
		if (is_args->'old_category_id') is distinct from null then
		insert into user_items_cnt(user_id, category_id,cnt,date)
		values((i_args->'user_id')::Int, (i_args->'old_category_id')::Int,
			 -1, now());
		end if;
			
	--TMP
	/*
	 if not exists (
         select user_id from tmp_user_cnt where user_id= (i_args->'user_id')::Int
         ) then
	 		insert into tmp_user_cnt (user_id) 
		      	values((i_args->'user_id')::Int);
	 end if;
	*/
	--TMP
	end if;
 	return 'OK';

end
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

 ---Subscriber init functions

CREATE OR REPLACE FUNCTION delete_user_cnt(i_args hstore)
  RETURNS text AS
$BODY$
begin
    delete from user_items_cnt where user_id = (i_args->'user_id')::int;
    return 'OK';
end
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


CREATE OR REPLACE FUNCTION init_cnt(i_args hstore)
  RETURNS text AS
$BODY$
begin
    insert into user_items_cnt(user_id, category_id,cnt,date)
        values((i_args->'user_id')::Int, (i_args->'category_id')::Int,
             (i_args->'count')::Int, now());
    return 'OK';

end
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

---Maintenance


        set local work_mem = '4 GB';
        
        with src as (
            --- найдём строки для удаления (агрегации)
            select
                cnt.user_id,
                cnt.category_id
            from
                user_items_cnt cnt
            group by
                cnt.user_id, cnt.category_id
            having
                count(*) > 1
        ), del as (
            --- удаляем и возвращаем что успешно удалилось на группировку
            delete from
                user_items_cnt cnt
            using
                src
            where
                cnt.user_id = src.user_id and cnt.category_id = src.category_id
            returning cnt.*
        ), agg as (
            --- суммируем только успешно удалённые (заблокированные) строки
            --- защита от конкурентного вызова по ошибке этой же функции
            select
                del.user_id,
                del.category_id,
                sum(del.cnt)::integer  as cnt
            from
                del
            group by
                del.user_id, del.category_id
        )
  insert into user_items_cnt(user_id, category_id, cnt, date)
    select agg.user_id, agg.category_id, agg.cnt, now() from agg where agg.cnt_total > 0;

    