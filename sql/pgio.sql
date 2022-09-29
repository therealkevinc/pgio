-- Copyright 1999 Kevin Closson

-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at

--    http://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

DROP TYPE pgio_return CASCADE;
CREATE TYPE pgio_return AS (
mypid int, 
loop_iterations bigint , 
sql_selects bigint, 
sql_updates bigint, 
sql_select_max_tm numeric, 
sql_update_max_tm numeric ,
select_blk_touch_cnt bigint,
update_blk_touch_cnt bigint
);


CREATE OR REPLACE FUNCTION mypgio(
v_mytab			varchar,
v_pctupd		int,
v_runtime_secs 		bigint,
v_scale			bigint,
v_select_batch_size	int,
v_update_batch_size	int
) RETURNS pgio_return LANGUAGE plpgsql
AS  $$
DECLARE
rec_myvar pgio_return%rowtype;

v_mytab        		ALIAS for $1;
v_pctupd       		ALIAS for $2;
v_runtime_secs 		ALIAS for $3;
v_scale        		ALIAS for $4;
v_select_batch_size	ALIAS for $5;
v_update_batch_size	ALIAS for $6;

v_end_time 		timestamp WITHOUT TIME ZONE; 
v_before		timestamp WITHOUT TIME ZONE; 
v_after			timestamp WITHOUT TIME ZONE; 
v_tm			timestamp WITHOUT TIME ZONE; 
v_tm_delta		numeric		:= 0.0;
v_select_max_tm		numeric		:= 0.0;
v_update_max_tm		numeric		:= 0.0;
v_select_batch_tm_total	numeric		:= 0.0;
v_update_batch_tm_total	numeric		:= 0.0;

v_tmp 			bigint		:= 0;
v_scratch 		bigint		:= 0;
v_optype 		int 		:= 0;
v_master_loop_cnt 	bigint 		:= 0;
v_mykey 		bigint 		:= 0;
v_pid 			int 		:= 0;
v_op_cnt 		int 		:= 0;
v_select_cnt 		int 		:= 0;
v_update_cnt 		int 		:= 0;
v_select_cnt_total 	bigint 		:= 0;
v_update_cnt_total	bigint 		:= 0;
v_select_blk_touch_cnt	bigint		:= 0;
v_update_blk_touch_cnt	bigint		:= 0;
v_update_quota		boolean 	:= FALSE;
v_select_quota		boolean 	:= FALSE;
v_select_only		boolean  	:= FALSE;
v_update_only		boolean  	:= FALSE;

BEGIN
rec_myvar.mypid 		:= 0 ;
rec_myvar.sql_updates		:= 0 ;
rec_myvar.sql_selects		:= 0 ;
rec_myvar.loop_iterations	:= 0 ;
rec_myvar.sql_select_max_tm	:= 0.0;
rec_myvar.sql_update_max_tm	:= 0.0;
rec_myvar.select_blk_touch_cnt	:= 0;
rec_myvar.update_blk_touch_cnt	:= 0;

SELECT pg_backend_pid() into v_pid;

CASE
	WHEN ( v_pctupd = 0 )   THEN v_select_only = TRUE ;
	WHEN ( v_pctupd = 100 ) THEN v_update_only = TRUE ;
	
	WHEN ( v_pctupd > 100 ) THEN RAISE EXCEPTION 'FATAL : UPDATE_PCT "%" IS GREATER THAN 100.', v_pctupd ;
	WHEN ( v_pctupd < 0 )   THEN RAISE EXCEPTION 'FATAL : UPDATE_PCT "%" IS LESS THAN ZERO.', v_pctupd ;
	WHEN v_pctupd BETWEEN 51 AND 99 THEN RAISE EXCEPTION 'FATAL : UPDATE_PCT "%" BETWEEN 51 and 99 ARE NOT SUPPORTED.', v_pctupd ;

ELSE
	RAISE NOTICE 'I am PID "%" : My table is "%" : UPDATE_PCT "%" : RUN TIME SECONDS "%"', v_pid, v_mytab, v_pctupd, v_runtime_secs ;
END CASE;


v_end_time := clock_timestamp() + (v_runtime_secs || ' seconds')::interval ;

WHILE ( clock_timestamp()::timestamp < v_end_time ) LOOP

SELECT pgio_get_random_number(1, v_scale - v_select_batch_size) INTO v_mykey;    

IF     ( v_update_only = TRUE ) THEN 
	v_optype := 1;
ELSEIF ((v_update_only = FALSE AND v_select_only = FALSE AND (MOD(v_mykey , 2) = 0) AND ( v_update_quota != TRUE )) OR (v_select_quota = TRUE) ) THEN
	v_optype := 1;
ELSEIF ( v_select_only = TRUE ) THEN 
	v_optype := 0;
ELSE
	v_optype := 0;
END IF;	

v_before := clock_timestamp();

IF ( v_optype = 0 ) THEN
	EXECUTE 'SELECT sum(scratch) FROM ' || v_mytab || ' WHERE mykey BETWEEN ' || v_mykey || ' AND ' || v_mykey + v_select_batch_size  INTO v_scratch;

	v_tm_delta := cast(extract(epoch from (clock_timestamp() - v_before)) as numeric(12,8));


	IF ( v_tm_delta > v_select_max_tm ) THEN
		v_select_max_tm := v_tm_delta;
	END IF;

	v_select_cnt := v_select_cnt + 1;
	v_select_cnt_total := v_select_cnt_total + 1;
	v_select_blk_touch_cnt := v_select_blk_touch_cnt + v_select_batch_size;

	v_select_batch_tm_total := v_select_batch_tm_total + v_tm_delta;

	IF ( v_select_cnt >= ( 100 - v_pctupd) ) THEN
		v_select_quota := TRUE;
	END IF;

ELSE
	EXECUTE 'UPDATE ' || v_mytab || ' SET scratch = scratch + 1 WHERE mykey BETWEEN ' || v_mykey || ' AND ' || v_mykey + v_update_batch_size;

	v_tm_delta := cast(extract(epoch from (clock_timestamp() - v_before)) as numeric(12,8));

	IF ( v_tm_delta > v_update_max_tm ) THEN
		v_update_max_tm := v_tm_delta;
	END IF;

	v_update_cnt := v_update_cnt + 1;
	v_update_cnt_total := v_update_cnt_total + 1;
	v_update_blk_touch_cnt := v_update_blk_touch_cnt + v_update_batch_size;

	v_update_batch_tm_total := v_update_batch_tm_total + v_tm_delta;

	IF ( v_update_cnt >= v_pctupd ) THEN
		v_update_quota := TRUE;
	END IF;

END IF;

v_op_cnt := v_op_cnt + 1 ;
v_master_loop_cnt := v_master_loop_cnt + 1 ;


IF ( v_op_cnt >= 100 ) THEN
	v_op_cnt := 0;
	v_select_cnt := 0;
	v_update_cnt := 0;
	v_select_batch_tm_total := 0;
	v_update_batch_tm_total := 0;
	v_update_quota := FALSE;
	v_select_quota := FALSE;
END IF;

END LOOP;

rec_myvar.mypid			:= v_pid;
rec_myvar.loop_iterations	:= v_master_loop_cnt;
rec_myvar.sql_selects		:= v_select_cnt_total;
rec_myvar.sql_updates		:= v_update_cnt_total;
rec_myvar.sql_select_max_tm   	:= v_select_max_tm;
rec_myvar.sql_update_max_tm   	:= v_update_max_tm;
rec_myvar.select_blk_touch_cnt  := v_select_blk_touch_cnt;
rec_myvar.update_blk_touch_cnt  := v_update_blk_touch_cnt;

RETURN rec_myvar;
END;
$$;
