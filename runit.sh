#!/bin/bash
# Copyright 1999 Kevin Closson

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Functions

function f_fix_scale() {
#global MIN_SCALE
local bsz="$1"
local scale="$2"
local factor=""

# Check for permissible values first
factor=$( echo "$scale" | sed 's/[0-9 MGT]//g'   )
[[ -n "$factor" ]] && return 1

#Work out scale:
factor=$( echo "$scale" | sed 's/[^MGT]//g'   )
if [ -z "$factor" ]
then
        # This is a simple integer assigment case
        scale=$( echo "$scale" | sed 's/[^0-9]//g' )
else
        scale=$( echo "$scale" | sed 's/[^0-9]//g' )
        case "$factor" in
                "M") scale=$(( ( 2**20  * $scale  ) / $bsz )) ;;
                "G") scale=$(( ( 2**30  * $scale  ) / $bsz )) ;;
                "T") scale=$(( ( 2**40  * $scale  ) / $bsz )) ;;
                * ) return 1 ;;
        esac
fi

if ( ! f_is_int $scale )
then
        echo "Computed scale is: \"${scale}\". Please report this logic error."
        return 1
fi

if [ $scale -lt $MIN_SCALE ]
then
        echo $scale
        return 1
else
        echo $scale
        return 0
fi
}

function f_is_int() {
local s="$1"

if ( ! echo $s |  grep -q "^-\?[0-9]*$" )
then
        return 1
else
        return 0
fi
}

function f_get_pg_stat_database_stats() {
local columns="$1"
local dbname="$2"
local conn_string="$3"
psql $conn_string  -t --quiet  -c "select $columns from pg_stat_database where datname = '$dbname'"
}

function f_get_shared_buffers() {
local dbname=$1
local conn_string=$2

psql $conn_string -c 'show shared_buffers'--quiet -t | sed -e '/^$/d' -e 's/ //g'
}

function f_format_output() {
#args: $run_time $after_reads $before_reads $dbname $num_schemas $ num_threads $before_cache_hits $after_cache_hits 

awk '{ printf("DBNAME:  %s. %d schemas, %d threads(each). Run time: %d seconds. RIOPS >%ld< CACHE_HITS/s >%ld<\n\n", $4,$5,$6, $1 , ($2 - $3 ) / $1, ($8 - $7) / $1  ) }'
}

function f_cr_functions() {
local required_functions="$1"
local conn_string="$2"
local tmp=""

for tmp in $( echo $required_functions )
do
	psql $conn_string -f $tmp
done
}

function f_print_results() {
echo "
 mypid | loop_iterations | sql_selects | sql_updates | sql_select_max_tm | sql_update_max_tm | select_blk_touch_cnt | update_blk_touch_cnt
-------+-----------------+-------------+-------------+-------------------+-------------------+----------------------+----------------------
" 

cat .pgio_schema_[0-9]*.out | sed -e '/[a-z]/d' -e '/---/g' -e '/^$/d'
}

function f_clean_files() {
rm -f pgio_*.out .pgio_*out
}

function f_diskstats() {
	{
	cat /proc/diskstats 
	echo 
	} >> pgio_diskstats.out
}

function f_os_monitoring() {
( iostat -xm 3 > iostat.out 2>&1 ) &
misc_pids="${misc_pids} $!"
( vmstat 3 > vmstat.out 2>&1 ) &
misc_pids="${misc_pids} $!"
( mpstat -P ALL 3  > mpstat.out 2>&1) &
misc_pids="${misc_pids} $!"

echo "$misc_pids"
}

function f_test_conn() {
local ret=0
local connect_string="$1"

echo '\q' | psql $connect_string > /dev/null 2>&1
ret=$?

[[ "$ret" -ne 0 ]] && return 1

return 0
}



# Main Program


source ./pgio.conf

UPDATE_PCT=${UPDATE_PCT:=0}
RUN_TIME=${RUN_TIME:=300}
NUM_SCHEMAS=${NUM_SCHEMAS:=1}
NUM_THREADS=${NUM_THREADS:=1}
WORK_UNIT=${WORK_UNIT:=255}
UPDATE_WORK_UNIT=${UPDATE_WORK_UNIT:=255}
SCALE=${SCALE:=128G}
DBNAME=${DBNAME:="pg10"}
CONNECT_STRING=${CONNECT_STRING:="pg10"}

pct=$UPDATE_PCT
run_tm=$RUN_TIME
num_schemas=$NUM_SCHEMAS
num_threads=$NUM_THREADS
work_unit=$WORK_UNIT
update_work_unit=$UPDATE_WORK_UNIT
scale=$SCALE
dbname="$DBNAME"
connect_string="$CONNECT_STRING"

misc_pids=""
mykill="/bin/kill "
waitpids=""
ret=0
tmp=""
mypsql=""

export MIN_SCALE=1024
export TABLE_SEED_NAME="pgio"
required_functions="sql/pgio_get_rand.sql sql/pgio_audit_table.sql sql/pgio.sql"

mypsql=$( type -p psql )

if [ ! -x "$mypsql" ]
then
	echo "Abort: psql is not in current shell execution path"
fi

if ( ! f_test_conn "$connect_string" )
then
        echo
        echo "Abort: The psql command aborted when invoked as follows: "
        echo "$ psql $connect_string"
        echo
        exit 1
fi



f_clean_files

tmp="$scale"  # Save the user-provided value for display
scale=$( f_fix_scale 8192 $tmp ) ; ret=$?

if [ $ret -ne 0 ]
then
        echo "Illegal scale value. Mininum value is $MIN_SCALE blocks"
        echo "Abort"
        exit 1
fi

f_cr_functions "$required_functions" "$connect_string" >> pgio_objects_creation.out 2>&1

shared_buffers=$( f_get_shared_buffers $dbname "$connect_string" )

[[ "$PGIO_DEBUG" = "TRUE" ]] && {
	echo $pct
	echo $run_tm
	echo $num_schemas
	echo $num_threads
	echo $work_unit
	echo $update_work_unit
	echo $scale
	echo $dbname
	echo "$connect_string"
}

echo "Date: `date`"
echo "Database connect string: \"${connect_string}\". "
echo "Shared buffers: ${shared_buffers}. "
echo "Testing $num_schemas schemas with $num_threads thread(s) accessing $tmp ($scale blocks) of each schema."
echo "Running iostat, vmstat and mpstat on current host--in background."

before_reads=$( f_get_pg_stat_database_stats blks_read $dbname "$connect_string")
before_cache_hits=$( f_get_pg_stat_database_stats blks_hit $dbname "$connect_string")

misc_pids=$( f_os_monitoring )

echo "Launching sessions. $num_schemas schema(s) will be accessed by $num_threads thread(s) each."

begin_secs=$SECONDS

for (( i = 1 ; i <= num_schemas ; i++ ))
do

	for (( j = 1 ; j <= num_threads ; j++ ))
	do
		( echo "SELECT * FROM mypgio('${TABLE_SEED_NAME}$i', $pct, $run_tm, $scale, $work_unit, $update_work_unit);" | psql $connect_string >> .pgio_schema_${i}_${j}.out 2>&1 ) &
		waitpids="$waitpids $!"
	done
done

launch_secs=$(( SECONDS - begin_secs ))
[[ $launch_secs -gt 0 ]] && echo "Sessions launched. NOTE: Launching the sessions took $launch_secs seconds." 

f_diskstats

echo "pg_stat_database stats:"
echo "          datname| blks_hit| blks_read|tup_returned|tup_fetched|tup_updated"
before_stats=$( f_get_pg_stat_database_stats "datname, blks_hit, blks_read,tup_returned,tup_fetched,tup_updated" $dbname "$connect_string" )

echo "BEFORE: $before_stats"

begin_secs=$SECONDS

wait $waitpids  # The workload is running while we wait here

end_secs=$SECONDS

after_stats=$( f_get_pg_stat_database_stats "datname, blks_hit, blks_read,tup_returned,tup_fetched,tup_updated" $dbname "$connect_string")
echo "AFTER:  $after_stats"

f_diskstats

after_reads=$( f_get_pg_stat_database_stats blks_read $dbname "$connect_string")
after_cache_hits=$( f_get_pg_stat_database_stats blks_hit $dbname "$connect_string")

run_time=$(( end_secs - begin_secs ))

echo "$run_time $after_reads $before_reads $dbname $num_schemas $num_threads $before_cache_hits $after_cache_hits" | f_format_output

$mykill -9 $misc_pids > /dev/null 2>&1

f_print_results > pgio_session_detail.out




