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

function f_create_base_table() {
local t=$1
local rows=$2

echo "
drop table ${t} ;

\timing
create table ${t} as 
	select mykey::bigint, (random()*1000000000)::bigint as scratch ,
	repeat('X', 1024)::char(1024) filler from generate_series(1,$rows) 
	as mykey order by scratch;

\d ${t} 
"
}

function f_drop_tables() {
local table_seed_name=$1
local i=0

for i in {1..8192}
do
	echo "DROP TABLE ${table_seed_name}${i} ;"
done

}

function f_get_base_table_facts() {
local connect_string="$1"
local out_string=""

out_string=$( psql $connect_string -c "\d+ pgio_base" 2>&1 )

if ( echo $out_string | grep -i 'did not find' > /dev/null 2>&1 )
then
	echo 
	echo "FATAL : The pgio.conf->CREATE_BASE_TABLE parameter is not set to \"TRUE\" but no prior pgio_base table exists."
	echo "FATAL : Please set pgio.conf->CREATE_BASE_TABLE to \"TRUE\" and execute $0 again."
	echo 
	return 1
fi

return 0
}

function f_create_table() {
local target=$1
local source=$2
local table_seed_name=$3

echo "
drop table ${table_seed_name}${target} ;
create table ${table_seed_name}${target} with (fillfactor=10) as select * from $source limit 1;
truncate table ${table_seed_name}${target};
create index ${table_seed_name}${target}_idx on ${table_seed_name}${target}(mykey) ;
insert into ${table_seed_name}${target} select * from $source;
"
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

export MIN_SCALE=1024
export TABLE_SEED_NAME="pgio"

ret=0

mypsql=$( type -p psql )

if [ ! -x "$mypsql" ]
then
        echo "Abort: psql is not in current shell execution path"
	exit 1
fi

source ./pgio.conf

SCALE=${SCALE:=4G}
NUM_SCHEMAS=${NUM_SCHEMAS:=1}
NUM_THREADS=${NUM_THREADS:=1}
CONNECT_STRING=${CONNECT_STRING:="pg10"}
CREATE_BASE_TABLE=${CREATE_BASE_TABLE:="TRUE"}

num_schemas=$NUM_SCHEMAS
num_threads=$NUM_THREADS
create_base_table=$CREATE_BASE_TABLE
connect_string="$CONNECT_STRING"


if ( ! f_test_conn "$connect_string" )
then
	echo
	echo "Abort: The psql command aborted when invoked as follows: "
        echo "$ psql $connect_string"
	echo
	exit 1
fi

scale=$( f_fix_scale 8192 $SCALE ) ; ret=$?

if [ $ret -ne 0 ]
then
        echo "Illegal scale value. Mininum value is $MIN_SCALE blocks"
        echo "Abort"
        exit 1
fi

rm -f pgio_setup*.out drop_tabs.sql table_*.sql index_*.sql table_*.out index_*.out

echo

echo "Job info:      Loading $SCALE scale into $num_schemas schemas as per pgio.conf->NUM_SCHEMAS."
echo "Batching info: Loading $num_threads schemas per batch as per pgio.conf->NUM_THREADS."

if [ "$create_base_table" = "TRUE" ]
then
	f_create_base_table pgio_base $scale | psql $connect_string > pgio_base_table_load.out 2>&1
	echo "Base table loading time: $(( SECONDS - before )) seconds."
else
	echo "NOTICE: Skipping creation of base table as per pgio.conf->CREATE_BASE_TABLE. Loading will proceed from existing pgio_base table."
	f_get_base_table_facts $connect_string ; ret=$?
	[[ "$ret" != 0 ]] && echo "Abort" && exit 1
fi


before=$SECONDS

f_drop_tables $TABLE_SEED_NAME | psql --echo-all $connect_string  > pgio_setup_drop_tables.out 2>&1

before_group_data_load=$SECONDS

for (( i=1 , cnt=1 ; i <= $num_schemas ; i++ , cnt++ ))
do
	( f_create_table $i pgio_base $TABLE_SEED_NAME | psql $connect_string > pgio_setup_t${i}.out 2>&1 ) &

	if [ $cnt = $num_threads ]
	then
		echo "Waiting for batch. Global schema count: ${i}. Elapsed: $(( SECONDS - before_group_data_load )) seconds." 
		wait 
		cnt=1
	fi

	if [ $i = $num_schemas ]
	then
		echo "Waiting for batch. Global schema count: ${i}. Elapsed: $(( SECONDS - before_group_data_load )) seconds." 

	fi	

done

wait

(( load_time = $SECONDS - $before_group_data_load ))

echo -e "\nGroup data loading phase complete.         Elapsed: $load_time seconds."

sleep 5
psql $connect_string -f sql/pgio_table_sizes.sql > pgio_data_load_table_sizes.out

rm -f pgio_setup_*



