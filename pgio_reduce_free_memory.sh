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

function f_calc_free_pct() {
local desired_free=$1

free -b | grep '^Mem' | awk -v x=$desired_free '{ printf("%4.1f \n", (x * 1024 * 1024 * 1024) / $2 * 100 ) }'

}

# Main program

[[ $# -ne 1 ]] && echo "Usage: $0 <integer amount of memory (in gigabytes) to remain free.>" && exit 1

if ( ! type -p bc > /dev/null 2>&1 )
then
	echo "bc(1) is not installed"
	exit 1
fi

[[ ! -w /etc/passwd ]] && echo "You must be superuser to execute $0" && exit 1

LeaveFreeGB=$1
LeaveFreePct=$( f_calc_free_pct $LeaveFreeGB )

echo;echo
read -p "Enter \"YES\" to consume free memory leaving only ${LeaveFreeGB}GB free (${LeaveFreePct}% of all RAM) : " tmp

[[ "$tmp" != "YES" ]] && echo "You did not enter \"YES\". Aborting." && exit 1

echo "Taking action to reduce free memory down to ${LeaveFreeGB}GB available."

sync;sync;sync  # I know modern systems don't need the "3 syncs" but I'm old :).
echo 0 > /proc/sys/vm/nr_hugepages
echo 3 > /proc/sys/vm/drop_caches

MemFreeBytes=$( grep '^MemFree' /proc/meminfo | awk '{ print $2 * 1024 }' )
Pages=$( echo " ( ($MemFreeBytes  ) - ( $LeaveFreeGB * 2^30 ) ) / ( 2 * 2 ^ 20 )" | bc )

free
sync;sync;sync
echo 0 > /proc/sys/vm/nr_hugepages
echo 3 > /proc/sys/vm/drop_caches
echo
echo "Attempting to allocate $Pages huge pages"
echo $Pages > /proc/sys/vm/nr_hugepages


egrep "HugePages_Total|MemAvailable" /proc/meminfo

