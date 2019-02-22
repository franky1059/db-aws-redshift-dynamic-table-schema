
#!/bin/bash
# ---------------------------------------------------------------------
#
# Module        : rs_to_csv_headers.sh
#
# Description   : Dynamically generate csv headers from a redshift table
#
# Parameters    : NA
#
# Author        : Frank Myers
#
# Created Dt    : 02/13/2019
#
# Last Mod Dt   :
#
#
# Version       Modified By        Modified Dt      Description
# ----------    ---------------    -----------      ----------------
# 1.0           Frank Myers        Feb 13, 2018    	Initial Version
# ------------------------------------------------------------------------------

#set -x

PROGNAME=$(basename $0)
ABSPATH=$(readlink -f $0)
ABSDIR=$(dirname $ABSPATH)


config_file=$ABSDIR/$PROGNAME.param
. config_file


fieldnames=""
fieldnames_results=`psql -X "host=$rs_host port=$rs_port dbname=$rs_dbname user=$rs_user password=$rs_password"  -A -t -q  -c "SELECT QUOTE_IDENT(a.attname) AS col_name FROM pg_namespace AS n INNER JOIN pg_class AS c ON n.oid = c.relnamespace INNER JOIN pg_attribute AS a ON c.oid = a.attrelid LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum WHERE c.relkind = 'r' AND a.attnum > 0 AND n.nspname = '$schema_name' AND c.relname = '$db_name' ORDER BY a.attnum;"`
for fieldnames_results_item in $(echo $fieldnames_results | sed "s/ / /g")
do
fieldnames=$fieldnames','$fieldnames_results_item
done

fieldnames="${fieldnames:1:${#fieldnames}}"

echo $fieldnames









