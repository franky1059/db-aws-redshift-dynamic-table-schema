

Dynamically Querying Table Column DDL Info in Redshift
--------------------------------------
#### Dynamic Column Info
A very useful ability when working with databases is to be able to dynamically retrieve table description data of a table where data is stored. You may want to do this for many different reasons; a very common one would be to create dynamically generated UI (also known as scaffolding), another may be to modify or append extra columns to an existing data-set - say for example, during an ETL step or report generation. <br />
The major point being that you'd like to create one block of code that won't need to change if the data schema of the tables in your databases happen to change. This will reduce regression errors and maintenance efforts throughout the life of your code. 

#### Redshift vs other Databases
Redshift differs from other, more established, databases in that it doesn't contain a pre-packaged command for describing the DDL for existing database objects. This differs greatly from older db's like MySQL that make this task [super easy](http://www.mysqltutorial.org/mysql-show-columns/). <br />
Since Redshift doesn't come with commands for this task what approach should we use? The answer is to use queries or views that query Redshift's internal tables to piece together the DDL info we need. 


#### Querying Column Info
To dynamically get our DDL information we'll use the [catalog tables](https://docs.aws.amazon.com/redshift/latest/dg/c_join_PG.html). If we only want a listing of column names in a table we can do so with this query...


	SELECT QUOTE_IDENT(a.attname) AS col_name
	FROM pg_namespace AS n
	INNER JOIN pg_class AS c ON n.oid = c.relnamespace
	INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
	LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum
	WHERE c.relkind = 'r'
	 AND a.attnum > 0
	 AND n.nspname = '<schema name>'
	 AND c.relname = '<table name>'
	ORDER BY a.attnum


<br />
If we want more information, like what data type each column is, we can use a query like this...
<br />


	SELECT
	n.nspname AS schemaname
	,c.relname AS tablename
	,100000000 + a.attnum AS seq
	,CASE WHEN a.attnum > 1 THEN ',' ELSE '' END AS col_delim
	,QUOTE_IDENT(a.attname) AS col_name
	,CASE WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING') > 0
	  THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING', 'VARCHAR')
	 WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER') > 0
	  THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER', 'CHAR')
	 ELSE UPPER(format_type(a.atttypid, a.atttypmod))
	 END AS col_datatype
	,CASE WHEN format_encoding((a.attencodingtype)::integer) = 'none'
	 THEN ''
	 ELSE 'ENCODE ' + format_encoding((a.attencodingtype)::integer)
	 END AS col_encoding
	,CASE WHEN a.atthasdef IS TRUE THEN 'DEFAULT ' + adef.adsrc ELSE '' END AS col_default
	,CASE WHEN a.attnotnull IS TRUE THEN 'NOT NULL' ELSE '' END AS col_nullable
	FROM pg_namespace AS n
	INNER JOIN pg_class AS c ON n.oid = c.relnamespace
	INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
	LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum
	WHERE c.relkind = 'r'
	 AND a.attnum > 0
	 AND n.nspname = ''
	 AND c.relname = ''
	ORDER BY a.attnum

The key to these queries is to put your schema name as the value for n.nspname, and your table name as the value for c.relname.





#### Generic Script to Dump Modified Column Headers into a CSV
Say we wanted to make a quick shell script to dynamically pull columns from a table and use that information for another step in our ETL process - for example, appending several tables' data to a csv file. Keep in mind Redshift already comes with a very robust set of data migration tools, [COPY](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) and [UNLOAD](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html), but there's always some peculiar operation that needs to be done where existing methods just won't cut it (such is the life of a professional developer). <br />
For our example we can use the [PostgreSQL cli client psql](https://docs.aws.amazon.com/redshift/latest/mgmt/connecting-from-psql.html) to dynamically query our table's columns, put them in csv form, then from there the world is our oyster. For example .... <br/>

	fieldnames=""
	fieldnames_results=`psql -X "host=$rs_host port=$rs_port dbname=$rs_dbname user=$rs_user password=$rs_password"  -A -t -q  -c "SELECT QUOTE_IDENT(a.attname) AS col_name FROM pg_namespace AS n INNER JOIN pg_class AS c ON n.oid = c.relnamespace INNER JOIN pg_attribute AS a ON c.oid = a.attrelid LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum WHERE c.relkind = 'r' AND a.attnum > 0 AND n.nspname = '$schema_name' AND c.relname = '$db_name' ORDER BY a.attnum;"`
	for fieldnames_results_item in $(echo $fieldnames_results | sed "s/ / /g")
	do
	fieldnames=$fieldnames','$fieldnames_results_item
	done

	fieldnames="${fieldnames:1:${#fieldnames}}"

... now "fieldnames" possesses a comma delimited string of our table which was created completely on the fly. 





Code
--------------------------------------	
- [db-aws-redshift-dynamic-table-schema (GitHub)](https://github.com/franky1059/db-aws-redshift-dynamic-table-schema)



Links
--------------------------------------
- [Examples of Catalog Queries (aws docs)](https://docs.aws.amazon.com/redshift/latest/dg/c_join_PG_examples.html)
- [Querying the Catalog Tables (aws docs)](https://docs.aws.amazon.com/redshift/latest/dg/c_join_PG.html)
- [awslabs/amazon-redshift-utils (aws github)](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/AdminViews)
- excerpt from <https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_generate_tbl_ddl.sql>	
	<code>
	   SELECT
	n.nspname AS schemaname
	,c.relname AS tablename
	,100000000 + a.attnum AS seq
	,CASE WHEN a.attnum > 1 THEN ',' ELSE '' END AS col_delim
	,QUOTE_IDENT(a.attname) AS col_name
	,CASE WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING') > 0
	  THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING', 'VARCHAR')
	 WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER') > 0
	  THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER', 'CHAR')
	 ELSE UPPER(format_type(a.atttypid, a.atttypmod))
	 END AS col_datatype
	,CASE WHEN format_encoding((a.attencodingtype)::integer) = 'none'
	 THEN ''
	 ELSE 'ENCODE ' + format_encoding((a.attencodingtype)::integer)
	 END AS col_encoding
	,CASE WHEN a.atthasdef IS TRUE THEN 'DEFAULT ' + adef.adsrc ELSE '' END AS col_default
	,CASE WHEN a.attnotnull IS TRUE THEN 'NOT NULL' ELSE '' END AS col_nullable
   FROM pg_namespace AS n
   INNER JOIN pg_class AS c ON n.oid = c.relnamespace
   INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
   LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum
   WHERE c.relkind = 'r'
	 AND a.attnum > 0
	 AND n.nspname = ''
	 AND c.relname = ''
   ORDER BY a.attnum
   </code>
   
- excerpt from <https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_generate_tbl_ddl.sql>
   <code>
	SELECT QUOTE_IDENT(a.attname) AS col_name
   FROM pg_namespace AS n
   INNER JOIN pg_class AS c ON n.oid = c.relnamespace
   INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
   LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum
   WHERE c.relkind = 'r'
	 AND a.attnum > 0
	 AND n.nspname = ''
	 AND c.relname = ''
	ORDER BY a.attnum
	</code>

- [PG_TABLE_DEF (aws docs)](https://docs.aws.amazon.com/redshift/latest/dg/r_PG_TABLE_DEF.html)	 
- [Redshift table discovery sql queries (dev blog)](https://oksoft.blogspot.com/2013/03/redshift-tips.html) 



