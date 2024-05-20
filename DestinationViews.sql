USE DataBase


CREATE view [dbo].[TablesAndColumnsDestination] AS 
select
	t1.TABLE_SCHEMA Schema
	, LEFT(t1.TABLE_NAME, CHARINDEX('$',t1.TABLE_NAME)) Prefix
	, CASE 
		WHEN CHARINDEX('$', t1.TABLE_NAME, CHARINDEX('$', t1.TABLE_NAME) + 1)>0 THEN 
		-- There is sufix
			SUBSTRING(
				t1.TABLE_NAME
				,CHARINDEX('$', t1.TABLE_NAME) + 1
				,CHARINDEX('$', t1.TABLE_NAME, CHARINDEX('$', t1.TABLE_NAME) + 1) - CHARINDEX('$', t1.TABLE_NAME) - 1)
		WHEN CHARINDEX('$', t1.TABLE_NAME) > 0 THEN 
		-- No sufix but there is prefix
			SUBSTRING(t1.TABLE_NAME,CHARINDEX('$', t1.TABLE_NAME) + 1, LEN(t1.TABLE_NAME) -CHARINDEX('$', t1.TABLE_NAME) + 1)
		ELSE
		-- No sufix no prefix
			t1.TABLE_NAME
      END AS Datamart
	, CASE 
		WHEN CHARINDEX('$', t1.TABLE_NAME, CHARINDEX('$', t1.TABLE_NAME) + 1)>0 THEN 
		-- There is sufix
			SUBSTRING(
				t1.TABLE_NAME
				,CHARINDEX('$', t1.TABLE_NAME, CHARINDEX('$', t1.TABLE_NAME) + 1)
				,LEN(t1.TABLE_NAME) - CHARINDEX('$', t1.TABLE_NAME, CHARINDEX('$', t1.TABLE_NAME) + 1) + 1)
		ELSE 
		-- No sufix
			''
	  END AS Sufix
	, t1.COLUMN_NAME Column
	, CASE 
		WHEN DATA_TYPE = 'timestamp' THEN 'varbinary(8)'
		WHEN t1.CHARACTER_MAXIMUM_LENGTH is null THEN DATA_TYPE 
		ELSE DATA_TYPE + '('+ CAST(CHARACTER_MAXIMUM_LENGTH as varchar(10)) + ')' END AS DataType
	, CAST(CASE WHEN t3.COLUMN_NAME is null THEN 0 ELSE 1 END AS bit) AS IsPKField
from information_schema.columns t1
	left join INFORMATION_SCHEMA.TABLE_CONSTRAINTS t2
		on t1.TABLE_SCHEMA = t2.TABLE_SCHEMA
		and t1.TABLE_NAME = t2.TABLE_NAME
		and t1.TABLE_CATALOG = t2.TABLE_CATALOG
		and t2.CONSTRAINT_TYPE = 'PRIMARY KEY'
	left join INFORMATION_SCHEMA.KEY_COLUMN_USAGE t3
		on t2.CONSTRAINT_NAME = t3.CONSTRAINT_NAME
		and t1.TABLE_SCHEMA = t3.TABLE_SCHEMA
		and t1.TABLE_NAME = t3.TABLE_NAME
		and t1.COLUMN_NAME = t3.COLUMN_NAME
where t1.TABLE_SCHEMA = 'dbo'
GO


CREATE VIEW dbo.vBadColumnsDeltaTrans AS 
select
	CASE 
		WHEN CHARINDEX('$', t1.TABLE_NAME, CHARINDEX('$', t1.TABLE_NAME) + 1)>0 THEN 
		-- There is sufix
			SUBSTRING(
				t1.TABLE_NAME
				,CHARINDEX('$', t1.TABLE_NAME) + 1
				,CHARINDEX('$', t1.TABLE_NAME, CHARINDEX('$', t1.TABLE_NAME) + 1) - CHARINDEX('$', t1.TABLE_NAME) - 1)
		WHEN CHARINDEX('$', t1.TABLE_NAME) > 0 THEN 
		-- No sufix there is prefix
			SUBSTRING(t1.TABLE_NAME,CHARINDEX('$', t1.TABLE_NAME) + 1, LEN(t1.TABLE_NAME) -CHARINDEX('$', t1.TABLE_NAME) + 1)
		ELSE
		-- No prefix no sufix
			t1.TABLE_NAME
      END AS Datamart
	, t1.COLUMN_NAME Column	
from information_schema.columns t1
	left join INFORMATION_SCHEMA.TABLE_CONSTRAINTS t2
		on t1.TABLE_SCHEMA = t2.TABLE_SCHEMA
		and t1.TABLE_NAME = t2.TABLE_NAME
		and t1.TABLE_CATALOG = t2.TABLE_CATALOG
		and t2.CONSTRAINT_TYPE = 'PRIMARY KEY'
	left join INFORMATION_SCHEMA.KEY_COLUMN_USAGE t3
		on t2.CONSTRAINT_NAME = t3.CONSTRAINT_NAME
		and t1.TABLE_SCHEMA = t3.TABLE_SCHEMA
		and t1.TABLE_NAME = t3.TABLE_NAME
		and t1.COLUMN_NAME = t3.COLUMN_NAME
where t1.TABLE_SCHEMA = 'deltaTrans'
	and CAST(CASE WHEN t3.COLUMN_NAME is null THEN 0 ELSE 1 END AS bit) = 0
GO
