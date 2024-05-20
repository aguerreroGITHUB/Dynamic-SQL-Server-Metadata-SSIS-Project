SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE view [dbo].[vSSISDatamarts] AS 
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
		--There is sufix
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
		ELSE DATA_TYPE + '('+ CAST(t1.CHARACTER_MAXIMUM_LENGTH as varchar(10)) + ')' END AS DataType
	, t2.TABLE_NAME as FullTable
from DataBase.INFORMATION_SCHEMA.TABLES t2
	inner join DataBase.information_schema.columns t1
		on  t1.TABLE_NAME = t2.TABLE_NAME
where TABLE_TYPE = 'BASE TABLE'
	and t2.TABLE_SCHEMA = 'dbo'
GO



CREATE view [dbo].[TablesAndColumnsSource] AS 
select
	 t1.[Schema]
	,t1.[Prefix]
	,t1.[Datamart]
	,t1.[Sufix]
	,t1.[Column]
	,t1.[DataType]
	, CAST(CASE WHEN t4.COLUMN_NAME is null THEN 0 ELSE 1 END AS bit) AS IsPKField
from [dbo].[vSSISDatamarts] t1
	inner join dbo.SSISDatamarts t2
		on t1.Datamart = t2.Datamart 
	left join DataBase.INFORMATION_SCHEMA.TABLE_CONSTRAINTS t3
		on t3.CONSTRAINT_TYPE = 'PRIMARY KEY'
		and t1.FullTable = t3.TABLE_NAME
	left join DataBase.INFORMATION_SCHEMA.KEY_COLUMN_USAGE t4
		on  t3.CONSTRAINT_NAME = t4.CONSTRAINT_NAME
		and t1.Column = t4.COLUMN_NAME
		and t1.FullTable = t4.TABLE_NAME

GO



