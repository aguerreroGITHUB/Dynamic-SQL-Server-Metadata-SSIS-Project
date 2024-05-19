# Dynamic SQL Server Metadata Synchronization SSIS Project

## Overview

This project provides a set of C# scripts designed to dynamically read and synchronize metadata between a source and destination SQL Server databases. The scripts are intended to be used within SQL Server Integration Services (SSIS) packages. This approach helps handling frequent structural changes in the source database without requiring redeployment, saving time and effort.

Queries are written into SSIS variables, you could exec SqlCommands inside the script if needed. 

## How it works

**Logic**: Initially a system view collects the data from your tables. Next, the SSIS package will be executed. In this package you iterate each table name (stored in the DWC in a configuration table) and collect the source and target column names, separating in different lists the columns that are part of a PK constraint from those that are not. Once it has these lists, it compares the columns between source and destination. If there are no differences, queries will be built to do a quick dump. If there were differences, you would build queries to transform the target table and match it with the source table.

**Table rebuilding**: If it is a deleted column, it is deleted and that's it. If it is a modified column, an ALTER COLUMN is launched and that's it (we assume that the data type change is compatible). If it is a new column, it will be loaded into a temporary table (deltaTrans schema) that will only have the PK fields and this column, and if a reload bit in rebuild is active, after executing ADD COLUMN an update will be triggered for that column.

**Date synchronization**: Once the structures are matched, a configurable period is deleted (or the table is truncated, depending on the data volume of the table) and the new data block is bulk inserted. If a new column arrives with all its data set to null, it would not be necessary to have the reload new columns bit active, that is why the functionality is allowed.

## Requirements

- Microsoft SQL Server
- SQL Server Integration Services (SSIS)

## Main features

- **Dynamic Metadata Comparison**: Compares column names and data types between the source and destination databases.
- **Flexible Data Synchronization**: Synchronizes data based on configurable date periods (Sync method for massive tables).
- **Automated SQL Query Generation**: Generates necessary SQL queries to handle new columns, removed columns, and data type changes.

## Script usage

- **MetadataComparison.cs**: Compares the metadata between the source and destination databases to enable structural sync only when it is needed.
- **QueryBuilding.cs**: Generates required SQL queries for metadata management and data synchronization.
- **DateSync.cs**: Synchronizes data from the source to the destination based on a "n-days" logic.
- **SourceStructure.sql**: View that brings the structure to the package.
- **DestinationStructure.sql**: View that brings the structure to the package.

