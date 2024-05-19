# Dynamic SQL Server Metadata Synchronization SSIS Project

## Overview

This project provides a set of C# scripts designed to dynamically read and synchronize metadata between a source and destination SQL Server databases. The scripts are intended to be used within SQL Server Integration Services (SSIS) packages. This approach helps handling frequent structural changes in the source database without requiring redeployment, saving time and effort.

Queries are written into SSIS variables, you could exec SqlCommands inside the script if needed. 

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

