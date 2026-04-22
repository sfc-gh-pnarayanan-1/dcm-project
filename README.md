# DCM Project for DEMO Database

This project manages the `DEMO` database in Snowflake using DCM (Database Change Management).

## Project Structure

```
dcm-project/
├── manifest.yml                        ← Targets (PROD/DEV) and templating config
├── pre_deploy.sql                      ← External access integrations (run before deploy)
├── post_deploy.sql                     ← Procedures, UDTFs, sequences (run after deploy)
├── README.md
└── sources/definitions/
    ├── infrastructure.sql              ← Database, schemas, stages
    ├── dt_demo_tables.sql              ← Tables in DT_DEMO schema
    ├── restro_tables.sql               ← Tables in RESTRO schema
    ├── analytics.sql                   ← Dynamic tables
    ├── serve.sql                       ← Views
    └── access.sql                      ← Grants
```

## Schemas

| Schema | Description |
|---|---|
| `PUBLIC` | General-purpose objects, views, procedures |
| `DT_DEMO` | Dynamic table demo with customer/sales/product data |
| `RESTRO` | Restaurant POS billing system (Posist integration) |

## Setup Instructions

### 1. Create DCM Admin Database and Project Object

```sql
CREATE DATABASE IF NOT EXISTS DCM_ADMIN;
CREATE SCHEMA IF NOT EXISTS DCM_ADMIN.PROJECTS;

-- For PROD target
CREATE OR REPLACE DCM PROJECT DCM_ADMIN.PROJECTS.DEMO_PROJECT_PROD
    COMMENT = 'DCM project for DEMO database - production';

-- For DEV target
CREATE OR REPLACE DCM PROJECT DCM_ADMIN.PROJECTS.DEMO_PROJECT_DEV
    COMMENT = 'DCM project for DEMO database - development';
```

### 2. Import to Snowsight Workspace

1. Connect this GitHub repo to Snowsight via API Integration
2. Create a Workspace from the Git repository
3. Select the project in the DCM control panel
4. Run Plan → verify all objects show as ALTER (adoption)
5. Run Deploy

### 3. Post-Deploy

Run `post_deploy.sql` manually after each deploy to create/update:
- Sequences
- Python UDTFs (data generators)
- Stored procedures
- Transient tables

## Making Changes

1. Create a feature branch in GitHub
2. Edit the relevant definition file
3. Open PR → CI runs plan → review → merge
4. Deploy from Snowsight or Snow CLI
5. Run `post_deploy.sql` if procedures/functions changed
