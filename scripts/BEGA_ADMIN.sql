
create or replace database BEGA_ADMIN COMMENT='Administrative Database';

create or replace schema BEGA_ADMIN.CONTROL;

create or replace TABLE BEGA_ADMIN.CONTROL.DATA_QUALITY_NOTIFICATION (
	NOTIFICATION_NAME VARCHAR(16777216) NOT NULL,
	NOTIFICATION_EMAIL_ADRESSES VARCHAR(16777216) NOT NULL,
	primary key (NOTIFICATION_NAME)
);
create or replace TABLE BEGA_ADMIN.CONTROL.DATA_QUALITY_VALIDATION_RULE (
	RULE_NAME VARCHAR(16777216) NOT NULL,
	RULE_SQL VARCHAR(16777216) NOT NULL,
	NOTIFICATION_NAME VARCHAR(16777216) NOT NULL,
	RULE_ACTIVE BOOLEAN NOT NULL,
	primary key (RULE_NAME),
	foreign key (NOTIFICATION_NAME) references BEGA_ADMIN.CONTROL.DATA_QUALITY_NOTIFICATION(NOTIFICATION_NAME)
);
CREATE OR REPLACE PROCEDURE BEGA_ADMIN.CONTROL.EXECUTE_DATA_QUALITY_VALIDATIONS()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    // Query the control table the get the distinct notification audiences
    var control_table_notif_query = `
        select distinct NOTIFICATION_NAME
        from BEGA_ADMIN.CONTROL.DATA_QUALITY_VALIDATION_RULE
        where RULE_ACTIVE
        `;

    var control_table_notif_result = snowflake.execute({sqlText: control_table_notif_query});

    // Loop through the notification audience records
    while (control_table_notif_result.next()) {

        // Declare variables
        var email_content = '''';
        var total_rows = 0;
        var total_errors = 0;

        // Query the control table
        var control_table_query = `
            select
                r.*,
                NOTIFICATION_EMAIL_ADRESSES
            from 
                BEGA_ADMIN.CONTROL.DATA_QUALITY_VALIDATION_RULE as r
                inner join BEGA_ADMIN.CONTROL.DATA_QUALITY_NOTIFICATION as n on r.NOTIFICATION_NAME = n.NOTIFICATION_NAME
            where
                r.RULE_ACTIVE
                and r.NOTIFICATION_NAME = ''` + control_table_notif_result.getColumnValue(''NOTIFICATION_NAME'') + `''
            `;

        var control_table_result = snowflake.execute({sqlText: control_table_query});

        // Loop through the control table records
        while (control_table_result.next()) {
            // Get email address
            var email_address = control_table_result.getColumnValue(''NOTIFICATION_EMAIL_ADRESSES'');

            // Compose the email content
            email_content += ''- '' + control_table_result.getColumnValue(''RULE_NAME'') + '':\\n    '';
            
            // Get the SQL statement from the column
            var execute_query = control_table_result.getColumnValue(''RULE_SQL'');
            // Execute the SQL statement
            try {
                var execute_result = snowflake.execute({sqlText: execute_query});
                if (execute_result.getRowCount() > 0) {
                    email_content += ''FAILED ('' + execute_result.getRowCount() + '' rows returned)'';
                    total_rows += execute_result.getRowCount();
                    }
                else {
                    email_content += ''PASSED'';
                    }
                }
            catch (err) {
                email_content += ''COULD NOT BE VALIDATED: '' + err.message.replace(/\\''/g, '''').replace(/(\\r\\n|\\n|\\r)/gm, '' '');
                total_errors++;
                }

            email_content += ''\\n\\n'';
        }

        var notification_query = `
            call system$send_email(
                ''bega_email_integration'',
                ''` + email_address + `'',
                ''Snowflake Data Quality Framework Notification'',
                ''Summary (all the below should be zero):\\n`
                    + `- ` + total_rows + ` rows returned\\n`
                    + `- ` + total_errors + ` errors validating\\n\\n`
                    + `Rules:\\n` + email_content + `''
            );`;

        // return notification_query;
        snowflake.execute({sqlText: notification_query});

    }

    return ''Snowflake Data Quality Validation executed successfully'';

';
create or replace task BEGA_ADMIN.CONTROL.TASK_EXECUTE_DATA_QUALITY_VALIDATIONS
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 6 * * * Australia/Sydney'
	COMMENT='Execute the Data Quality Validations'
	as call BEGA_ADMIN.CONTROL.EXECUTE_DATA_QUALITY_VALIDATIONS();
create or replace schema BEGA_ADMIN.PUBLIC;

CREATE OR REPLACE PROCEDURE BEGA_ADMIN.PUBLIC.CREATE_ENV_DB_FROM_MASTER_CLONE("MASTER_CLONE_DB" VARCHAR, "ENVIRONMENT_NAME" VARCHAR, "SUFFIX" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS 'DECLARE
    prod_source_db string :=  replace(:master_clone_db, ''MASTR'', ''PROD'');
    dev_db string;
    stmt string;
    res_schema_grants RESULTSET;
    res_future_schema_grants RESULTSET;
    db_owner_role string;

    // grants on db into a cursor
    res1 RESULTSET DEFAULT (show grants on database identifier( :prod_source_db));
    c1 CURSOR FOR res1;

    // list of schemas into a cursor
    res2 RESULTSET DEFAULT (show schemas in database identifier( :prod_source_db));
    c2 CURSOR FOR res2;
    

BEGIN
    //assign database name to new clone depending on if there is a suffix 
    IF (suffix = '''') THEN
        dev_db := replace(:master_clone_db,  ''MASTR'', :environment_name );
    ELSE
        dev_db := replace(:master_clone_db,  ''MASTR'', :environment_name || ''_'' || :suffix);
    END IF;

    //create dev database from master clone
    EXECUTE IMMEDIATE ''CREATE OR REPLACE DATABASE '' || :dev_db || ''  CLONE '' || :master_clone_db || '' ;'';
    FOR record IN c1 DO
    //iterate through database grants and copy from prod
        IF (record."privilege" = ''OWNERSHIP'') THEN
            // revoke privilege on the database to current owner
            EXECUTE IMMEDIATE ''REVOKE ALL PRIVILEGES ON DATABASE '' || :dev_db  || '' FROM ROLE '' || record."grantee_name" || '';'';
            stmt :=  ''CREATE DATABASE ROLE IF NOT EXISTS '' || :dev_db || ''.'' || REPLACE(record."grantee_name", ''PROD'',:environment_name) || '';'';
            EXECUTE IMMEDIATE :stmt;
            stmt :=  ''GRANT '' || record."privilege"  ||  '' ON '' || record."granted_on"  ||'' ''||  :dev_db  || '' TO ROLE '' || REPLACE(record."grantee_name", ''PROD'',:environment_name) || '';'';
            EXECUTE IMMEDIATE :stmt;
        END IF;
        stmt :=  ''CREATE DATABASE ROLE IF NOT EXISTS '' || :dev_db || ''.'' || REPLACE(record."grantee_name", ''PROD'',:environment_name) || '';'';
        EXECUTE IMMEDIATE :stmt;
        //grant database privileges for ownership
        stmt :=  ''GRANT '' || record."privilege"  ||  '' ON '' || record."granted_on"  ||'' ''||  :dev_db  || '' TO ROLE '' || REPLACE(record."grantee_name", ''PROD'',:environment_name) || '';'';
        EXECUTE IMMEDIATE :stmt;

    END FOR;     

    //iterate through schema grants revoking grants brought from clone and granting to dev access roles
    FOR rec in c2 DO      
        IF ((rec."name" != ''INFORMATION_SCHEMA'') OR (rec."name" != ''PUBLIC''))  THEN 
        
            res_schema_grants := (EXECUTE IMMEDIATE ''show grants on schema '' || :prod_source_db || ''.'' || rec."name"  || '';'');
            //iterate through grants on schema
            FOR rec_grant IN res_schema_grants DO 
                IF (rec_grant."privilege" = ''OWNERSHIP'') THEN
                    stmt :=  ''CREATE ROLE IF NOT EXISTS '' || REPLACE(rec_grant."grantee_name", ''PROD'',:environment_name) || '';'';
                    EXECUTE IMMEDIATE :stmt;
                    //grant ownership on schemas
                    EXECUTE IMMEDIATE ''GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE '' || :dev_db || '' TO ROLE '' || REPLACE(rec_grant."grantee_name", ''PROD'',:environment_name)  || '' REVOKE CURRENT GRANTS'';
                ELSE
                    stmt :=  ''CREATE ROLE IF NOT EXISTS '' || REPLACE(rec_grant."grantee_name", ''PROD'',:environment_name) || '';'';
                    EXECUTE IMMEDIATE :stmt;
                    //grant schema level privileges to roles
                    EXECUTE IMMEDIATE ''GRANT ''  || rec_grant."privilege"  ||  '' ON '' || rec_grant."granted_on"  ||'' ''||  :dev_db || ''.'' || rec."name"  || '' TO ROLE '' || REPLACE(rec_grant."grantee_name", ''PROD'',:environment_name) || '' ;'';
                END IF;
            END FOR;

        
            res_future_schema_grants := (EXECUTE IMMEDIATE ''show future grants in schema '' || :prod_source_db || ''.'' || rec."name"  || '';'');
            //iterate through future grants on schema revoking from cloned grants and granting to dev access roles
            FOR rec_f_grant IN res_future_schema_grants DO 
                IF (rec_f_grant."privilege" = ''OWNERSHIP'') THEN
                    // grant ownership on all 
                    EXECUTE IMMEDIATE ''GRANT ''  || rec_f_grant."privilege"  ||  '' ON ALL '' || REPLACE(rec_f_grant."grant_on", ''_'', '' '')  ||''S IN SCHEMA ''||  :dev_db || ''.'' || rec."name"  || '' TO ROLE '' || REPLACE(rec_f_grant."grantee_name", ''PROD'',:environment_name) || '' REVOKE CURRENT GRANTS;'';
                    //revoke future ownership
                    EXECUTE IMMEDIATE ''REVOKE ''  || rec_f_grant."privilege"  ||  '' ON FUTURE '' || REPLACE(rec_f_grant."grant_on", ''_'', '' '')  ||''S IN SCHEMA ''||  :dev_db || ''.'' || rec."name"  || '' FROM ROLE '' || rec_f_grant."grantee_name" || '' ;'';
                    //grant future ownership
                    EXECUTE IMMEDIATE ''GRANT ''  || rec_f_grant."privilege"  ||  '' ON FUTURE '' || REPLACE(rec_f_grant."grant_on", ''_'', '' '')  ||''S IN SCHEMA ''||  :dev_db || ''.'' || rec."name"  || '' TO ROLE '' || REPLACE(rec_f_grant."grantee_name", ''PROD'',:environment_name) || '' ;'';

                
                ELSEIF (rec_f_grant."grant_on" != ''STAGE'') THEN
                    //revoke privilege on oject
                    EXECUTE IMMEDIATE ''REVOKE ''  || rec_f_grant."privilege"  ||  '' ON FUTURE '' || REPLACE(rec_f_grant."grant_on", ''_'', '' '')  ||''S IN SCHEMA ''||  :dev_db || ''.'' || rec."name"  || '' FROM ROLE '' || rec_f_grant."grantee_name" || '' ;'';
                    //grant privilege on object
                    EXECUTE IMMEDIATE ''GRANT ''  || rec_f_grant."privilege"  ||  '' ON ALL '' || REPLACE(rec_f_grant."grant_on", ''_'', '' '')  ||''S IN SCHEMA  ''||  :dev_db || ''.'' || rec."name"  || '' TO ROLE '' || REPLACE(rec_f_grant."grantee_name", ''PROD'',:environment_name) || '' ;'';
                    //grant privilege on future object
                    EXECUTE IMMEDIATE ''GRANT ''  || rec_f_grant."privilege"  ||  '' ON FUTURE '' || REPLACE(rec_f_grant."grant_on", ''_'', '' '')  ||''S IN SCHEMA  ''||  :dev_db || ''.'' || rec."name"  || '' TO ROLE '' || REPLACE(rec_f_grant."grantee_name", ''PROD'',:environment_name) || '' ;'';

                //stages need all revoking first
                ELSEIF (rec_f_grant."grant_on" = ''STAGE'') THEN
                    //revoke all future privilege on stage
                    EXECUTE IMMEDIATE ''REVOKE ALL PRIVILEGES ON FUTURE STAGES IN SCHEMA ''||  :dev_db || ''.'' || rec."name"  || '' FROM ROLE '' || rec_f_grant."grantee_name" || '' ;'';
                    //grant privilege on stage
                    EXECUTE IMMEDIATE ''GRANT ''  || rec_f_grant."privilege"  ||  '' ON ALL '' || REPLACE(rec_f_grant."grant_on", ''_'', '' '')  ||''S IN SCHEMA  ''||  :dev_db || ''.'' || rec."name"  || '' TO ROLE '' || REPLACE(rec_f_grant."grantee_name", ''PROD'',:environment_name) || '' ;'';
                    //grant privilege on future stage
                    EXECUTE IMMEDIATE ''GRANT ''  || rec_f_grant."privilege"  ||  '' ON FUTURE '' || REPLACE(rec_f_grant."grant_on", ''_'', '' '')  ||''S IN SCHEMA  ''||  :dev_db || ''.'' || rec."name"  || '' TO ROLE '' || REPLACE(rec_f_grant."grantee_name", ''PROD'',:environment_name) || '' ;'';
    
                END IF;
            END FOR;

        END IF;
    END FOR;
    
    RETURN ''New database '' || :dev_db || '' created.'';

END';
CREATE OR REPLACE PROCEDURE BEGA_ADMIN.PUBLIC.CREATE_MASTER_CLONE("DATABASE_TO_CLONE" VARCHAR, "CLONE_TIME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS 'DECLARE
    stmt string;
    master_clone_db string;
BEGIN
    master_clone_db :=  replace(:database_to_clone, ''PROD'', ''MASTR'');
    IF (clone_time = '''') THEN
        stmt := ''CREATE OR REPLACE DATABASE '' || :master_clone_db || ''  CLONE '' || :database_to_clone || '';'';
    ELSE
        stmt := ''CREATE OR REPLACE DATABASE '' || :master_clone_db || ''  CLONE '' || :database_to_clone || '' AT (TIMESTAMP => TO_TIMESTAMP_TZ( \\'''' ||:clone_time || ''\\'', \\''yyyy-mm-dd hh24:mi:ss\\''));'' ;
    END IF;

    EXECUTE IMMEDIATE :stmt;
    RETURN ''Database cloned ('' || :database_to_clone|| '') - Created database: '' || :master_clone_db;
END';
CREATE OR REPLACE PROCEDURE BEGA_ADMIN.PUBLIC.GRANT_TO_ACCESS_ROLE("DB_NAME" VARCHAR(16777216), "ROLE_TYPE" VARCHAR(16777216), "SCHEMA_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
	sql_text varchar;
    sql_text_future varchar;
    role_name varchar;
BEGIN
	role_name := DB_NAME || ''.'' || ROLE_TYPE || ''_'' || DB_NAME;
    
    -- ######################################################################################################################
	-- Database - All ROLE_TYPEs
	-- ######################################################################################################################
	sql_text := ''GRANT USAGE ON DATABASE '' || DB_NAME || '' TO DATABASE ROLE '' || role_name || '';'';
	execute immediate :sql_text;

	-- ######################################################################################################################
	-- Schemas
	-- ######################################################################################################################
	sql_text := ''GRANT USAGE'';
	-- Grant Types in addition to USAGE (above)
	IF (ROLE_TYPE = ''CONTRIBUTOR'') THEN
		sql_text := sql_text || '', CREATE TABLE, CREATE DYNAMIC TABLE'';
        sql_text := sql_text || '', CREATE VIEW, CREATE MATERIALIZED VIEW'';
		sql_text := sql_text || '', CREATE FILE FORMAT, CREATE FUNCTION, CREATE PROCEDURE'';
	ELSEIF (ROLE_TYPE = ''LANDING'') THEN
		sql_text := sql_text || '', CREATE TABLE'';
	END IF;
	-- Future Schemas
    sql_text_future := sql_text || '' ON FUTURE SCHEMAS IN DATABASE '' || DB_NAME;
	-- On Schema or Database
	IF (SCHEMA_NAME is null) THEN
		sql_text := sql_text || '' ON ALL SCHEMAS IN DATABASE '' || DB_NAME;
	ELSE
		sql_text := sql_text || '' ON SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
	END IF;
    -- TO
    sql_text := sql_text || '' TO DATABASE ROLE '' || role_name || '';'';
    sql_text_future := sql_text_future || '' TO DATABASE ROLE '' || role_name || '';'';
    execute immediate :sql_text;
    execute immediate :sql_text_future;

	-- ######################################################################################################################
	-- Tables
	-- ######################################################################################################################
	sql_text := ''GRANT '';
	-- Grant Types in addition to USAGE (above)
	IF (ROLE_TYPE = ''READER'' or ROLE_TYPE = ''POWERREADER'') THEN
		sql_text := sql_text || ''SELECT, REFERENCES'';
	ELSEIF (ROLE_TYPE = ''CONTRIBUTOR'') THEN
		sql_text := sql_text || ''ALL'';
	ELSEIF (ROLE_TYPE = ''LANDING'') THEN
		sql_text := sql_text || ''ALL'';
	END IF;
	-- Future Tables
	IF (SCHEMA_NAME is null) THEN
        sql_text_future := sql_text || '' ON FUTURE TABLES IN DATABASE '' || DB_NAME;
	ELSE
        sql_text_future := sql_text || '' ON FUTURE TABLES IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
	END IF;
	-- On Schema or Database
	IF (SCHEMA_NAME is null) THEN
		sql_text := sql_text || '' ON ALL TABLES IN DATABASE '' || DB_NAME;
	ELSE
		sql_text := sql_text || '' ON ALL TABLES IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
	END IF;
    -- TO
    sql_text := sql_text || '' TO DATABASE ROLE '' || role_name || '';'';
    sql_text_future := sql_text_future || '' TO DATABASE ROLE '' || role_name || '';'';
	execute immediate :sql_text;
	execute immediate :sql_text_future;

	-- ######################################################################################################################
	-- Dynamic Tables
	-- ######################################################################################################################
	sql_text := ''GRANT '';
	-- Grant Types in addition to USAGE (above)
	IF (ROLE_TYPE = ''READER'' or ROLE_TYPE = ''POWERREADER'') THEN
		sql_text := sql_text || ''SELECT'';
	ELSEIF (ROLE_TYPE = ''CONTRIBUTOR'') THEN
		sql_text := sql_text || ''ALL'';
	ELSEIF (ROLE_TYPE = ''LANDING'') THEN
		sql_text := sql_text || ''ALL'';
	END IF;
	-- Future Tables
	IF (SCHEMA_NAME is null) THEN
        sql_text_future := sql_text || '' ON FUTURE DYNAMIC TABLES IN DATABASE '' || DB_NAME;
	ELSE
        sql_text_future := sql_text || '' ON FUTURE DYNAMIC TABLES IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
	END IF;
	-- On Schema or Database
	IF (SCHEMA_NAME is null) THEN
		sql_text := sql_text || '' ON ALL DYNAMIC TABLES IN DATABASE '' || DB_NAME;
	ELSE
		sql_text := sql_text || '' ON ALL DYNAMIC TABLES IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
	END IF;
    -- TO
    sql_text := sql_text || '' TO DATABASE ROLE '' || role_name || '';'';
    sql_text_future := sql_text_future || '' TO DATABASE ROLE '' || role_name || '';'';
	execute immediate :sql_text;
	execute immediate :sql_text_future;

	-- ######################################################################################################################
	-- Views
	-- ######################################################################################################################
	sql_text := ''GRANT '';
	-- Grant Types in addition to USAGE (above)
	IF (ROLE_TYPE = ''READER'' or ROLE_TYPE = ''POWERREADER'') THEN
		sql_text := sql_text || ''SELECT, REFERENCES'';
	ELSEIF (ROLE_TYPE = ''CONTRIBUTOR'') THEN
		sql_text := sql_text || ''ALL'';
	ELSEIF (ROLE_TYPE = ''LANDING'') THEN
		sql_text := sql_text || ''ALL'';
	END IF;
	-- Future Tables
	IF (SCHEMA_NAME is null) THEN
        sql_text_future := sql_text || '' ON FUTURE VIEWS IN DATABASE '' || DB_NAME;
	ELSE
        sql_text_future := sql_text || '' ON FUTURE VIEWS IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
	END IF;
	-- On Schema or Database
	IF (SCHEMA_NAME is null) THEN
		sql_text := sql_text || '' ON ALL VIEWS IN DATABASE '' || DB_NAME;
	ELSE
		sql_text := sql_text || '' ON ALL VIEWS IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
	END IF;
    -- TO
    sql_text := sql_text || '' TO DATABASE ROLE '' || role_name || '';'';
    sql_text_future := sql_text_future || '' TO DATABASE ROLE '' || role_name || '';'';
	execute immediate :sql_text;
	execute immediate :sql_text_future;

	-- ######################################################################################################################
	-- Materialised Views
	-- ######################################################################################################################
	sql_text := ''GRANT '';
	-- Grant Types in addition to USAGE (above)
	IF (ROLE_TYPE = ''READER'' or ROLE_TYPE = ''POWERREADER'') THEN
		sql_text := sql_text || ''SELECT, REFERENCES'';
	ELSEIF (ROLE_TYPE = ''CONTRIBUTOR'') THEN
		sql_text := sql_text || ''ALL'';
	ELSEIF (ROLE_TYPE = ''LANDING'') THEN
		sql_text := sql_text || ''ALL'';
	END IF;
	-- Future Tables
	IF (SCHEMA_NAME is null) THEN
        sql_text_future := sql_text || '' ON FUTURE MATERIALIZED VIEWS IN DATABASE '' || DB_NAME;
	ELSE
        sql_text_future := sql_text || '' ON FUTURE MATERIALIZED VIEWS IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
	END IF;
	-- On Schema or Database
	IF (SCHEMA_NAME is null) THEN
		sql_text := sql_text || '' ON ALL MATERIALIZED VIEWS IN DATABASE '' || DB_NAME;
	ELSE
		sql_text := sql_text || '' ON ALL MATERIALIZED VIEWS IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
	END IF;
    -- TO
    sql_text := sql_text || '' TO DATABASE ROLE '' || role_name || '';'';
    sql_text_future := sql_text_future || '' TO DATABASE ROLE '' || role_name || '';'';
	execute immediate :sql_text;
	execute immediate :sql_text_future;

	-- ######################################################################################################################
	-- File Formats
	-- ######################################################################################################################
	IF (ROLE_TYPE != ''READER'' and ROLE_TYPE != ''POWERREADER'') THEN
        sql_text := ''GRANT USAGE'';
    	-- Future Tables
    	IF (SCHEMA_NAME is null) THEN
            sql_text_future := sql_text || '' ON FUTURE FILE FORMATS IN DATABASE '' || DB_NAME;
    	ELSE
            sql_text_future := sql_text || '' ON FUTURE FILE FORMATS IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
    	END IF;
    	-- On Schema or Database
    	IF (SCHEMA_NAME is null) THEN
    		sql_text := sql_text || '' ON ALL FILE FORMATS IN DATABASE '' || DB_NAME;
    	ELSE
    		sql_text := sql_text || '' ON ALL FILE FORMATS IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
    	END IF;
        -- TO
        sql_text := sql_text || '' TO DATABASE ROLE '' || role_name || '';'';
        sql_text_future := sql_text_future || '' TO DATABASE ROLE '' || role_name || '';'';
    	execute immediate :sql_text;
    	execute immediate :sql_text_future;
	END IF;

	-- ######################################################################################################################
	-- Functions
	-- ######################################################################################################################
	IF (ROLE_TYPE != ''READER'' and ROLE_TYPE != ''POWERREADER'') THEN
        sql_text := ''GRANT USAGE'';
    	-- Future Tables
    	IF (SCHEMA_NAME is null) THEN
            sql_text_future := sql_text || '' ON FUTURE FUNCTIONS IN DATABASE '' || DB_NAME;
    	ELSE
            sql_text_future := sql_text || '' ON FUTURE FUNCTIONS IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
    	END IF;
    	-- On Schema or Database
    	IF (SCHEMA_NAME is null) THEN
    		sql_text := sql_text || '' ON ALL FUNCTIONS IN DATABASE '' || DB_NAME;
    	ELSE
    		sql_text := sql_text || '' ON ALL FUNCTIONS IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
    	END IF;
        -- TO
        sql_text := sql_text || '' TO DATABASE ROLE '' || role_name || '';'';
        sql_text_future := sql_text_future || '' TO DATABASE ROLE '' || role_name || '';'';
    	execute immediate :sql_text;
    	execute immediate :sql_text_future;
	END IF;

	-- ######################################################################################################################
	-- Procedures
	-- ######################################################################################################################
	IF (ROLE_TYPE != ''READER'' and ROLE_TYPE != ''POWERREADER'') THEN
        sql_text := ''GRANT USAGE'';
    	-- Future Tables
    	IF (SCHEMA_NAME is null) THEN
            sql_text_future := sql_text || '' ON FUTURE PROCEDURES IN DATABASE '' || DB_NAME;
    	ELSE
            sql_text_future := sql_text || '' ON FUTURE PROCEDURES IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
    	END IF;
    	-- On Schema or Database
    	IF (SCHEMA_NAME is null) THEN
    		sql_text := sql_text || '' ON ALL PROCEDURES IN DATABASE '' || DB_NAME;
    	ELSE
    		sql_text := sql_text || '' ON ALL PROCEDURES IN SCHEMA '' || DB_NAME || ''.'' || SCHEMA_NAME;
    	END IF;
        -- TO
        sql_text := sql_text || '' TO DATABASE ROLE '' || role_name || '';'';
        sql_text_future := sql_text_future || '' TO DATABASE ROLE '' || role_name || '';'';
    	execute immediate :sql_text;
    	execute immediate :sql_text_future;
	END IF;
    
    -- RETURN sql_text || ''  >>>>  '' || sql_text_future;
	RETURN ''Succeeded'';

END';