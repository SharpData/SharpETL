-- workflow=hello_world
--  loadType=incremental
--  logDrivenType=timewindow

-- step=define variable
-- source=temp
-- target=variables

SELECT 'RESULT' AS `OUTPUT_COL`;

-- step=print SUCCESS to console
-- source=temp
-- target=console

SELECT 'SUCCESS' AS `${OUTPUT_COL}`;