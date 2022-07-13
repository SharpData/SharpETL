-- workflow=sftp_test
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=sftp
--  configPrefix=sftp-ticketflap
--  path=/Users/yangwliu/sftp
--  destinationDir=/Users/yangwliu/sftp-copy
--  fileNamePattern=edw_sales_thodw
--  sourceDir=/users/yangwliu/sftp
--  readAll=false
-- target=variables

-- step=2
-- source=csv
--  filePath=/Users/yangwliu/sftp-copy/edw_sales_thodw_20211107.txt
--  sep=|
--  fileNamePattern=edw_sales_thodw
--  sourceDir=/users/yangwliu/sftp
-- target=variables

