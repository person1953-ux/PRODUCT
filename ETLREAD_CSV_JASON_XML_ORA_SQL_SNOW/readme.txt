PROJECT etl_read_csv_jason_xml_to_oracle_mysql_databricks_production.py
1)The python scripts use padas library loop through a directory and read all the files
in this directory (in this hand-ons there are 9 files whose types are
CSV, JASON, XML) to a  pandas data frame
2) Regardless different type of the files (number of attributes/items  will be the same
in this hand-ons)
3) Data are concatinated in one pandas data frame  and output to one csv file.
4) Then data from dataframe is used inserting to 3 diferrent database tables:
   Oracle, mySQL and slnowflakes


