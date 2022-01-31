# testpostgres
Postgres Testing Functions

Main functionality
Lists all tables and ratio of table size compared to number if rows in each table
Breaks down central table and related indexes

Example Usage:-
#### ./testpostgres1 docker
Runs using configuration stored in docker.yaml

Example output:-
- Connected successfully!
- table1		3371 DB rows	1.72 MB total	(534 bytes per row)	IDX 216.0 KB total	(65 bytes per row)	TOTAL 1.93 MB total	(599 bytes per row)
