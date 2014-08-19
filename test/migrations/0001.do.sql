CREATE TABLE call_records 
(
	imsi varchar(15),
	timestamp timestamp,
	duration int,
	calling_number varchar(15),
	called_number varchar(15),
	primary key(imsi, timestamp)
);