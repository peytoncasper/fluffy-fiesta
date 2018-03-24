CREATE KEYSPACE test WITH replication={'class':'SimpleStrategy', 'replication_factor':'3'};

CREATE TABLE test.paging_test (
  id int PRIMARY KEY,
  description text
);