DROP KEYSPACE IF EXISTS citizens_statistics;
CREATE KEYSPACE citizens_statistics WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};

CREATE TABLE citizens_statistics.citizens (id TEXT PRIMARY KEY, passportNumber TEXT, ageCategory TEXT, mean_salary INT, mean_trips INT );
CREATE TABLE citizens_statistics.data (id INT PRIMARY KEY, passportNumber TEXT, month INT, data INT );