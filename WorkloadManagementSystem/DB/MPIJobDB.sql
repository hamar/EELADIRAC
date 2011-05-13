-- $Header: $

-- ------------------------------------------------------------------------------
--
--  Schema definition for the PilotAgentsDB database - containing the job status
--  history ( logging ) information
-- -
-- ------------------------------------------------------------------------------
DROP DATABASE IF EXISTS MPIJobDB;

CREATE DATABASE MPIJobDB;

-- ------------------------------------------------------------------------------
-- Database owner definition

USE mysql;
DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
USE MPIJobDB;
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS Rings;
CREATE TABLE Rings (
    RingID INTEGER NOT NULL AUTO_INCREMENT,
    JobID INTEGER NOT NULL,
    Status VARCHAR(31) NOT NULL,
    Site VARCHAR(63) NOT NULL,
    CE VARCHAR(63) NOT NULL,
    Platform VARCHAR(63) NOT NULL, 
    Master VARCHAR(63) NOT NULL,
    Port INTEGER NOT NULL,
    NumberOfProcessorsJob INTEGER NOT NULL,
    NumberOfProcessorsRing INTEGER NOT NULL,
    TimeNew DATETIME NOT NULL,
    LastTimeUpdate DATETIME NOT NULL,
    Flavor VARCHAR(31),
    ExecutionTime DATETIME,
    PRIMARY KEY (JobID,RingID)
);

-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS MPIPilots;
CREATE TABLE MPIPilots (
    PilotID INTEGER NOT NULL AUTO_INCREMENT,
    PilotJobReference VARCHAR(255) NOT NULL DEFAULT 'Unknown',
    JobID INTEGER NOT NULL,
    RingID INTEGER NOT NULL, 
    PilotType VARCHAR(31) NOT NULL, 
    Status VARCHAR(63) NOT NULL,
    Hostname VARCHAR(63) NOT NULL,
    ResourceJDL BLOB NOT NULL DEFAULT '',
    Rank INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (JobID, RingID, PilotID)
);
