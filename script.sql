CREATE DATABASE ProyectoETL;

USE ProyectoETL;

CREATE TABLE dbo.CompanyProfiles (
  Symbol varchar(10) NOT NULL,
  Company varchar(80) NOT NULL,
  Sector varchar(80) NULL,
  SubSector varchar(80) NULL,
  HeadQuarter varchar(80) NULL,
  IncorporationDate date NULL,
  CentralIndexKey varchar(10) NULL,
  FundationYear varchar(50) NULL,
  CONSTRAINT PK_CompanyProfiles_Symbol
    PRIMARY KEY (Symbol)
);

CREATE TABLE dbo.Companies (
  Date date NOT NULL,
  Symbol varchar(10) NOT NULL,
  ClosePrice float NOT NULL,
  CONSTRAINT PK_Companies_Date_Symbol
    PRIMARY KEY (Date, Symbol)
);