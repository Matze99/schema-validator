USE LibraryDB;
CREATE TABLE Books (
    Id INT PRIMARY KEY IDENTITY(1,1),
    Name VARCHAR(50) NOT NULL,
    Price INT
);

ALTER TABLE Books ADD ISBN INT NOT NULL;