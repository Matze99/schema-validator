USE LibraryDB;
CREATE TABLE base1.Books3 (
    Id INT PRIMARY KEY IDENTITY(1,1),
    Name VARCHAR(50) NOT NULL,
    Price INT,
    Price FLOAT(30,5)
);