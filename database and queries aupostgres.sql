-- Creating 'publications' table with constraints
CREATE TABLE publications (
    id SERIAL PRIMARY KEY,
    submitter VARCHAR(255) NOT NULL,
    title TEXT NOT NULL CHECK (title <> '' AND char_length(title) > 1),
    comments TEXT,
    journal_ref VARCHAR(255),
    doi VARCHAR(255) UNIQUE,
    report_no VARCHAR(255) UNIQUE,
    categories VARCHAR(255),
    license VARCHAR(255),
    abstract TEXT,
    publication_type VARCHAR(255),
    update_date DATE
);

ALTER TABLE publications
ADD CONSTRAINT unique_title UNIQUE (title);

CREATE TABLE citations (
    id SERIAL PRIMARY KEY,
    publication_id INT NOT NULL,
    title TEXT NOT NULL,
    author VARCHAR(255),
    year INT,
    FOREIGN KEY (publication_id) REFERENCES publications(id)
);



-- Creating 'authors' table with constraints
CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL CHECK (name <> ''),
    affiliation VARCHAR(255) NOT NULL CHECK (affiliation <> '')
);

-- Creating 'publication_venues' table with constraints
CREATE TABLE publication_venues (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL CHECK (name <> ''),
    impact_factor FLOAT CHECK (impact_factor >= 0)
);

-- Creating 'categories' table with constraints
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL CHECK (category_name <> '')
);

-- Creating 'authorship' table with foreign key constraints
CREATE TABLE authorship (
    publication_id INT NOT NULL,
    author_id INT NOT NULL,
    PRIMARY KEY (publication_id, author_id),
    FOREIGN KEY (publication_id) REFERENCES publications(id),
    FOREIGN KEY (author_id) REFERENCES authors(id)
);

-- Creating 'publication_category' table with foreign key constraints
CREATE TABLE publication_category (
    publication_id INT NOT NULL,
    category_id INT NOT NULL,
    PRIMARY KEY (publication_id, category_id),
    FOREIGN KEY (publication_id) REFERENCES publications(id),
    FOREIGN KEY (category_id) REFERENCES categories(id)
);

-- Creating 'log_table' to store changes in the database
CREATE TABLE log_table (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    operation VARCHAR(50) NOT NULL,
    old_values TEXT,
    new_values TEXT,
    operation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);



-- Trigger function for logging changes
CREATE OR REPLACE FUNCTION log_publication_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- Insert log record into 'log_table'
    -- The log now includes the operation type and timestamps
    INSERT INTO log_table (table_name, operation, old_values, new_values, operation_time)
    VALUES ('publications', TG_OP, row_to_json(OLD), row_to_json(NEW), CURRENT_TIMESTAMP);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_log_publications
AFTER INSERT OR UPDATE OR DELETE ON publications
FOR EACH ROW
EXECUTE FUNCTION log_publication_changes();




SELECT a.id, a.name, COUNT(ap.publication_id) AS publication_count
FROM authors a
JOIN authorship ap ON a.id = ap.author_id
GROUP BY a.id, a.name
ORDER BY publication_count DESC;

SELECT 
    p.id AS publication_id, 
    p.title AS publication_title, 
    COUNT(c.id) AS citation_count
FROM 
    publications p
LEFT JOIN 
    citations c ON p.id = c.publication_id
GROUP BY 
    p.id, p.title
ORDER BY 
    citation_count DESC, p.title;




SELECT cat.category_name, COUNT(p.id) AS publication_count
FROM categories cat
JOIN publication_category pc ON cat.id = pc.category_id
JOIN publications p ON pc.publication_id = p.id
GROUP BY cat.category_name
ORDER BY publication_count DESC;













SELECT a.id, a.name, COUNT(ap.publication_id) AS publication_count
FROM authors a
JOIN authorship ap ON a.id = ap.author_id
GROUP BY a.id, a.name
ORDER BY publication_count DESC;


SELECT p.id AS publication_id, p.title AS publication_title, COUNT(c.id) AS citation_count
FROM publications p
LEFT JOIN citations c ON p.id = c.publication_id
GROUP BY p.id, p.title
ORDER BY citation_count DESC;

