CREATE TABLE public.authors (
    id int PRIMARY KEY,
    name varchar(255)
);

INSERT INTO public.authors
    (id, name)
VALUES
    (1, 'J. k. Rowling'),
    (2, 'Stephen King'),
    (3, 'Timothy Ferris');