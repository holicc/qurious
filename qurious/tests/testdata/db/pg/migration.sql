-- create role table
CREATE TABLE IF NOT EXISTS public.schools (
    id INT PRIMARY KEY,
    name VARCHAR
);

-- insert initial default roles
INSERT INTO public.schools (id,name) VALUES (0,'QingHua University'),(1,'BeiJing University');