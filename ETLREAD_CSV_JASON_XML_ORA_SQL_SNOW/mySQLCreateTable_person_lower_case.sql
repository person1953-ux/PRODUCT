-- Table: public.PERSONS

-- DROP TABLE IF EXISTS public."PERSONS";

CREATE TABLE IF NOT EXISTS public."PERSONS"
(
    name "char",
    height bigint,
    weight bigint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."PERSONS"
    OWNER to postgres;
select * from public."persons"