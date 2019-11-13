CREATE SEQUENCE active_test_upload_o4_2g_id_seq INCREMENT 1 START 1;
CREATE TABLE public.active_test_upload_o4_2g
(
    index integer NOT NULL DEFAULT nextval('active_test_upload_o4_2g_id_seq'::regclass),
    hex_id character varying(15) COLLATE pg_catalog."default",
    measoperatorcode character varying(15) COLLATE pg_catalog."default",
    measnettype character varying(15) COLLATE pg_catalog."default",
    meastesttype integer,
    value double precision,
    count integer,
    bin_size integer,
    geom geometry(Polygon,4326),
    CONSTRAINT "active_test_upload_o4_2g_pkey" PRIMARY KEY (index)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.active_test_upload_o4_2g
    OWNER to postgres;