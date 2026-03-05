-- select
--     table_name,
--     pg_size_pretty(pg_total_relation_size(quote_ident(table_name))),
--     pg_total_relation_size(quote_ident(table_name))
-- from information_schema.tables
-- where table_schema = 'public'
-- order by 3 desc;

VACUUM FULL VERBOSE public.hfj_spidx_token;
VACUUM FULL VERBOSE public.hfj_spidx_string;
VACUUM FULL VERBOSE public.hfj_res_ver;
VACUUM FULL VERBOSE public.hfj_res_link;
VACUUM FULL VERBOSE public.hfj_spidx_quantity;

DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC
        LIMIT 10
    LOOP
        EXECUTE 'ANALYZE VERBOSE ' || quote_ident(r.table_name);
    END LOOP;
END;
$$;
