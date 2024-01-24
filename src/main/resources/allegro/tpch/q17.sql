select sum(cast(l_extendedprice as double)) / 7.0 as avg_yearly
from lineitem, part
where p_partkey = l_partkey
        and p_brand = 'Brand#51'
        and p_container = 'WRAP PACK'
        and cast(l_quantity as double) < (
                select cast(0.2 * avg(l_quantity) as double)
                from lineitem
                where l_partkey = p_partkey)