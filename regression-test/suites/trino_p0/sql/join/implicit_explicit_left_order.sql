SELECT p_partkey,
       n_name,
       r_name
FROM   nation,
       region
       LEFT OUTER JOIN part
         ON r_regionkey = p_partkey
WHERE  n_nationkey = r_regionkey

