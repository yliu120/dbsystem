PRAGMA page_size = 8192;
PRAGMA cache_size = 32678;

CREATE TABLE PART (
  P_PARTKEY integer,
  P_NAME varchar(55),
  P_MFGR char(25),
  P_BRAND char(10),
  P_TYPE varchar(25),
  P_SIZE integer,
  P_CONTAINER char(10),
  P_RETAILPRICE decimal,
  P_COMMENT varchar(23)
);

CREATE TABLE SUPPLIER (
  S_SUPPKEY integer,
  s_NAME char(25),
  S_ADDRESS varchar(40),
  S_NATIONKEY integer,
  S_PHONE char(15),
  S_ACCTBAL decimal,
  S_COMMENT varchar(101)
);

CREATE TABLE PARTSUPP (
  PS_PARTKEY integer,
  PS_SUPPKEY integer,
  PS_AVAILQTY integer,
  PS_SUPPLYCOST decimal,
  PS_COMMENT varchar(199)
);

CREATE TABLE CUSTOMER (
  C_CUSTKEY integer,
  C_NAME varchar(25),
  C_ADDRESS varchar(40),
  C_NATIONKEY integer,
  C_PHONE char(15),
  C_ACCTBAL decimal,
  C_MKTSEGMENT char(10),
  C_COMMENT varchar(117)
);

CREATE TABLE ORDERS (
  O_ORDERKEY integer,
  O_CUSTKEY integer,
  O_ORDERSTATUS char(1),
  O_TOTALPRICE decimal,
  O_ORDERDATE date,
  O_ORDERPRIORITY char(15),
  O_CLERK char(15),
  O_SHIPPRIORITY integer,
  O_COMMENT varchar(79)
);

CREATE TABLE LINEITEM (
  L_ORDERKEY integer,
  L_PARTKEY integer,
  L_SUPPKEY integer,
  L_LINENUMBER integer,
  L_QUANTITY decimal,
  L_EXTENDEDPRICE decimal,
  L_DISCOUNT decimal,
  L_TAX decimal,
  L_RETURNFLAG char(1),
  L_LINESTATUS char(1),
  L_SHIPDATE date,
  L_COMMITDATE date,
  L_RECEIPTDATE date,
  L_SHIPINSTRUCT char(25),
  L_SHIPMODE char(10),
  L_COMMENT varchar(44)
);

CREATE TABLE NATION (
  N_NATIONKEY integer,
  N_NAME char(25),
  N_REGIONKEY integer,
  N_COMMENT varchar(152)
);

CREATE TABLE REGION (
  R_REGIONKEY integer,
  R_NAME char(25),
  R_COMMENT varchar(152)
);

.import /home/cs416/datasets/hw0/tpch-sf0.1/part.csv PART
.import /home/cs416/datasets/hw0/tpch-sf0.1/lineitem.csv LINEITEM
.import /home/cs416/datasets/hw0/tpch-sf0.1/supplier.csv SUPPLIER
.import /home/cs416/datasets/hw0/tpch-sf0.1/orders.csv ORDERS
.import /home/cs416/datasets/hw0/tpch-sf0.1/partsupp.csv PARTSUPP
.import /home/cs416/datasets/hw0/tpch-sf0.1/customer.csv CUSTOMER
.import /home/cs416/datasets/hw0/tpch-sf0.1/nation.csv NATION
.import /home/cs416/datasets/hw0/tpch-sf0.1/region.csv REGION

.timer on
select p.p_name, s.s_name
from part p, supplier s, partsupp ps
where p.p_partkey = ps.ps_partkey
  and ps.ps_suppkey = s.s_suppkey
  and ps.ps_availqty = 1
union all
select p.p_name, s.s_name
from part p, supplier s, partsupp ps
where p.p_partkey = ps.ps_partkey
  and ps.ps_suppkey = s.s_suppkey
  and ps.ps_supplycost < 5;

select PART.P_NAME, count(*) as count
 from PART, LINEITEM
 where PART.P_PARTKEY = LINEITEM.L_PARTKEY and LINEITEM.L_RETURNFLAG = 'R'
 group by PART.P_PARTKEY;

create table temp as
   select n.n_name as nation, p.p_name as p_part, sum(l.l_quantity) as num
   from customer c, nation n, orders o, lineitem l, part p
   where c.c_nationkey = n.n_nationkey
     and c.c_custkey = o.o_custkey
     and o.o_orderkey = l.l_orderkey
     and l.l_partkey = p.p_partkey
   group by n.n_name, p.p_name;

 select nation, max(num)
  from temp
  group by nation;