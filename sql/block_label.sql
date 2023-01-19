create table block_label (
    block_height int,
    operator_address varchar(100),
    tokens decimal(38, 4),
    commission_rate double precision,
    self_bonded decimal(38, 4),
    delegators_token decimal(38, 6),
    voting_power_score double precision,
    commission_score double precision,
    self_bonded_score double precision,
    vote_score double precision,
    score double precision,
    label double precision,
    primary key (block_height, operator_address)
);


select count(*) from public.block_label;

select count(*) from (select distinct block_height from public.block_label) as t;