create table block_label (
    operator_address varchar(100),
    commission_score double precision,
    voting_power_score double precision,
    tokens_proportion decimal(30, 10),
    commission_rate double precision,
    self_bonded decimal(38, 4),
    self_bonded_score double precision,
    delegator_shares decimal(38, 4),
    tokens decimal(38, 4),
    vote_propose_score double precision,
    label double precision,
    block_height int,
    score double precision,
    primary key (block_height, operator_address)
);