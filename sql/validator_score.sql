create table validator_score(
    block_height int,
    operator_address varchar(100),
    validators_count_per_block numeric,
    total_token_amt_per_block numeric,
    total_self_bonded_amt_per_block numeric,
    tokens_proportion numeric,
    voting_power_score numeric,
    comission_score numeric,
    self_bonded_score numeric,
    vote_propose_score numeric,
    score numeric,
    primary key (block_height, operator_address)
);
