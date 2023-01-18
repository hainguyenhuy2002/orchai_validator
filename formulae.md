# | 1. Data
+ table **validator_block**:
  + total: 87724 blocks
  + interval: 7059473 -> 9826222
    + 7059473 -> 9583823: (19.18%)
      + distance between 2 rows is 150 blocks
      + the end represents all the interval 
      + no missing blocks
    + 9583823 -> 9826222:
      + distance not stable (1 to 150)
  + attribute:
    + tokens: orai
    + commission rate: %
    + delegators_shared: skip
    + self_bonded: orai * 10^16
    + vote: boolean
    + delegators_token = tokens - self_bonded
+ table **validator_reward**:
  + total: 252737 blocks
  + interval: 9573815 -> 9826424
    + missing 128 blocks
    + distance between 2 rows is 1 block
  + use data from: 9573923 (% 150 == 23)
  + interval: a (% 150 == 23) -> a + 150
    + take all blocks from a to a + 150
    + if missing => skip
    + assign data in table: a + 150 represents the interval from a to a + 150
  + attribute:
    + reward_amount: total reward of validator and delegators
    + commission_amount: reward amount that delegators have to share to validator
    + delegators_reward = commission_amount * (1 - commission_rate) / commission_rate

# | 2. Label (for 1800 blocks ~ 3 hours)
## | Voting power score (attribute **tokens**)
<font size="5">$score = 1 - \frac{tokens}{max\_tokens}$ <br> 
+ if $\frac{tokens}{total\_tokens} > \frac{2}{num\_vals}$ => score = 0
</font>

## | Commission score (attribute **commission_rate**)
<font size="5">$score = 1 - \frac{commission\_rate}{accept\_rate}$ <br> </font>

## | Self bonded score (attribute **self_bonded**)
<font size="5">$score = self\_bonded - \frac{total\_self\_bonded}{num\_vals \times concentration\_level}$ <br> 
+ if $score < 0$ => $score = 0$ <br>

$score = \frac{score}{max\_score}$
</font>

## | Vote score (attribute **vote**)
<font size="5">$score = \frac{\sum(vote)}{\max \sum(vote)}$ <br> </font>

## | NOTE:
+ First 3 scores are calculated at the last block of the interval
+ Vote score is calculated in entire interval (sum)

# | 3. Back test 
> Model ---output---> score_1, score_2, ... , score_n

=> distribute money to $validator_i$ bases on proportion: <font size="5">$p_i = \frac{score_i}{\sum(score)}$</font>

+ Assumption: all users (delegators) stake their money bases on Orchai's suggestion (model's output)

+ time interval: from Tb to Te     (begin to end)

+ we have:
  + D = delegators_token 
  + U = delegators_reward
  + real APR of validator i 

           
<font size="5">$$APR = \frac{\sum_{Tb}^{Te}(U)}{D_{Te} - D_{Tb}}$$</font>
> denominator may be equal to zero

+ Assume that money-redistributing does not affect APR

+ We will redistribute <font size="4">$M = \sum(D_{Te} - D_{Tb})$</font> tokens in the network <br>
=> new reward when staking to validator i 

<font size="5">$$Img\_reward_i = M \times p_i \times APR_i$$</font>

=> Compare real total reward to total reward bases on Orchai's suggestion

<font size="5">$$Real\_total\_reward <> \sum(Img\_reward_i)$$</font>


# | 4. Functional
+ ETL:
  + validator_block:
    + input:
      + data: from table validator_block
      + weights: weight for 4 scores
      + 