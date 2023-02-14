# 1. Data
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

# 2. Score
## 2.1. Voting power score (attribute **tokens**)
<font size="4">

$score = 1 - \frac{tokens}{max\_tokens}$ 

> if $\frac{tokens}{total\_tokens} > \frac{2}{num\_vals}$ : score = 0
</font>

## 2.2. Commission score (attribute **commission_rate**)
<font size="4">

$score = 1 - \frac{commission\_rate}{accept\_rate}$ </font>

## 2.3. Self bonded score (attribute **self_bonded**)
<font size="4">

$score = self\_bonded - \frac{total\_self\_bonded}{num\_vals \times concentration\_level}$ 

> if $score < 0$ : $score = 0$ 

$score = \frac{score}{max\_score}$

> if $max\_score == 0$ : $score = 0$
</font>

## 2.4. Vote score (attribute **vote**)
<font size="4">$score = \frac{\sum{vote}}{\max \sum{vote}}$ <br> </font>

## **NOTE:**
+ First 3 scores are calculated at the last block of the interval
+ Vote score is calculated in entire interval (sum)

# 3. Back test 
> Model output: score_1, score_2, ... , score_n

+ distribute money to validator $i$ bases on proportion: <font size="4">$p_i = \frac{score_i}{\sum{score}}$</font>

+ Assumption: all users (delegators) stake their money bases on Orchai's suggestion (model's output)

+ time interval: from Tb to Te     (begin to end)

+ we have:
  + D = delegators_token 

## 3.1. Back test reward

> Calculate user delegated from Tb to Te of validator $i$
> <font size="5">
> + $\Delta^i = \frac{\sum{\max(D_j - D_0, 0)}}{num - 1}$
> </font>
> 
> where: $j$ denotes timestamp $j^{th}$ from Tb to Te, started from 0 and
> $num$ indicates the number of timestamps from Tb to Te

> **Total user delegated:**
> <font size="4">
> + $total\_delegated = \sum{\Delta^i}$
> </font>

> **APR:**
> + APR is only affected by the commission_rate
> + Assume that when $commission\_rate = CR_{base}$ the APR will be $APR_{base}$
>
> <font size="4">
> 
> $\Rightarrow$ when $commission\_rate_i = CR_i \rightarrow APR_i = \frac{APR_{base}}{1 - CR_{base}} \times (1 - CR_i)$ 
> </font>
> + <font size="4"> $C^+ = \frac{APR_{base}}{1 - CR_{base}}$ </font> is a constant

> **Reward:**
> <font size="4">
> $$\begin{aligned}
total\_real\_reward & = \sum{real\_reward_i} \\ 
                    & = \sum{(APR_i \times \Delta^i)} \\
                    & = C^+ \times \sum{(1 - CR_i) \Delta^i}
\end{aligned}$$
> </font>

> Assume that money-redistributing does not affect APR
> <font size="4"> 
> $$\begin{aligned}
fake\_delegated_i   & = p_i \times total\_delegated \\
fake\_reward_i      & = APR_i \times fake\_delegated_i \\
                    & = C^+ \times (1 - CR_i)p_i \times \sum{\Delta^i} \\
total\_fake\_reward & = C^+ \times \sum{\Delta^i} \times \sum{(1 - CR_i)p_i}
\end{aligned}$$
> </font>

> **Compare $total\_fake\_reward$ and $total\_real\_reward$**
> 
> $\Rightarrow$ Compare <font size="4">$\sum{(1 - CR_i) \Delta^i}$</font> and <font size="4">$\sum{\Delta^i} \times \sum{(1 - CR_i)p_i}$</font>



