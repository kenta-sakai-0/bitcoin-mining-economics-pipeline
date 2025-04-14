SELECT 
    `date`,
    ANY_VALUE(block_reward) block_reward

FROM {{ref('stg_block_reward')}}
GROUP BY `date`
ORDER BY `date` 