SELECT CP.nickname
FROM CatchedPokemon AS CP
WHERE CP.level >= 40 AND CP.owner_id >=5
ORDER BY CP.nickname
;