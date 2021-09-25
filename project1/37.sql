SELECT SUM(CP.level)
FROM CatchedPokemon AS CP, Pokemon AS P
WHERE CP.pid = P.id AND P.type LIKE 'Fire'
;