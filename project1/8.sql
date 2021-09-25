SELECT count(*)
FROM CatchedPokemon AS CP, Pokemon AS P
WHERE CP.pid = P.id
GROUP BY P.type
ORDER BY P.type
;