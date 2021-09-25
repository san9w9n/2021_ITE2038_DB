SELECT AVG(CP.level) 
FROM CatchedPokemon AS CP, Pokemon AS P
WHERE CP.pid = P.id AND P.type = 'Water'
;