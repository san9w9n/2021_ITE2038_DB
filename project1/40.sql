SELECT T.name
FROM Trainer AS T, CatchedPokemon AS CP, Pokemon AS P
WHERE T.id = CP.owner_id AND CP.pid = P.id AND P.name = 'Pikachu'
ORDER BY T.name
;