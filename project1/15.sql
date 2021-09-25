SELECT T.name
FROM Trainer AS T, CatchedPokemon AS CP, Pokemon AS P
WHERE T.hometown = 'Sangnok City' AND T.id = CP.owner_id AND CP.pid = P.id AND P.name LIKE 'P%'
ORDER BY T.name
;