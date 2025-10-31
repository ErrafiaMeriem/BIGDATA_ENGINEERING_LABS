SELECT * FROM clients;


SELECT * FROM hotels_partitioned
WHERE ville = 'Paris';


SELECT R.*, H.nom AS hotel_nom, H.ville, C.nom AS client_nom
FROM reservations_bucketed R
JOIN clients C ON R.client_id = C.client_id
JOIN hotels_partitioned H ON R.hotel_id = H.hotel_id;


SELECT C.client_id, C.nom, COUNT(R.reservation_id) AS nb_reservations
FROM clients C
JOIN reservations_bucketed R ON C.client_id = R.client_id
GROUP BY C.client_id, C.nom;


SELECT C.client_id, C.nom, DATEDIFF(R.date_fin, R.date_debut) AS nb_nuits
FROM clients C
JOIN reservations_bucketed R ON C.client_id = R.client_id
WHERE DATEDIFF(R.date_fin, R.date_debut) > 2;


SELECT C.nom AS client_nom, H.nom AS hotel_nom, H.ville
FROM clients C
JOIN reservations_bucketed R ON C.client_id = R.client_id
JOIN hotels_partitioned H ON R.hotel_id = H.hotel_id;


SELECT H.nom, COUNT(R.reservation_id) AS nb_reservations
FROM hotels_partitioned H
JOIN reservations_bucketed R ON H.hotel_id = R.hotel_id
GROUP BY H.nom
HAVING COUNT(R.reservation_id) > 1;


SELECT H.nom
FROM hotels_partitioned H
LEFT JOIN reservations_bucketed R ON H.hotel_id = R.hotel_id
WHERE R.reservation_id IS NULL;


SELECT DISTINCT C.nom
FROM clients C
WHERE C.client_id IN (
    SELECT R.client_id
    FROM reservations_bucketed R
    JOIN hotels_partitioned H ON R.hotel_id = H.hotel_id
    WHERE H.etoiles > 4
);


SELECT H.nom AS hotel_nom, SUM(R.prix_total) AS total_revenus
FROM hotels_partitioned H
JOIN reservations_bucketed R ON H.hotel_id = R.hotel_id
GROUP BY H.nom;


SELECT H.ville, SUM(R.prix_total) AS total_revenus
FROM reservations_bucketed R
JOIN hotels_partitioned H ON R.hotel_id = H.hotel_id
GROUP BY H.ville;


SELECT C.client_id, C.nom, COUNT(R.reservation_id) AS nb_reservations
FROM clients C
JOIN reservations_bucketed R ON C.client_id = R.client_id
GROUP BY C.client_id, C.nom;

