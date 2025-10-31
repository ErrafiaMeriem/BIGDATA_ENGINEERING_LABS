LOAD DATA LOCAL INPATH '/shared_volume/DATSETS_HIVE/clients.txt' INTO TABLE clients;

LOAD DATA LOCAL INPATH '/shared_volume/DATSETS_HIVE/hotels.txt' INTO TABLE hotels;

LOAD DATA LOCAL INPATH '/shared_volume/DATSETS_HIVE/reservations.txt' INTO TABLE reservations_tmp;

INSERT INTO TABLE reservations
PARTITION (date_debut)
SELECT
    reservation_id,
    client_id,
    hotel_id,
    date_fin,
    prix_total,
    date_debut
FROM reservations_tmp;


INSERT INTO TABLE hotels_partitioned PARTITION (ville)
SELECT hotel_id, nom, etoiles, ville FROM hotels;

INSERT INTO TABLE reservations_bucketed
SELECT reservation_id, client_id, hotel_id, date_debut, date_fin, prix_total
FROM reservations;