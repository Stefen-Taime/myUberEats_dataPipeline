-- Création de la vue TOTAL_ORDERS_PER_YEAR
create or replace view UBER_EAT.PUBLIC.TOTAL_ORDERS_PER_YEAR (
    "Annee",
    "TotalOrders"
) as
SELECT 
    d."Annee",
    COUNT(c."ID_Commande") AS "TotalOrders"
FROM "COMMANDES_FACT" c
JOIN "DATE_DIM" d ON c."ID_DateHeure" = d."DateHeure"
WHERE d."Annee" IN (2021, 2022, 2023)
GROUP BY d."Annee"
ORDER BY d."Annee";

-- Création de la vue TOP_3_RESTAURANTS_BY_ORDERS_EXCLUDING_UNKNOWN
create or replace view UBER_EAT.PUBLIC.TOP_3_RESTAURANTS_BY_ORDERS_EXCLUDING_UNKNOWN (
    "NomRestaurant",
    "TotalNumberOfOrders",
    "OrderRank"
) as
SELECT 
    "NomRestaurant",
    "TotalNumberOfOrders",
    "OrderRank"
FROM (
    SELECT 
        r."NomRestaurant",
        COUNT(c."ID_Commande") AS "TotalNumberOfOrders",
        RANK() OVER (ORDER BY COUNT(c."ID_Commande") DESC) AS "OrderRank"
    FROM "COMMANDES_FACT" c
    JOIN "RESTAURANT_DIM" r ON c."ID_Restaurant" = r."ID_Restaurant"
    WHERE r."NomRestaurant" <> 'Nom inconnu'
    GROUP BY r."NomRestaurant"
) AS ranked_restaurants
WHERE "OrderRank" <= 3
ORDER BY "OrderRank";

-- Création de la vue RESTAURANT_PAYMENT_PREFERENCES_EXCLUDING_UNKNOWN
create or replace view UBER_EAT.PUBLIC.RESTAURANT_PAYMENT_PREFERENCES(
	"Annee",
	"NomRestaurant",
	"TypePaiement",
	"NumberOfOrders",
	"TotalOrderAmount"
) as
SELECT 
    d."Annee",
    r."NomRestaurant",
    p."TypePaiement",
    COUNT(c."ID_Commande") AS "NumberOfOrders",
    SUM(c."MontantTotal") AS "TotalOrderAmount"
FROM "COMMANDES_FACT" c
JOIN "RESTAURANT_DIM" r ON c."ID_Restaurant" = r."ID_Restaurant"
JOIN "PAIEMENT_DIM" p ON c."ID_Paiement" = p."ID_Paiement"
JOIN "DATE_DIM" d ON c."ID_DateHeure" = d."DateHeure"
WHERE r."NomRestaurant" <> 'Nom inconnu'
GROUP BY d."Annee", r."NomRestaurant", p."TypePaiement"
ORDER BY d."Annee", r."NomRestaurant", "NumberOfOrders" DESC, "TotalOrderAmount" DESC;


-- Création de la vue ANNUAL_SPEND_RANKING_EXCLUDING_UNKNOWN
create or replace view UBER_EAT.PUBLIC.ANNUAL_SPEND_RANKING_EXCLUDING_UNKNOWN(
	"Annee",
	"NomRestaurant",
	"TotalSpend",
	"AnnualRank"
) as
SELECT 
    d."Annee",
    r."NomRestaurant",
    SUM(c."MontantTotal") AS "TotalSpend",
    RANK() OVER (PARTITION BY d."Annee" ORDER BY SUM(c."MontantTotal") DESC) AS "AnnualRank"
FROM "COMMANDES_FACT" c
JOIN "RESTAURANT_DIM" r ON c."ID_Restaurant" = r."ID_Restaurant"
JOIN "DATE_DIM" d ON c."ID_DateHeure" = d."DateHeure"
WHERE r."NomRestaurant" <> 'Nom inconnu'
GROUP BY d."Annee", r."NomRestaurant"
ORDER BY d."Annee", "TotalSpend" DESC;

-- Création de la vue TOP_3_RESTAURANTS_BY_ORDERS_EXCLUDING_UNKNOWN
create or replace view UBER_EAT.PUBLIC.TOP_3_RESTAURANTS_BY_ORDERS(
	"NomRestaurant",
	"TotalNumberOfOrders",
	"OrderRank"
) as
SELECT 
    "NomRestaurant",
    "TotalNumberOfOrders",
    "OrderRank"
FROM (
    SELECT 
        r."NomRestaurant",
        COUNT(c."ID_Commande") AS "TotalNumberOfOrders",
        RANK() OVER (ORDER BY COUNT(c."ID_Commande") DESC) AS "OrderRank"
    FROM "COMMANDES_FACT" c
    JOIN "RESTAURANT_DIM" r ON c."ID_Restaurant" = r."ID_Restaurant"
    GROUP BY r."NomRestaurant"
) AS ranked_restaurants
WHERE "OrderRank" <= 3
ORDER BY "OrderRank";

