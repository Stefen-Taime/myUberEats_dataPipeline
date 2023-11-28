-- Vue sur les Tendances des Commandes au Fil du Temps
create or replace view UBER_EAT.PUBLIC.ORDER_TRENDS AS
SELECT 
    d."Annee",
    d."Mois",
    COUNT(c."ID_Commande") AS "TotalOrders"
FROM "COMMANDES_FACT" c
JOIN "DATE_DIM" d ON c."ID_DateHeure" = d."DateHeure"
JOIN "RESTAURANT_DIM" r ON c."ID_Restaurant" = r."ID_Restaurant"
WHERE r."NomRestaurant" <> 'Nom inconnu'
GROUP BY d."Annee", d."Mois"
ORDER BY d."Annee", d."Mois";

-- Vue sur le Total des Dépenses par Restaurant et par Type de Paiement
create or replace view UBER_EAT.PUBLIC.RESTAURANT_SPENDING_BY_PAYMENT_TYPE AS
SELECT 
    r."NomRestaurant",
    p."TypePaiement",
    SUM(c."MontantTotal") AS "TotalSpending"
FROM "COMMANDES_FACT" c
JOIN "RESTAURANT_DIM" r ON c."ID_Restaurant" = r."ID_Restaurant"
JOIN "PAIEMENT_DIM" p ON c."ID_Paiement" = p."ID_Paiement"
WHERE r."NomRestaurant" <> 'Nom inconnu'
GROUP BY r."NomRestaurant", p."TypePaiement"
ORDER BY "TotalSpending" DESC;

-- Vue sur la Distribution des Commandes par Heure de la Journée
create or replace view UBER_EAT.PUBLIC.ORDER_DISTRIBUTION_BY_HOUR AS
SELECT 
    d."Heure",
    COUNT(c."ID_Commande") AS "TotalOrders"
FROM "COMMANDES_FACT" c
JOIN "DATE_DIM" d ON c."ID_DateHeure" = d."DateHeure"
JOIN "RESTAURANT_DIM" r ON c."ID_Restaurant" = r."ID_Restaurant"
WHERE r."NomRestaurant" <> 'Nom inconnu'
GROUP BY d."Heure"
ORDER BY d."Heure";

-- Vue sur le Classement Annuel des Dépenses par Restaurant
create or replace view UBER_EAT.PUBLIC.ANNUAL_RESTAURANT_SPENDING_RANKING AS
SELECT 
    d."Annee",
    r."NomRestaurant",
    SUM(c."MontantTotal") AS "TotalSpending",
    RANK() OVER (PARTITION BY d."Annee" ORDER BY SUM(c."MontantTotal") DESC) AS "AnnualRank"
FROM "COMMANDES_FACT" c
JOIN "RESTAURANT_DIM" r ON c."ID_Restaurant" = r."ID_Restaurant"
JOIN "DATE_DIM" d ON c."ID_DateHeure" = d."DateHeure"
WHERE r."NomRestaurant" <> 'Nom inconnu'
GROUP BY d."Annee", r."NomRestaurant"
ORDER BY d."Annee", "TotalSpending" DESC;
