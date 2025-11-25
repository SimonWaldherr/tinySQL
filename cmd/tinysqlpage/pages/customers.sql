-- Customers listing page.

SELECT
    'hero' AS component,
    'Customers' AS title,
    'Simple directory driven by SQL' AS subtitle
FROM ui_context;

SELECT
    'table' AS component,
    'Customer roster' AS title,
    id,
    name,
    segment,
    city,
    active
FROM customers
ORDER BY name;
