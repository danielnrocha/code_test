-- question 1
SELECT
    be.name as lower_earning_name,
    be1.name as higher_earning_name
FROM 
    blue_employee as be
JOIN (
    SELECT
        *
    FROM
        blue_employee        
) be1 ON be.id = be1.id and be1.name <> be.name and be1.salary > be.salary
ORDER BY
    be.id asc,
    be1.salary asc;

-- question 2
SELECT
    bd.name,
    COUNT(bed.id) as count_employees
FROM
    blue_department as bd
LEFT JOIN
    blue_employee_dept as bed
    ON bd.id = bed.dept
GROUP BY 1
ORDER BY 
    2 desc, 
    1 asc;
