using BenchmarkDotNet.Attributes;
using SequelLight.Parsing;

namespace SequelLight.Benchmarks;

[MemoryDiagnoser]
public class SqlParserBenchmarks
{
    private const string SimpleSelect = "SELECT id, name, age FROM users WHERE id = 1";

    private const string MultiTableJoin =
        """
        SELECT o.id, o.total, u.name, u.email, p.title, p.price
        FROM orders o
        INNER JOIN users u ON o.user_id = u.id
        LEFT JOIN order_items oi ON oi.order_id = o.id
        INNER JOIN products p ON p.id = oi.product_id
        WHERE o.status = 'active' AND u.verified = 1
        ORDER BY o.created_at DESC
        LIMIT 50
        """;

    private const string ComplexWhere =
        """
        SELECT *
        FROM events
        WHERE (status = 'active' OR status = 'pending')
          AND created_at BETWEEN '2024-01-01' AND '2024-12-31'
          AND category_id IN (1, 2, 3, 4, 5)
          AND title LIKE '%conference%'
          AND NOT is_deleted
          AND priority >= 3
          AND (assignee_id IS NOT NULL OR team_id = 10)
        """;

    private const string NestedSubqueries =
        """
        SELECT u.id, u.name,
               (SELECT COUNT(*) FROM orders WHERE user_id = u.id) AS order_count,
               (SELECT SUM(total) FROM orders WHERE user_id = u.id) AS total_spent
        FROM users u
        WHERE u.id IN (
            SELECT DISTINCT user_id
            FROM orders
            WHERE total > (SELECT AVG(total) FROM orders)
              AND status IN (SELECT code FROM statuses WHERE active = 1)
        )
        AND EXISTS (
            SELECT 1 FROM user_roles ur
            INNER JOIN roles r ON r.id = ur.role_id
            WHERE ur.user_id = u.id AND r.name = 'premium'
        )
        ORDER BY total_spent DESC
        LIMIT 100
        """;

    private const string WindowFunctions =
        """
        SELECT
            department,
            employee_name,
            salary,
            ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank,
            SUM(salary) OVER (PARTITION BY department) AS dept_total,
            AVG(salary) OVER () AS company_avg,
            salary - AVG(salary) OVER (PARTITION BY department) AS diff_from_dept_avg
        FROM employees
        WHERE active = 1
        ORDER BY department, rank
        """;

    private const string CteQuery =
        """
        WITH RECURSIVE
            category_tree(id, name, parent_id, depth) AS (
                SELECT id, name, parent_id, 0 FROM categories WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, c.parent_id, ct.depth + 1
                FROM categories c
                INNER JOIN category_tree ct ON c.parent_id = ct.id
                WHERE ct.depth < 10
            ),
            leaf_categories AS (
                SELECT ct.id, ct.name, ct.depth
                FROM category_tree ct
                WHERE NOT EXISTS (SELECT 1 FROM categories c WHERE c.parent_id = ct.id)
            )
        SELECT lc.name, COUNT(p.id) AS product_count, AVG(p.price) AS avg_price
        FROM leaf_categories lc
        LEFT JOIN products p ON p.category_id = lc.id
        GROUP BY lc.name
        HAVING product_count > 0
        ORDER BY product_count DESC
        """;

    private const string InsertWithUpsert =
        """
        INSERT INTO inventory (sku, warehouse_id, quantity, updated_at)
        VALUES ('ABC-123', 1, 50, '2024-06-01'),
               ('DEF-456', 1, 30, '2024-06-01'),
               ('GHI-789', 2, 100, '2024-06-01')
        ON CONFLICT (sku, warehouse_id) DO UPDATE SET
            quantity = quantity + excluded.quantity,
            updated_at = excluded.updated_at
        RETURNING *
        """;

    [Benchmark(Description = "Simple SELECT")]
    public object ParseSimpleSelect() => SqlParser.Parse(SimpleSelect);

    [Benchmark(Description = "Multi-table JOIN")]
    public object ParseMultiTableJoin() => SqlParser.Parse(MultiTableJoin);

    [Benchmark(Description = "Complex WHERE")]
    public object ParseComplexWhere() => SqlParser.Parse(ComplexWhere);

    [Benchmark(Description = "Nested subqueries")]
    public object ParseNestedSubqueries() => SqlParser.Parse(NestedSubqueries);

    [Benchmark(Description = "Window functions")]
    public object ParseWindowFunctions() => SqlParser.Parse(WindowFunctions);

    [Benchmark(Description = "Recursive CTE")]
    public object ParseCteQuery() => SqlParser.Parse(CteQuery);

    [Benchmark(Description = "INSERT with UPSERT")]
    public object ParseInsertWithUpsert() => SqlParser.Parse(InsertWithUpsert);
}
