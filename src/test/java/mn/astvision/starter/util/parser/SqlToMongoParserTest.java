package mn.astvision.starter.util.parser;

import mn.astvision.commontools.sqlparser.MongoQuery;
import mn.astvision.commontools.sqlparser.SqlToMongoParser;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SqlToMongoParserTest {

    @Test
    void shouldParse() {
        String sql = "SELECT name, age FROM users WHERE age > 25 AND city = 'NY' ORDER BY age DESC LIMIT 10 OFFSET 5";

        MongoQuery query = SqlToMongoParser.parse(sql);

        System.out.println("Collection: " + query.getCollection());
        System.out.println("Filter: " + query.getFilter().toJson());
        System.out.println("Projection: " + query.getProjection().toJson());
        System.out.println("Sort: " + query.getSort().toJson());
        System.out.println("Limit: " + query.getLimit());
        System.out.println("Skip: " + query.getSkip());

        MongoQuery parse = SqlToMongoParser.parse(sql);
        System.out.println(parse.toString());
    }

    @Test
    void shouldParseLike() {
        String sql = "SELECT * FROM employees WHERE department LIKE 'Eng%' OR salary NOT IN (1000,2000,3000) LIMIT 5";
        String mongoShell = SqlToMongoParser.toMongoShell(sql);

        System.out.println(mongoShell);
    }

    @Test
    void testSelectAll() {
        String sql = "SELECT * FROM users";
        MongoQuery query = SqlToMongoParser.parse(sql);

        assertThat(query.getType()).isEqualTo(MongoQuery.Type.FIND);
        assertThat(query.getFilter()).isEmpty();
        assertThat(query.getProjection()).isEmpty();
    }

    @Test
    void testSelectFields() {
        String sql = "SELECT name, age FROM users";
        MongoQuery query = SqlToMongoParser.parse(sql);

        assertThat(query.getProjection()).containsKeys("name", "age");
        assertThat(query.getProjection().get("name")).isEqualTo(1);
    }

    @Test
    void testWhereConditions() {
        String sql = "SELECT * FROM users WHERE age >= 18 AND active = true";
        MongoQuery query = SqlToMongoParser.parse(sql);

        Document filter = query.getFilter();
        assertThat(filter.containsKey("$and")).isTrue();
        List<Document> andList = (List<Document>) filter.get("$and");
        assertThat(andList).anySatisfy(doc -> assertThat(doc).containsEntry("age", new Document("$gte", 18)));
        assertThat(andList).anySatisfy(doc -> assertThat(doc).containsEntry("active", true));
    }

    @Test
    void testInAndNotIn() {
        String sql = "SELECT * FROM users WHERE city IN ('NY','LA') AND role NOT IN ('guest')";
        MongoQuery query = SqlToMongoParser.parse(sql);

        Document filter = query.getFilter();
        List<Document> andList = (List<Document>) filter.get("$and");
        assertThat(andList).anySatisfy(doc -> assertThat(doc.get("city")).isEqualTo(new Document("$in", List.of("NY", "LA"))));
        assertThat(andList).anySatisfy(doc -> assertThat(doc.get("role")).isEqualTo(new Document("$nin", List.of("guest"))));
    }

    @Test
    void testLike() {
        String sql = "SELECT * FROM users WHERE name LIKE 'Jo%n'";
        MongoQuery query = SqlToMongoParser.parse(sql);

        Document filter = query.getFilter();
        assertThat(filter.get("name")).isInstanceOf(Document.class);
        Document regexDoc = (Document) filter.get("name");
        assertThat(regexDoc.getString("$regex")).isEqualTo("^Jo.*n$");
        assertThat(regexDoc.getString("$options")).isEqualTo("i");
    }

    @Test
    void testBetween() {
        String sql = "SELECT * FROM orders WHERE price BETWEEN 100 AND 500";
        MongoQuery query = SqlToMongoParser.parse(sql);

        Document filter = query.getFilter();
        Document priceDoc = (Document) filter.get("price");
        assertThat(priceDoc.get("$gte")).isEqualTo(100);
        assertThat(priceDoc.get("$lte")).isEqualTo(500);
    }


    @Test
    void testIsNullAndIsNotNull() {
        String sql = "SELECT * FROM users WHERE email IS NULL OR phone IS NOT NULL";
        MongoQuery query = SqlToMongoParser.parse(sql);

        Document filter = query.getFilter();
        assertThat(filter.containsKey("$or")).isTrue();
        List<Document> orList = (List<Document>) filter.get("$or");
        assertThat(orList).anySatisfy(doc -> assertThat(doc.get("email")).isNull());
        assertThat(orList).anySatisfy(doc -> assertThat(doc.get("phone")).isEqualTo(new Document("$ne", null)));
    }

    @Test
    void testOrderLimitOffset() {
        String sql = "SELECT * FROM users ORDER BY age DESC, name ASC LIMIT 10 OFFSET 5";
        MongoQuery query = SqlToMongoParser.parse(sql);

        Document sort = query.getSort();
        assertThat(sort).containsEntry("age", -1);
        assertThat(sort).containsEntry("name", 1);
        assertThat(query.getLimit()).isEqualTo(10);
        assertThat(query.getSkip()).isEqualTo(5);
    }

    @Test
    void testCount() {
        String sql = "SELECT COUNT(*) FROM users WHERE active = true";
        MongoQuery query = SqlToMongoParser.parse(sql);

        assertThat(query.getType()).isEqualTo(MongoQuery.Type.AGGREGATE);
        List<Document> pipeline = query.getPipeline();
        assertThat(pipeline).anySatisfy(stage -> assertThat(stage.containsKey("$match")).isTrue());
        assertThat(pipeline.get(pipeline.size() - 1)).containsKey("$count");
    }

    @Test
    void testDistinct() {
        String sql = "SELECT DISTINCT city FROM users";
        MongoQuery query = SqlToMongoParser.parse(sql);

        assertThat(query.getType()).isEqualTo(MongoQuery.Type.AGGREGATE);
        List<Document> pipeline = query.getPipeline();
        assertThat(pipeline.get(pipeline.size() - 1).containsKey("$project")).isTrue();
    }

    @Test
    void testGroupBy() {
        String sql = "SELECT * FROM orders GROUP BY customerId, status";
        MongoQuery query = SqlToMongoParser.parse(sql);

        assertThat(query.getType()).isEqualTo(MongoQuery.Type.AGGREGATE);
        List<Document> pipeline = query.getPipeline();
        Document groupStage = pipeline.get(pipeline.size() - 1);
        Document id = (Document) groupStage.get("$group");
        assertThat(id.containsKey("_id")).isTrue();
        Document groupId = (Document) id.get("_id");
        assertThat(groupId).containsKeys("customerId", "status");
    }
}