package cn.wk;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class Elasticsearch_Client {
    RestHighLevelClient client = null;

    @Before
    public void before() {
        //创建客户端对象
        client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    @After
    public void after() throws IOException {
        //关闭客户端连接
        client.close();
    }

    //创建客户端
    @Test
    public void test01() throws IOException {
        //创建索引-请求对象
        CreateIndexRequest request = new CreateIndexRequest("user");
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = response.isAcknowledged();
        //响应状态
        System.out.println("操作状态" + acknowledged);


    }

    //查看索引
    @Test
    public void test02() throws IOException {
        GetIndexRequest request = new GetIndexRequest("user");
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);

        System.out.println("aliases" + response.getAliases());
        System.out.println("aliases" + response.getMappings());
        System.out.println("aliases" + response.getSettings());
    }

    //删除索引
    @Test
    public void test03() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("user");
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);

        System.out.println("操作结果" + response.isAcknowledged());
    }

    //创建文档对象
    @Test
    public void test04() throws IOException {
        //新增文档 - 请求对象
        IndexRequest request = new IndexRequest();
        //设置索引及唯一性标识
        request.index("user").id("1001");
        //创建数据对象
        User user = new User();
        user.setName("张三");
        user.setAge(30);
        user.setSex("男");
        ObjectMapper objectMapper = new ObjectMapper();
        String productJson = objectMapper.writeValueAsString(user);
        //添加文档数据,数据格式为JSON格式
        request.source(productJson, XContentType.JSON);
        //客户端发送请求,获取响应对象
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        //打印结果信息
        System.out.println("_index:" + response.getIndex());
        System.out.println("_id:" + response.getId());
        System.out.println("_result:" + response.getResult());
    }


    //修改文档
    @Test
    public void test05() throws IOException {
        //修改文档 - 请求对象
        UpdateRequest request = new UpdateRequest();

//        配置修改参数
        request.index("user").id("1001");
        request.doc(XContentType.JSON,"sex","女");
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        //打印结果信息
        System.out.println("_index:" + response.getIndex());
        System.out.println("_id:" + response.getId());
        System.out.println("_result:" + response.getResult());
    }


    //查询文档
    @Test
    public void test06() throws IOException {

        //1.创建请求对象
        GetRequest request = new GetRequest().index("user").id("1001");
        //2.客户端发送请求,获取响应对象
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //打印结果信息
        System.out.println("_index:" + response.getIndex());
        System.out.println("_id:" + response.getId());
        System.out.println("_type:" + response.getType());
        System.out.println("source:" +response.getSourceAsString());
    }

    //删除文档
    @Test
    public void test07() throws IOException {

        //1.创建请求对象
        //2.客户端发送请求,获取响应对象
        DeleteRequest request = new DeleteRequest().index("user").id("1001");
        DeleteResponse response = client.delete(request,RequestOptions.DEFAULT);
        //打印结果信息
        System.out.println(response.toString());
    }


    //批量添加
    @Test
    public void test08() throws IOException {

        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("user").id("1001").source(XContentType.JSON,"name","zhangsan"));
        request.add(new IndexRequest("user").id("1001").source(XContentType.JSON,"name","lisi"));
        request.add(new IndexRequest("user").id("1001").source(XContentType.JSON,"name","wangwu"));
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);

        System.out.println("took:" +responses.getTook());
        System.out.println("items:" +responses.getItems());
    }


    //批量删除
    @Test
    public void test09() throws IOException {

        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest("user").id("1001"));
        request.add(new DeleteRequest("user").id("1002"));
        request.add(new DeleteRequest("user").id("1003"));
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);

        System.out.println("took:" +responses.getTook());
        System.out.println("items:" +responses.getItems());
    }


    //高级查询
    //请求体查询
    @Test
    public void test10() throws IOException {
        //创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("student");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有数据
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //查询匹配
        SearchHits hits = response.getHits();
        System.out.println("took:" +response.getTook());
        System.out.println("timeout:" +response.isTimedOut());
        System.out.println("total:" +hits.getTotalHits());
        System.out.println("MaxScore:" +hits.getMaxScore());
        System.out.println("hits===============>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<===================");
    }



    //高级查询
    //term查询,查询条件为关键字
    @Test
    public void test11() throws IOException {
        //创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("student");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有数据
        sourceBuilder.query(QueryBuilders.termQuery("age","30"));
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //查询匹配
        SearchHits hits = response.getHits();
        System.out.println("took:" +response.getTook());
        System.out.println("timeout:" +response.isTimedOut());
        System.out.println("total:" +hits.getTotalHits());
        System.out.println("MaxScore:" +hits.getMaxScore());
        System.out.println("hits===============>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<===================");
    }


    //高级查询
    //分页查询
    @Test
    public void test12() throws IOException {
        //创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("student");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有数据
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        //分页查询
        //当前页起始索引(第一条数据的顺序号),from
        sourceBuilder.from(2);
        //每页显示多少条
        sourceBuilder.size(2);
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //查询匹配
        SearchHits hits = response.getHits();
        System.out.println("took:" +response.getTook());
        System.out.println("timeout:" +response.isTimedOut());
        System.out.println("total:" +hits.getTotalHits());
        System.out.println("MaxScore:" +hits.getMaxScore());
        System.out.println("hits===============>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<===================");
    }



    //高级查询
    //数据排序
    @Test
    public void test13() throws IOException {
        //创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("student");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有数据
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        //排序
        sourceBuilder.sort("age", SortOrder.DESC);
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //查询匹配
        SearchHits hits = response.getHits();
        System.out.println("took:" +response.getTook());
        System.out.println("timeout:" +response.isTimedOut());
        System.out.println("total:" +hits.getTotalHits());
        System.out.println("MaxScore:" +hits.getMaxScore());
        System.out.println("hits===============>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<===================");
    }


    //高级查询
    //过滤字段
    @Test
    public void test14() throws IOException {
        //创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("student");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询所有数据
        sourceBuilder.query(QueryBuilders.matchAllQuery());

        //查询过滤字段
        String[] includes = {};

        String[] excludes = {"name","age"};
        sourceBuilder.fetchSource(includes,excludes);


        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //查询匹配
        SearchHits hits = response.getHits();
        System.out.println("took:" +response.getTook());
        System.out.println("timeout:" +response.isTimedOut());
        System.out.println("total:" +hits.getTotalHits());
        System.out.println("MaxScore:" +hits.getMaxScore());
        System.out.println("hits===============>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<===================");
    }

    //高级查询
    //bool查询
    @Test
    public void test15() throws IOException {
        //创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("student");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //必须包含
        boolQueryBuilder.must(QueryBuilders.matchQuery("age","30"));
        //一定不包含
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("name","zhangsan"));
        //可能包含
        boolQueryBuilder.should(QueryBuilders.matchQuery("sex","男"));

        sourceBuilder.query(boolQueryBuilder);
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //查询匹配
        SearchHits hits = response.getHits();
        System.out.println("took:" +response.getTook());
        System.out.println("timeout:" +response.isTimedOut());
        System.out.println("total:" +hits.getTotalHits());
        System.out.println("MaxScore:" +hits.getMaxScore());
        System.out.println("hits===============>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<===================");
    }


    //高级查询
    //范围查询
    @Test
    public void test16() throws IOException {
        //创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("student");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
        rangeQuery.gte("30");
        rangeQuery.lte("40");

        sourceBuilder.query(rangeQuery);
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //查询匹配
        SearchHits hits = response.getHits();
        System.out.println("took:" +response.getTook());
        System.out.println("timeout:" +response.isTimedOut());
        System.out.println("total:" +hits.getTotalHits());
        System.out.println("MaxScore:" +hits.getMaxScore());
        System.out.println("hits===============>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<===================");
    }


    //高级查询
    //模糊查询
    @Test
    public void test17() throws IOException {
        //创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("student");
        //构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query( QueryBuilders.fuzzyQuery("name","zhangsan").fuzziness(Fuzziness.ONE));
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //查询匹配
        SearchHits hits = response.getHits();
        System.out.println("took:" +response.getTook());
        System.out.println("timeout:" +response.isTimedOut());
        System.out.println("total:" +hits.getTotalHits());
        System.out.println("MaxScore:" +hits.getMaxScore());
        System.out.println("hits===============>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<===================");
    }


    //高级查询
    //高亮查询
    @Test
    public void test18() throws IOException {
        //创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("student");
        //构建查询的请求体构建体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query( QueryBuilders.fuzzyQuery("name","zhangsan").fuzziness(Fuzziness.ONE));

        //构建高亮字段
        HighlightBuilder highlighterBuilder = new HighlightBuilder();
        highlighterBuilder.preTags("<font color='red'>");//设置标签前缀
        highlighterBuilder.postTags("</font>");//设置标签后缀
        highlighterBuilder.field("name");//设置高亮字段sourceBuilder.highlighter(highlighterBuilder);
        sourceBuilder.highlighter(highlighterBuilder);

        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);


        //查询匹配
        SearchHits hits = response.getHits();
        System.out.println("took:" +response.getTook());
        System.out.println("timeout:" +response.isTimedOut());
        System.out.println("total:" +hits.getTotalHits());
        System.out.println("MaxScore:" +hits.getMaxScore());
        System.out.println("hits===============>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
        }
        System.out.println("<===================");
    }


    //高级查询
    //聚合查询
    @Test
    public void test19() throws IOException {
        //创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("student");
        //构建查询的请求体构建体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.aggregation(AggregationBuilders.max("maxAge").field("age"));
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);


        //查询匹配
        SearchHits hits = response.getHits();
        System.out.println(response);
    }


    //高级查询
    //分组查询
    @Test
    public void test20() throws IOException {
        //创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("student");
        //构建查询的请求体构建体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.aggregation(AggregationBuilders.terms("age_groupby").field("age"));
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //查询匹配
        SearchHits hits = response.getHits();
        System.out.println(response);
    }


}
