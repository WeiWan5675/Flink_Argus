//package org.weiwan.argus.reader.oplog;
//
//import com.mongodb.*;
//import com.mongodb.client.FindIterable;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoCursor;
//import com.mongodb.client.MongoDatabase;
//import org.apache.flink.configuration.Configuration;
//import org.bson.BsonTimestamp;
//import org.bson.Document;
//import org.weiwan.argus.common.utils.DateUtils;
//import org.weiwan.argus.common.utils.StringUtil;
//import org.weiwan.argus.core.pub.config.ArgusContext;
//import org.weiwan.argus.core.pub.enums.ColumnType;
//import org.weiwan.argus.core.pub.input.BaseRichInputFormat;
//
//import org.weiwan.argus.core.pub.pojo.DataField;
//import org.weiwan.argus.core.pub.pojo.DataRecord;
//import org.weiwan.argus.core.pub.pojo.DataRow;
//import org.weiwan.argus.core.pub.pojo.JobFormatState;
//import org.weiwan.argus.reader.oplog.input.KV;
//import org.weiwan.argus.reader.oplog.input.OplogInputSpliter;
//
//import java.io.IOException;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.*;
//import java.util.concurrent.TimeUnit;
//
///**
// * @Author: lsl
// * @Date: 2020/8/21 11:15
// * @Description:
// **/
//public class OplogInputFormat extends BaseRichInputFormat<DataRecord<List<KV>>, OplogInputSpliter> {
//
//    private static final String KEY_READER_TYPE = "reader.type";
//    private static final String KEY_READER_NAME = "reader.name";
//    private static final String KEY_READER_CLASS = "reader.class";
//    private static final String KEY_READER_mongodb_HOST = "reader.mongodb.HOST";
//    private static final String KEY_READER_mongodb_PORT = "reader.mongodb.PORT";
//    private static final String KEY_READER_mongodb_URL = "reader.mongodb.url";
//    private static final String KEY_READER_mongodb_USERNAME = "reader.mongodb.username";
//    private static final String KEY_READER_mongodb_PASSWORD = "reader.mongodb.password";
//    private static final String KEY_READER_DATABASE = "reader.database";
//    private static final String KEY_READER_TABLENAME = "reader.tableName";
//    private static final String KEY_READER_ANALYSIS_TABLENAME = "reader.analysisTableName";
//    private static final String KEY_READER_PULLTIME = "reader.pullTime";
//    private static final String KEY_READER_BATCHSIZE = "reader.batchSize";
//    private static final String KEY_READER_QUERYTIMEOUT = "reader.queryTimeout";
//
//    protected String mongodbHost;
//    protected Integer mongodbPort;
//    protected String mongodbUrl;
//    protected String mongodbUsername;
//    protected String mongodbPassword;
//    protected String database;
//    protected String tablename;
//    protected String[] analysisTablename;
//    protected String pullTime;
//    protected Integer batchsize;
//    protected Integer querytimeout;
//
//
//    private MongoClient mongoClient;
//    private MongoDatabase mdb;
//    private List<ServerAddress> addrs;
//    private MongoCursor<Document> iterator;
//    private MongoCollection<Document> collection;
//    private BsonTimestamp ts;
//    private Document next;
//
//    @Override
//    public void openInput(OplogInputSpliter split) {
//        try {
//            String[] hosts = mongodbHost.split(",");
//            if(hosts.length == 1 && !StringUtil.isEmpty(mongodbUsername) && !StringUtil.isEmpty(mongodbPassword)){
//                mongoClient = new MongoClient(mongodbHost, mongodbPort);
//
//            }else {
//                //连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址
//                //ServerAddress()两个参数分别为 服务器地址 和 端口
//                addrs = new ArrayList<ServerAddress>();
//                for (String host : hosts) {
//                    ServerAddress serverAddress = new ServerAddress("",mongodbPort);
//                    addrs.add(serverAddress);
//                }
//                //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
//                MongoCredential credential = MongoCredential.createScramSha1Credential(mongodbUsername, database, mongodbPassword.toCharArray());
//                List<MongoCredential> credentials = new ArrayList<MongoCredential>();
//                credentials.add(credential);
//                //通过连接认证获取MongoDB连接
//                mongoClient = new MongoClient(addrs,credentials);
//            }
//
//
//            BsonTimestamp endTs = null;
//            if(isRestore()){
//                Document state = (Document)formatState.getState();
//                //获取重启之后最后一条数据的时间戳
//                endTs = (BsonTimestamp) state.get("ts");
//            }
//
//            //连接到数据库
//            mdb = mongoClient.getDatabase(database);
//            System.out.println("Connect to database successfully");
//            collection = mdb.getCollection(tablename);
//            /*//如果是首次订阅，需要使用自然排序查询，获取第最后一次操作的操作时间戳。如果是续订阅直接读取记录的值赋值给queryTs即可
//            FindIterable<Document> tsCursor = collection.find().sort(new BasicDBObject("$natural", -1)).limit(1);
//            Document first = tsCursor.first();
//            ts = (BsonTimestamp)first.get("ts");*/
//            if(!StringUtil.isEmpty(this.pullTime) && endTs!=null){
//                ts = endTs;
//            }else if(!StringUtil.isEmpty(this.pullTime) && endTs==null){
//                //设定拉数据起始时间
//                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                Date date = simpleDateFormat.parse(this.pullTime);
//                long time = date.getTime();
//                ts = new BsonTimestamp((int) time,1);
//            }else{
//                //按当前时间为拉数据的起始时间
//                long time = System.currentTimeMillis();
//                ts = new BsonTimestamp((int)time ,1);;
//            }
//            BasicDBObject query = new BasicDBObject();
//            for (String tableName : analysisTablename) {
//                //添加指定表的过滤条件
//                query.append("ns", new BasicDBObject("$eq",tableName));
//            }
//            //构建查询语句,查询大于当前查询时间戳queryTs的记录
//            query.append("ts", new BasicDBObject("$gt", ts));
//
//            //获取最后执行时间
//            Document lastValue = collection.find(query).sort(new BasicDBObject("$natural", -1)).limit(1).first();
//            ts = (BsonTimestamp) lastValue.get("ts");
//            iterator = collection.find(query)
//                    .cursorType(CursorType.TailableAwait)//没有数据时阻塞休眠
//                    .noCursorTimeout(true) //防止服务器在不活动时间（10分钟）后使空闲的游标超时。
//                    .oplogReplay(true) //结合query条件，获取增量数据，这个参数比较难懂，见：https://docs.mongodb.com/manual/reference/command/find/index.html
//                    .maxAwaitTime(querytimeout, TimeUnit.SECONDS) //设置此操作在服务器上的最大等待执行时间
//                    .iterator();
//        } catch (Exception e) {
//            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
//        }
//    }
//
//    @Override
//    public OplogInputSpliter[] getInputSpliter(int minNumSplits) {
//        return new OplogInputSpliter[0];
//    }
//
//    @Override
//    public DataRecord<List<KV>> nextRecordInternal(DataRecord<List<KV>>  row)  {
//        DataRecord<List<KV>>  rowDataRecord = new DataRecord();
//        rowDataRecord.setTableName(tablename);
//        rowDataRecord.setSchemaName(database);
//        rowDataRecord.setTimestamp(DateUtils.getDateStr(new java.util.Date()));
//        List<KV> list = new ArrayList<KV>();
//        while(iterator.hasNext()){
//            //处理数据
//
//            next = iterator.next();
//            Object t = next.get("t");
//            Map o = next.get("o", Map.class);
//            DataRow<Object> dataRow = new DataRow<>(o.size());
//
//            for (Object key : o.keySet()) {
//                DataField<Object> dataField = new DataField<>();
//
//                Object o1 = o.get(key);
//                ColumnType type = null;
//
//                if(o1 instanceof  String){
//                    type = ColumnType.getType(o1.getClass().getName());
//                   o1 = o1.toString();
//                }else if(o1 instanceof Date){
//
//                }
//                dataField.setFieldKey(key.toString());
//                dataField.setFieldType(type);
//                dataField.setValue(o1);
//            }
//
//            Set<Map.Entry<String, Object>> entries = next.entrySet();
//
//            for (Map.Entry<String, Object> entry : entries) {
//                String type = entry.getValue().getClass().getName();
//                KV kv = null;
//                try {
//                    kv = TypeTransition(entry);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                list.add(kv);
//            }
//        }
//        rowDataRecord.setData(list);
//        return rowDataRecord;
//    }
//
//    public  KV  TypeTransition(Map.Entry<String, Object> entry) throws Exception {
//        String type = entry.getValue().getClass().getName();
//        KV kv = null;
//        type = type.substring(type.lastIndexOf(".") + 1);
//        String key = entry.getKey();
//        if (type.equals("Long")){
//            Long value = (Long)entry.getValue();
//            kv = new KV(key,value);
//        }else if(type.equals("Integer")){
//            Integer value = (Integer)entry.getValue();
//            kv = new KV(key,value);
//        }else if(type.equals("BsonTimestamp")){
//            BsonTimestamp value = (BsonTimestamp)entry.getValue();
//            kv = new KV(key,value);
//        }else if(type.equals("String")){
//            String value = (String)entry.getValue();
//            kv = new KV(key,value);
//        }else if(type.equals("Document")){
//            Document value = (Document)entry.getValue();
//            Map map = DCTransition(value);
//            kv = new KV(key,map);
//        }else{
//            throw new Exception(type+" Type Transition Exception");
//        }
//        return  kv;
//    }
//
//    public Map DCTransition(Document value){
//        Set<String> keys = value.keySet();
//        Map map = new HashMap();
//        for (String k : keys) {
//            Object v = value.get(k);
//            map.put(k,v);
//        }
//        return map;
//    }
//
//    @Override
//    public void closeInput() {
//
//    }
//
//    @Override
//    public void snapshot(JobFormatState formatState) {
//        formatState.setState(next);
//    }
//
//    @Override
//    public void configure(Configuration parameters) {
//        this.mongodbHost = readerConfig.getStringVal(KEY_READER_mongodb_HOST);
//        this.mongodbPort = readerConfig.getIntVal(KEY_READER_mongodb_PORT,27017);
//        this.mongodbUrl = readerConfig.getStringVal(KEY_READER_mongodb_URL);
//        this.mongodbUsername = readerConfig.getStringVal(KEY_READER_mongodb_USERNAME);
//        this.mongodbPassword = readerConfig.getStringVal(KEY_READER_mongodb_PASSWORD);
//        this.database = readerConfig.getStringVal(KEY_READER_DATABASE);
//        this.tablename = readerConfig.getStringVal(KEY_READER_TABLENAME);
//        String[] split = readerConfig.getStringVal(KEY_READER_ANALYSIS_TABLENAME, "").split(",");
//
//        this.analysisTablename =;
//        this.pullTime = readerConfig.getStringVal(KEY_READER_PULLTIME);
//        this.batchsize = readerConfig.getIntVal(KEY_READER_BATCHSIZE,10000);
//        this.querytimeout = readerConfig.getIntVal(KEY_READER_QUERYTIMEOUT,60);
//    }
//
//
//    @Override
//    public boolean reachedEnd() throws IOException {
//        return super.reachedEnd();
//    }
//
//    public OplogInputFormat(ArgusContext context) {
//        super(context);
//    }
//
//
//
//
//}
