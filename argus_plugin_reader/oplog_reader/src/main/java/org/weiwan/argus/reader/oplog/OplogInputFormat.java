package org.weiwan.argus.reader.oplog;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.weiwan.argus.common.utils.DateUtils;
import org.weiwan.argus.common.utils.StringUtil;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.enums.ColumnType;
import org.weiwan.argus.core.pub.input.BaseRichInputFormat;

import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;
import org.weiwan.argus.core.pub.pojo.JobFormatState;
import org.weiwan.argus.reader.oplog.input.OplogInputSpliter;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Author: lsl
 * @Date: 2020/8/21 11:15
 * @Description:
 **/
public class OplogInputFormat extends BaseRichInputFormat<DataRecord<DataRow<DataField>>, OplogInputSpliter> {

    private static final String KEY_READER_TYPE = "reader.type";
    private static final String KEY_READER_NAME = "reader.name";
    private static final String KEY_READER_CLASS = "reader.class";
    private static final String KEY_READER_MONGODB_HOST = "reader.mongodb.host";
    private static final String KEY_READER_MONGODB_PORT = "reader.mongodb.port";
    private static final String KEY_READER_MONGODB_URL = "reader.mongodb.url";
    private static final String KEY_READER_MONGODB_USERNAME = "reader.mongodb.username";
    private static final String KEY_READER_MONGODB_PASSWORD = "reader.mongodb.password";
    private static final String KEY_READER_DATABASE = "reader.dataBase";
    private static final String KEY_READER_TABLENAME = "reader.tableName";
    private static final String KEY_READER_ANALYSIS_TABLENAME = "reader.analysisTableName";
    private static final String KEY_READER_PULLTIME = "reader.pullTime";
    private static final String KEY_READER_PULLINDEX = "reader.pullIndex";
    private static final String KEY_READER_BATCHSIZE = "reader.batchSize";
    private static final String KEY_READER_QUERYTIMEOUT = "reader.queryTimeout";

    protected String mongodbHost;
    protected Integer mongodbPort;
    protected String mongodbUrl;
    protected String mongodbUsername;
    protected String mongodbPassword;
    protected String database;
    protected String tablename;
    protected String[] analysisTablename;
    protected String pullTime;
    protected Integer pullIndex;
    protected Integer batchsize;
    protected Integer querytimeout;


    private MongoClient mongoClient;
    private MongoDatabase mdb;
    private List<ServerAddress> addrs;
    private MongoCursor<Document> iterator;
    private MongoCollection<Document> collection;
    private BsonTimestamp ts;
    private Document next;

    @Override
    public void openInput(OplogInputSpliter split) {
        try {
            String[] hosts = mongodbHost.split(",");
            if (hosts.length == 1 && StringUtil.isEmpty(mongodbUsername) && StringUtil.isEmpty(mongodbPassword)) {
                mongoClient = new MongoClient(mongodbHost, mongodbPort);

            } else {
                //连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址
                //ServerAddress()两个参数分别为 服务器地址 和 端口
                addrs = new ArrayList<ServerAddress>();
                for (String host : hosts) {
                    ServerAddress serverAddress = new ServerAddress("", mongodbPort);
                    addrs.add(serverAddress);
                }
                //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
                MongoCredential credential = MongoCredential.createScramSha1Credential(mongodbUsername, database, mongodbPassword.toCharArray());
                List<MongoCredential> credentials = new ArrayList<MongoCredential>();
                credentials.add(credential);
                //通过连接认证获取MongoDB连接
                mongoClient = new MongoClient(addrs, credentials);
            }
            BsonTimestamp endTs = null;
            if (isRestore()) {
                Document state = (Document) formatState.getState();
                //获取重启之后最后一条数据的时间戳
                endTs = (BsonTimestamp) state.get("ts");
            }
            //连接到数据库
            mdb = mongoClient.getDatabase(database);
            collection = mdb.getCollection(tablename);

            if (!StringUtil.isEmpty(this.pullTime) && endTs != null) {
                ts = endTs;
            } else if (!StringUtil.isEmpty(this.pullTime) && endTs == null) {
                //设定拉数据起始时间
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = simpleDateFormat.parse(this.pullTime);
                long time = date.getTime();
                ts = new BsonTimestamp((int) (time/1000), pullIndex);
            } else {
                //按当前时间为拉数据的起始时间
                long time = System.currentTimeMillis();
                ts = new BsonTimestamp((int) (time/1000), pullIndex);
            }
            BasicDBObject query = new BasicDBObject();
            if(analysisTablename != null){
                List<String> tableList= new ArrayList<String>();
                for (String tableName : analysisTablename) {
                    //添加指定表的过滤条件
                    tableList.add(tableName);
                }
                query.put("ns", new BasicDBObject("$in",tableList));
            }
            //构建查询语句,查询大于当前查询时间戳queryTs的记录
            query.append("ts", new BasicDBObject("$gt", ts));
            //query.append("ts", new BasicDBObject("$gt", ts));  //config.system.sessions

            iterator = collection.find(query)
                    .cursorType(CursorType.TailableAwait)//没有数据时阻塞休眠
                    .noCursorTimeout(true) //防止服务器在不活动时间（10分钟）后使空闲的游标超时。
                    .oplogReplay(true) //结合query条件，获取增量数据，这个参数比较难懂，见：https://docs.mongodb.com/manual/reference/command/find/index.html
                    .maxAwaitTime(querytimeout, TimeUnit.SECONDS) //设置此操作在服务器上的最大等待执行时间
                    .iterator();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public OplogInputSpliter[] getInputSpliter(int minNumSplits) {
        OplogInputSpliter[] splits = new OplogInputSpliter[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            OplogInputSpliter exampleInputSplit = new OplogInputSpliter(i, minNumSplits);
            splits[i] = exampleInputSplit;
        }
        return splits;
    }

    @Override
    public DataRecord<DataRow<DataField>> nextRecordInternal(DataRecord<DataRow<DataField>> row) {
        if (iterator.hasNext()) {
            DataRecord<DataRow<DataField>> rowDataRecord = new DataRecord();
            rowDataRecord.setTableName(tablename);
            rowDataRecord.setSchemaName(database);
            rowDataRecord.setTimestamp(DateUtils.getDateStr(new java.util.Date()));
            Map mateMap = new HashMap();
            //处理数据
            next = iterator.next();
            ts = (BsonTimestamp) next.get("ts");
            Long t = next.get("t", Long.class);
            Long h = (Long) next.get("h", Long.class);
            Integer v = next.get("v", Integer.class);
            String op = next.get("op", String.class);
            String ns = next.get("ns", String.class);
            Document o = next.get("o", Document.class);
            mateMap.put("ts", ts);
            mateMap.put("t", t);
            mateMap.put("h", h);
            mateMap.put("v", v);
            mateMap.put("op", op);
            mateMap.put("ns", ns);
            rowDataRecord.setDataMeta(mateMap);
            Map map = (Map) converDocment(o);
            DataRow<DataField> dataRow = setDataField(map);

            rowDataRecord.setData(dataRow);
            return rowDataRecord;
        }
        return null;
    }

    public DataRow<DataField> setDataField(Map o) {
        DataRow<DataField> dataRow = new DataRow<>(o.size());
        Set<String> keyset = o.keySet();
        int i = -1;
        for (String key : keyset) {
            i++;
            DataField<Object> dataField = new DataField<>();

            ColumnType type = null;
            Object value = o.get(key);
            if (value != null) {
                String vt = value.getClass().getName();
                type = ColumnType.getType(vt);
            }
            dataField.setFieldKey(key.toString());
            dataField.setFieldType(type);
            dataField.setValue(value);
            dataRow.setField(i, dataField);
        }
        return dataRow;

    }


    public Object converDocment(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Document) {
            Map<Object, Object> dres = new HashMap<>();
            Document document = (Document) obj;
            Set<String> keySet = document.keySet();
            for (String key : keySet) {
                Object value = converDocment(document.get(key));
                dres.put(key, value);
            }
            return dres;
        } else if (obj instanceof Map) {
            HashMap<Object, Object> mres
                    = new HashMap<>();
            Map omap = (Map) obj;
            for (Object okey : omap.keySet()) {
                Object value = converDocment(omap.get(okey));
                mres.put(okey, value);
            }
            return mres;
        }
        if (obj instanceof List) {
            ArrayList lRes = new ArrayList();
            List olist = (List) obj;
            for (Object lobj : olist) {
                Object value = converDocment(lobj);
                lRes.add(value);
            }
            return lRes;
        }
        return obj.toString();
    }

    @Override
    public void closeInput() {

    }
    @Override
    public void snapshot(JobFormatState formatState) {
        formatState.setState(next);
    }
    @Override
    public void configure(Configuration parameters) {
        this.mongodbHost = readerConfig.getStringVal(KEY_READER_MONGODB_HOST);
        this.mongodbPort = readerConfig.getIntVal(KEY_READER_MONGODB_PORT, 27017);
        this.mongodbUrl = readerConfig.getStringVal(KEY_READER_MONGODB_URL);
        this.mongodbUsername = readerConfig.getStringVal(KEY_READER_MONGODB_USERNAME);
        this.mongodbPassword = readerConfig.getStringVal(KEY_READER_MONGODB_PASSWORD);
        this.database = readerConfig.getStringVal(KEY_READER_DATABASE);
        this.tablename = readerConfig.getStringVal(KEY_READER_TABLENAME);
        //String[] analysisTablenameArray =
        String stringVal = readerConfig.getStringVal(KEY_READER_ANALYSIS_TABLENAME, "");
        if(!StringUtil.isEmpty(stringVal)){
            String[] split = stringVal.split(",");
            this.analysisTablename = split;
        }
        this.pullTime = readerConfig.getStringVal(KEY_READER_PULLTIME);
        this.pullIndex = readerConfig.getIntVal(KEY_READER_PULLINDEX,1);
        this.batchsize = readerConfig.getIntVal(KEY_READER_BATCHSIZE, 10000);
        this.querytimeout = readerConfig.getIntVal(KEY_READER_QUERYTIMEOUT, 60);
    }


    @Override
    public boolean reachedEnd() throws IOException {
        return super.reachedEnd();
    }

    public OplogInputFormat(ArgusContext context) {
        super(context);
    }


}
