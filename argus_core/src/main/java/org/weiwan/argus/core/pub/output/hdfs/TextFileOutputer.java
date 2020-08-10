package org.weiwan.argus.core.pub.output.hdfs;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.weiwan.argus.common.utils.DateUtil;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRow;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/5 17:52
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: TextFileOutputer
 * @Description:
 **/
public class TextFileOutputer extends BaseFileOutputer<DataRow> {

    private OutputStream stream;

    public TextFileOutputer(Configuration configuration, FileSystem fileSystem) {
        super(configuration, fileSystem);
    }

    @Override
    public void closeOutputer() {
        try {
            if (stream != null) {
                stream.flush();
                stream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void initOutputer() throws IOException {
        Path path = new Path(blockPath);
        switch (compressType) {
            case NONE:
                stream = fileSystem.create(path);
                break;
            case GZIP:
                stream = new GzipCompressorOutputStream(fileSystem.create(path));
                break;
            case BIZP2:
                stream = new BZip2CompressorOutputStream(fileSystem.create(path));
                break;
        }
    }

    @Override
    public boolean out(Map<String, DataField> data) throws Exception {
        StringBuffer sb = new StringBuffer();
        for (String key : columnTypes.keySet()) {
            DataField dataField = data.get(key);
            Object value = dataField.getValue();
            if (value == null) {
                sb.append(HdfsUtil.NULL_VALUE);
            }
            if (value == null || value.toString().length() < 1) {
                sb.append("");
            } else {
                String rowData = value.toString();
                ColumnType fieldType = dataField.getFieldType();

                switch (fieldType) {
                    case TINYINT:
                        sb.append(Byte.valueOf(rowData));
                        break;
                    case SMALLINT:
                        sb.append(Short.valueOf(rowData));
                        break;
                    case INT:
                    case INTEGER:
                        sb.append(Integer.valueOf(rowData));
                        break;
                    case BIGINT:
                    case LONG:
                        if (value instanceof Timestamp) {
                            value = ((Timestamp) value).getTime();
                            sb.append(value);
                            break;
                        }

                        BigInteger bigVar = new BigInteger(rowData);
                        if (bigVar.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0) {
                            sb.append(bigVar);
                        } else {
                            sb.append(Long.valueOf(rowData));
                        }
                        break;
                    case FLOAT:
                        sb.append(Float.valueOf(rowData));
                        break;
                    case DOUBLE:
                        sb.append(Double.valueOf(rowData));
                        break;
                    case DECIMAL:
                        sb.append(HiveDecimal.create(new BigDecimal(rowData)));
                        break;
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        if (value instanceof Timestamp) {
                            SimpleDateFormat fm = DateUtil.getDateTimeFormatter();
                            sb.append(fm.format(value));
                        } else {
                            sb.append(rowData);
                        }
                        break;
                    case BOOLEAN:
                        sb.append(Boolean.valueOf(rowData));
                        break;
                    case DATE:
                        value = DateUtil.columnToDate(value, null);
                        sb.append(DateUtil.dateToString((Date) value));
                        break;
                    case TIMESTAMP:
                        value = DateUtil.columnToTimestamp(value, null);
                        sb.append(DateUtil.timestampToString((Date) value));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported column type: " + fieldType);
                }
            }
            sb.append(fieldDelimiter);
        }

        String tmpStr = sb.toString();
        int lastDelimiterIndex = tmpStr.lastIndexOf(fieldDelimiter);
        String subStr = tmpStr.substring(0,lastDelimiterIndex);
        String line = subStr + lineDelimiter;
        byte[] bytes = line.getBytes(this.charsetName);
        stream.write(bytes);
        stream.flush();
        return true;
    }


}
