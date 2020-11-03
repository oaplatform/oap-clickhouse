package oap.clickhouse.migration;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.StringJoiner;

@Slf4j
class SqlUtils {
    static String addFieldsIndexesToInitQuery( TableEngine tableEngine, List<ConfigField> fields, List<ConfigIndex> indexes ) {
        var ct = "CREATE TABLE ${TABLE}${THREAD_IDX} (\n${FIELDS_INDEXES}\n) " + tableEngine.toString();


        var sj = new StringJoiner( ",\n" );
        for( var field : fields ) {
            sj.add( field.getColumnSql() );
        }

        for( var index : indexes ) {
            sj.add( index.getIndexSql() );
        }

        ct = StringUtils.replace( ct, "${FIELDS_INDEXES}", sj.toString() );

        return ct;
    }
}
