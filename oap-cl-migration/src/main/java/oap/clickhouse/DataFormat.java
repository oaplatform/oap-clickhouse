package oap.clickhouse;

/**
 * Created by igor.petrenko on 19.02.2019.
 */
public enum DataFormat {
    TabSeparated, TabSeparatedRaw, TabSeparatedWithNames, TabSeparatedWithNamesAndTypes,
    CSV, CSVWithNames, Values, Vertical, JSON, JSONCompact, JSONEachRow, TSKV, Pretty, PrettyCompact,
    PrettyCompactMonoBlock, PrettyNoEscapes, PrettySpace, RowBinary, Native, Null, XML, CapnProto;
}
