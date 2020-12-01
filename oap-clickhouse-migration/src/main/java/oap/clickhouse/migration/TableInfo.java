/*
 * The MIT License (MIT)
 *
 * Copyright (c) Open Application Platform Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package oap.clickhouse.migration;

import lombok.ToString;

import java.util.List;
import java.util.Map;

/**
 * Created by igor.petrenko on 2019-10-28.
 */
@ToString
public class TableInfo {
    public final List<ConfigField> fields;
    public final List<ConfigIndex> indexes;
    public final TableEngine tableEngine;
    public final Map<String, String> params;
    public final String name;

    public TableInfo( String name,
                      List<ConfigField> fields,
                      List<ConfigIndex> indexes,
                      TableEngine tableEngine,
                      Map<String, String> params ) {
        this.name = name;
        this.fields = fields;
        this.indexes = indexes;
        this.tableEngine = tableEngine;
        this.params = params;
    }

    public TableInfo( String name, List<ConfigField> fields, List<ConfigIndex> indexes, TableEngine tableEngine ) {
        this( name, fields, indexes, tableEngine, Map.of() );
    }
}
