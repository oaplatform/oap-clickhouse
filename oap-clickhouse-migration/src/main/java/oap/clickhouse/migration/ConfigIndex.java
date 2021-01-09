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

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@ToString
@EqualsAndHashCode
public class ConfigIndex {
    public final String name;
    public final ArrayList<String> fields;
    public final String type;
    public final int granularity;

    @JsonCreator
    public ConfigIndex( String name, List<String> fields, String type, int granularity ) {
        this.name = name;
        this.fields = new ArrayList<>( fields );
        this.type = type;
        this.granularity = granularity;
    }

    public static String set() {
        return set( 0 );
    }

    public static String minmax() {
        return "minmax";
    }

    public static String set( int maxRows ) {
        return "set(" + maxRows + ")";
    }

    public static ConfigIndex index( String name, List<String> fields, String type, int granularity ) {
        return new ConfigIndex( name, fields, type, granularity );
    }

    public String getIndexSql() {
        return "INDEX " + name + " (" + String.join( ", ", fields ) + ") TYPE " + type + " GRANULARITY " + granularity;
    }
}
